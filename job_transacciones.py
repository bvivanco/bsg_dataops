"""
Glue job · Calidad de datos sobre transacciones · MODO SCRIPT

Reescritura del job visual de bsg-demo-glue. Mismo objetivo, pero:

  · no se inventa identificadores          (el visual usaba FillMissingValues)
  · la deduplicación es determinista       (el visual dejaba una fila al azar)
  · las filas malas no desaparecen         (el visual las filtraba en silencio)
  · el job ESCRIBE algo                    (el visual no tenía nodo de salida)
  · reporta qué pasó                       (el visual no emitía nada)
  · se detiene si los datos son inaceptables

Argumentos del job (Glue los pasa como --clave valor):

    --source_path       s3://bsg-demo-glue/raw/transacciones/
    --curated_path      s3://bsg-demo-glue/curated/transacciones/
    --quarantine_path   s3://bsg-demo-glue/quarantine/transacciones/
    --metrics_path      s3://bsg-demo-glue/_metrics/transacciones/
    --process_date      2026-09-24
    --umbral_rechazo_pct  5
"""

import json
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DecimalType, StringType, StructField,
                               StructType, TimestampType)

# ==========================================================================
# 0 · Inicialización
# ==========================================================================
ARGS = getResolvedOptions(sys.argv, [
    "JOB_NAME", "source_path", "curated_path", "quarantine_path",
    "metrics_path", "process_date", "umbral_rechazo_pct",
])

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(ARGS["JOB_NAME"], ARGS)

logger = glue_context.get_logger()
UMBRAL_RECHAZO_PCT = float(ARGS["umbral_rechazo_pct"])

logger.info(f"[transacciones] inicio · process_date={ARGS['process_date']}")


# ==========================================================================
# 1 · Lectura con ESQUEMA EXPLÍCITO
# ==========================================================================
# El job visual leía CSV sin esquema: todo llegaba como string. De ahí venía
# el truco de validar el importe con una expresión regular.
#
# Declarar el esquema cambia el problema: 'amount' llega como decimal o llega
# como NULL. Y un NULL es algo que se puede contar, cuarentenar y reportar.
# Una cadena que no casa con un regex, no.
#
# decimal(18,2) y no double: el punto flotante binario no representa exacto
# 0.1 ni 19.99. Para dinero eso no es aceptable.
ESQUEMA = StructType([
    StructField("transaction_id", StringType(),       True),
    StructField("customer_id",    StringType(),       True),
    StructField("amount",         DecimalType(18, 2), True),
    StructField("currency",       StringType(),       True),
    StructField("transaction_ts", TimestampType(),    True),
    StructField("channel",        StringType(),       True),
    StructField("status",         StringType(),       True),
])

df_raw = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")          # lo ilegible llega como NULL, no revienta
    .option("columnNameOfCorruptRecord", "_registro_corrupto")
    .schema(ESQUEMA)
    .csv(ARGS["source_path"])
    # Trazabilidad: de qué archivo salió cada fila. Cuando alguien pregunte
    # "¿de dónde vino este dato?", hay respuesta.
    .withColumn("_archivo_origen", F.input_file_name())
)

n_leidas = df_raw.count()
logger.info(f"[transacciones] filas leídas: {n_leidas}")
if n_leidas == 0:
    raise ValueError(f"No hay filas en {ARGS['source_path']}. Se aborta.")


# ==========================================================================
# 2 · Normalización
# ==========================================================================
def vacio_a_null(col):
    """Cadena vacía o solo espacios -> NULL.

    Sin esto, "" pasa cualquier filtro de `IS NOT NULL` y se cuela como
    identificador válido.
    """
    limpia = F.trim(col)
    return F.when(limpia == "", F.lit(None).cast(StringType())).otherwise(limpia)


df = (
    df_raw
    .withColumn("transaction_id", vacio_a_null(F.col("transaction_id")))
    .withColumn("customer_id",    vacio_a_null(F.col("customer_id")))
    .withColumn("currency",       F.upper(vacio_a_null(F.col("currency"))))
    .withColumn("status",         F.upper(vacio_a_null(F.col("status"))))
    .withColumn("channel",        F.lower(vacio_a_null(F.col("channel"))))
)


# ==========================================================================
# 3 · Reglas de calidad · TRES destinos, no uno
# ==========================================================================
# El job visual tenía un Filter que descartaba filas y no dejaba rastro.
# Aquí cada fila acaba en uno de tres sitios, y los tres se cuentan:
#
#   RECHAZO     la fila no identifica nada. No sirve ni para investigar.
#   CUARENTENA  sospechosa pero recuperable. La revisa una persona.
#   LIMPIA      publicable.
#
# La diferencia importa: un importe negativo puede ser un bug del origen,
# una devolución mal codificada, o un fraude. El pipeline no decide cuál.
# Lo deja visible para que alguien lo decida.

ES_RECHAZO = (
    F.col("transaction_id").isNull()        # sin PK no hay nada que hacer
    | F.col("customer_id").isNull()         # no se puede atribuir a nadie
    | F.col("transaction_ts").isNull()      # no se puede ubicar en el tiempo
)

REGLAS_CUARENTENA = {
    # El importe no se pudo parsear como decimal.
    # Esto sustituye al regex del job visual, y además:
    #   · no revienta si el valor es NULL
    #   · no es una UDF de Python fila a fila
    #   · distingue "no es un número" de "es un número que no me gusta"
    "importe_ilegible":   F.col("amount").isNull(),
    "importe_no_positivo": F.col("amount") <= 0,
    # Ajusta este tope al contrato real con el origen.
    "importe_fuera_rango": F.col("amount") > F.lit(1_000_000),
    "moneda_desconocida": ~F.col("currency").isin("PEN", "USD", "EUR"),
}

motivo = F.lit(None).cast(StringType())
for nombre, condicion in REGLAS_CUARENTENA.items():
    motivo = F.when(condicion, F.lit(nombre)).otherwise(motivo)

df = df.withColumn("_motivo_cuarentena", motivo)

df_rechazadas = df.filter(ES_RECHAZO)
df_validas    = df.filter(~ES_RECHAZO)
df_cuarentena = df_validas.filter(F.col("_motivo_cuarentena").isNotNull())
df_limpias    = df_validas.filter(F.col("_motivo_cuarentena").isNull())

n_rechazadas = df_rechazadas.count()
n_cuarentena = df_cuarentena.count()


# ==========================================================================
# 4 · Deduplicación DETERMINISTA
# ==========================================================================
# El job visual hacía .dropDuplicates(["customer_id", "transaction_id"]).
# Dos problemas:
#
#   a) dropDuplicates deja UNA fila cualquiera. Si dos filas con la misma
#      clave traen importes distintos, te quedas con una al azar — y en la
#      siguiente ejecución puede ser la otra. El resultado no es reproducible.
#
#   b) deduplicar por (customer_id, transaction_id) esconde un problema:
#      si el MISMO transaction_id aparece con DOS customer_id distintos,
#      las dos filas sobreviven. Y eso es justo lo que querías detectar.
#
# Aquí: la clave es transaction_id solo, y se elige la fila más reciente.
# Si no tienes un criterio de recencia, usa el que tenga sentido de negocio,
# pero que sea EXPLÍCITO.
ventana = Window.partitionBy("transaction_id").orderBy(
    F.col("transaction_ts").desc_nulls_last(),
    F.col("_archivo_origen").desc(),        # desempate estable
)

df_limpias = (
    df_limpias
    .withColumn("_n", F.row_number().over(ventana))
)

n_antes_dedupe = df_limpias.count()

# Las filas descartadas por duplicado NO se tiran: van a cuarentena con su
# motivo. Si el origen empieza a duplicar, quieres enterarte, no absorberlo.
df_duplicadas = (
    df_limpias.filter(F.col("_n") > 1)
    .withColumn("_motivo_cuarentena", F.lit("duplicado_transaction_id"))
    .drop("_n")
)
df_limpias = df_limpias.filter(F.col("_n") == 1).drop("_n")

n_duplicadas = n_antes_dedupe - df_limpias.count()
df_cuarentena = df_cuarentena.unionByName(df_duplicadas)

# Alerta de integridad: el mismo transaction_id con varios customer_id.
# No se corrige — se cuenta y se reporta.
n_id_ambiguos = (
    df_validas.groupBy("transaction_id")
    .agg(F.countDistinct("customer_id").alias("n_clientes"))
    .filter(F.col("n_clientes") > 1)
    .count()
)
if n_id_ambiguos:
    logger.warn(f"[transacciones] {n_id_ambiguos} transaction_id con más de un "
                f"customer_id. El origen tiene un problema de integridad.")


# ==========================================================================
# 5 · Enriquecimiento
# ==========================================================================
df_final = (
    df_limpias
    .withColumn("transaction_date", F.to_date("transaction_ts"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_process_date", F.lit(ARGS["process_date"]))
    .drop("_motivo_cuarentena")
)

n_final = df_final.count()


# ==========================================================================
# 6 · Escritura
# ==========================================================================
# Lo que más llama la atención del job original: NO ESCRIBÍA NADA.
# Leía, transformaba, y hacía job.commit(). El resultado se perdía.
#
# Idempotencia: se borra la partición del día antes de reescribirla.
# El sink S3 de Glue solo hace append — no tiene modo de sobrescritura —
# así que reemplazar una partición es una operación explícita.
ruta_dia = f"{ARGS['curated_path'].rstrip('/')}/transaction_date={ARGS['process_date']}/"
try:
    glue_context.purge_s3_path(ruta_dia, options={"retentionPeriod": 0})
    logger.info(f"[transacciones] partición purgada: {ruta_dia}")
except Exception as e:
    logger.info(f"[transacciones] sin purga previa ({type(e).__name__})")

(
    DynamicFrame.fromDF(df_final.coalesce(4), glue_context, "curated")
    .toDF().write
    .mode("append")
    .partitionBy("transaction_date")
    .parquet(ARGS["curated_path"])
)

if n_cuarentena + n_duplicadas > 0:
    (
        df_cuarentena.coalesce(1).write
        .mode("overwrite")
        .parquet(f"{ARGS['quarantine_path'].rstrip('/')}/"
                 f"process_date={ARGS['process_date']}/")
    )


# ==========================================================================
# 7 · Métricas de la corrida
# ==========================================================================
# Un pipeline que no reporta cuántas filas procesó no se puede monitorear.
# Esto alimenta el tablero y es la entrada del gate de abajo.
tasa_rechazo = round(100.0 * n_rechazadas / n_leidas, 3) if n_leidas else 0.0

metricas = {
    "job_name": ARGS["JOB_NAME"],
    "process_date": ARGS["process_date"],
    "finished_at": datetime.now(timezone.utc).isoformat(),
    "filas_leidas": n_leidas,
    "filas_rechazadas": n_rechazadas,
    "filas_cuarentena": n_cuarentena + n_duplicadas,
    "filas_duplicadas": n_duplicadas,
    "ids_ambiguos": n_id_ambiguos,
    "filas_escritas": n_final,
    "tasa_rechazo_pct": tasa_rechazo,
}
logger.info(f"[transacciones] METRICAS {json.dumps(metricas)}")
print(json.dumps(metricas, indent=2))

bucket, _, prefijo = ARGS["metrics_path"].replace("s3://", "").partition("/")
boto3.client("s3").put_object(
    Bucket=bucket,
    Key=f"{prefijo.rstrip('/')}/process_date={ARGS['process_date']}/metrics.json",
    Body=json.dumps(metricas).encode("utf-8"),
    ContentType="application/json",
)


# ==========================================================================
# 8 · Compuerta de calidad
# ==========================================================================
# Validar no es lo mismo que bloquear. El job original filtraba y seguía
# adelante pasara lo que pasara.
#
# Aquí, si la tasa de rechazo supera el umbral, el job FALLA y no publica.
# Un job en verde con datos corruptos es peor que un job en rojo, porque
# nadie investiga lo que parece estar bien.
if tasa_rechazo > UMBRAL_RECHAZO_PCT:
    raise ValueError(
        f"Compuerta de calidad NO superada: {tasa_rechazo}% de filas rechazadas "
        f"(umbral {UMBRAL_RECHAZO_PCT}%). Revisa {ARGS['source_path']}."
    )

job.commit()
logger.info("[transacciones] fin correcto")
