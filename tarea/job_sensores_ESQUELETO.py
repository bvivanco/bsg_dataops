"""
TAREA · Job de Glue para lecturas de sensores · ESQUELETO

Este archivo está incompleto a propósito. Hay CUATRO bloques marcados con TODO
que tienes que escribir tú. El resto ya está hecho.

No copies el job del laboratorio: las reglas aquí son distintas.
En el laboratorio un número negativo era sospechoso. Aquí, una temperatura
de -20 grados es exactamente lo que debe salir.

Argumentos del job:
    --source_path       s3://TU-BUCKET/raw/sensores/
    --curated_path      s3://TU-BUCKET/curated/sensores/
    --quarantine_path   s3://TU-BUCKET/quarantine/sensores/
    --metrics_path      s3://TU-BUCKET/_metrics/sensores/
    --process_date      2026-03-01
"""

import json
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DecimalType, IntegerType, StringType,
                               StructField, StructType)

# ==========================================================================
# 0 · Inicialización · YA ESTÁ HECHO
# ==========================================================================
ARGS = getResolvedOptions(sys.argv, [
    "JOB_NAME", "source_path", "curated_path", "quarantine_path",
    "metrics_path", "process_date",
])

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(ARGS["JOB_NAME"], ARGS)
logger = glue_context.get_logger()


# ==========================================================================
# 1 · TODO · El esquema
# ==========================================================================
# El CSV tiene estas seis columnas, en este orden:
#
#     lectura_id, sensor_id, sucursal, fecha_lectura, temperatura_c, humedad_pct
#
# Decide el tipo de cada una. Piensa antes de escribir:
#
#   · ¿lectura_id es un número o un texto? ¿Vas a sumarlos alguna vez?
#   · temperatura_c lleva decimales y PUEDE SER NEGATIVA. ¿Decimal o entero?
#   · humedad_pct es un porcentaje entero.
#   · fecha_lectura viene en DOS formatos distintos. Mira el CSV y verás.
#     Pista: si la declaras como fecha, Spark solo entiende UNO de los dos
#     formatos y el otro queda en NULL. Quizá te convenga leerla como texto
#     y convertirla tú en el paso 3.
#
ESQUEMA = StructType([
    # TODO: completa las seis columnas
])

df_raw = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .schema(ESQUEMA)
    .csv(ARGS["source_path"])
    .withColumn("_archivo_origen", F.input_file_name())
)

n_leidas = df_raw.count()
logger.info(f"[sensores] filas leidas: {n_leidas}")
if n_leidas == 0:
    raise ValueError(f"No hay filas en {ARGS['source_path']}")


# ==========================================================================
# 2 · Limpieza · YA ESTÁ HECHO (te lo damos como ejemplo)
# ==========================================================================
def vacio_a_null(col):
    """Cadena vacía o solo espacios -> NULL."""
    limpia = F.trim(col)
    return F.when(limpia == "", F.lit(None).cast(StringType())).otherwise(limpia)


df = (
    df_raw
    .withColumn("lectura_id", vacio_a_null(F.col("lectura_id")))
    .withColumn("sensor_id", vacio_a_null(F.col("sensor_id")))
)


# ==========================================================================
# 3 · TODO · Normalizar la sucursal y la fecha
# ==========================================================================
# a) SUCURSAL
#    Mira el CSV: la misma sucursal aparece escrita de varias formas:
#        "LIMA-01"   " lima-01 "   "Lima-01"   "LIMA01"
#    Déjalas todas igual. Pistas: F.trim(), F.upper(), F.regexp_replace()
#
# b) FECHA
#    Vienen DOS formatos mezclados:
#        2026-03-21 18:30:00     (año-mes-día, con espacio)
#        05/03/2026 07:15:00     (día/mes/año)
#    Conviértelos a un solo tipo timestamp.
#    Pista: F.coalesce() se queda con el primer valor que NO sea nulo,
#           y F.to_timestamp(columna, "formato") devuelve NULL si no encaja.
#           Encadena dos to_timestamp dentro de un coalesce.
#
df = (
    df
    # TODO: normaliza la sucursal
    # TODO: convierte fecha_lectura a timestamp
)


# ==========================================================================
# 4 · TODO · Las reglas de calidad
# ==========================================================================
# RECHAZO · la lectura no identifica nada, no sirve ni para investigar
#    · sin lectura_id
#    · sin sensor_id
#
ES_RECHAZO = (
    # TODO
)

# CUARENTENA · sospechosa, pero la revisa una persona
#    · "temperatura_ilegible"  -> no se pudo convertir a número
#    · "sensor_averiado"       -> la temperatura está fuera de -40 a +40,
#                                 un rango que ningún sensor real daría
#
REGLAS_CUARENTENA = {
    # TODO: dos reglas, con esos nombres exactos
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
# 5 · Deduplicación · YA ESTÁ HECHO
# ==========================================================================
ventana = Window.partitionBy("lectura_id").orderBy(F.col("_archivo_origen").desc())
df_limpias = df_limpias.withColumn("_n", F.row_number().over(ventana))
n_antes = df_limpias.count()
df_limpias = df_limpias.filter(F.col("_n") == 1).drop("_n")
n_duplicadas = n_antes - df_limpias.count()


# ==========================================================================
# 6 · TODO · La transformación · estado de la cadena de frío
# ==========================================================================
# Añade una columna nueva llamada 'estado_cadena' con estos valores:
#
#     temperatura_c <= -18          ->  "OK"
#     entre -18 (sin incluir) y -12 ->  "ALERTA"
#     mayor que -12                 ->  "CRITICO"
#
# Pista: F.when(condicion, valor).when(...).otherwise(valor)
#
# Y añade también una columna 'fecha' con solo la fecha (sin la hora),
# sacada de fecha_lectura. Pista: F.to_date()
#
df_final = (
    df_limpias
    # TODO: estado_cadena
    # TODO: fecha
    .withColumn("_ingested_at", F.current_timestamp())
    .drop("_motivo_cuarentena")
)

n_final = df_final.count()


# ==========================================================================
# 7 · Escritura · YA ESTÁ HECHO
# ==========================================================================
ruta_dia = f"{ARGS['curated_path'].rstrip('/')}/fecha={ARGS['process_date']}/"
try:
    glue_context.purge_s3_path(ruta_dia, options={"retentionPeriod": 0})
except Exception:
    pass

df_final.coalesce(1).write.mode("append").partitionBy("fecha") \
    .parquet(ARGS["curated_path"])

if n_cuarentena > 0:
    df_cuarentena.coalesce(1).write.mode("overwrite") \
        .parquet(f"{ARGS['quarantine_path'].rstrip('/')}/"
                 f"process_date={ARGS['process_date']}/")


# ==========================================================================
# 8 · Métricas · YA ESTÁ HECHO
# ==========================================================================
# Fíjate en que además de las métricas técnicas guardamos el conteo por
# estado. Eso es una métrica de NEGOCIO: si mañana hay 20 lecturas en
# CRITICO, alguien tiene que enterarse.
por_estado = {r["estado_cadena"]: r["n"] for r in
              df_final.groupBy("estado_cadena").agg(F.count("*").alias("n")).collect()}

metricas = {
    "job_name": ARGS["JOB_NAME"],
    "process_date": ARGS["process_date"],
    "finished_at": datetime.now(timezone.utc).isoformat(),
    "filas_leidas": n_leidas,
    "filas_rechazadas": n_rechazadas,
    "filas_cuarentena": n_cuarentena,
    "filas_duplicadas": n_duplicadas,
    "filas_escritas": n_final,
    "por_estado": por_estado,
}
print(json.dumps(metricas, indent=2))

bucket, _, prefijo = ARGS["metrics_path"].replace("s3://", "").partition("/")
boto3.client("s3").put_object(
    Bucket=bucket,
    Key=f"{prefijo.rstrip('/')}/process_date={ARGS['process_date']}/metrics.json",
    Body=json.dumps(metricas).encode("utf-8"),
    ContentType="application/json",
)

job.commit()
