# Manual del participante
# Tu primer pipeline de datos que funciona solo

**Módulo VII · DataOps en Plataformas en la Nube**
Duración: 90 minutos · Todo se hace en la consola de AWS

---

## Qué vas a construir

Hoy vas a montar algo que, cuando alguien sube un archivo, **se procesa solo**:
sin que nadie apriete un botón, a cualquier hora, incluso de madrugada.

```
  alguien sube        S3               Lambda              Glue              S3
  un archivo    ──▶  raw/      ──▶   revisa y     ──▶   transforma   ──▶  curated/
                                      dispara          y limpia         (datos buenos)
```

## Qué vas a aprender

Al terminar vas a poder explicar:

- Por qué un pipeline que **se dispara con el dato** es mejor que uno con horario
- Qué pasa cuando llega un archivo con errores, y por qué **no se borra en silencio**
- Por qué los permisos en la nube son **dos cosas distintas** que todo el mundo confunde
- Cómo saber si tu pipeline funcionó sin entrar a mirarlo

---

## Antes de empezar · 5 minutos

### Lo que necesitas

- Una cuenta de AWS y poder entrar a <https://console.aws.amazon.com>
- Un editor de texto cualquiera

### ⚠️ Elige la región y no la cambies

Arriba a la derecha de la consola hay un desplegable con un nombre de ciudad
o estado. **Elige `US East (N. Virginia)`**.

> **¿Por qué importa tanto?**
> Cada región de AWS es un mundo aparte. Si creas el almacén en una y el
> programa en otra, **no se ven entre ellos** — y el mensaje de error no te dice
> eso. Es el fallo silencioso más común cuando se empieza.

### Si te sale un error rojo de permisos

En cualquier paso puede salir algo como *"You are not authorized"* o
*"access denied"*. Significa que a tu usuario le falta un permiso.

Avisa al instructor. Las políticas que necesitas son:

```
AmazonS3FullAccess        AWSGlueConsoleFullAccess
AWSLambda_FullAccess      CloudWatchLogsFullAccess
IAMFullAccess
```

---

### ✍️ Anota aquí tus datos

Los vas a necesitar durante todo el laboratorio:

```
Mi bucket se llama:  ________________________________

Mi región:           US East (N. Virginia) us-east-1
```

---

# Parte 1 · El almacén

**10 minutos**

Un **bucket** es como una carpeta gigante en la nube. Ahí van a vivir tus datos.

### Pasos

**1.1** En el buscador de arriba escribe **S3** y entra.

**1.2** Pulsa el botón naranja **Create bucket**.

**1.3** En *Bucket name* escribe `transacciones-` seguido de tus iniciales y un
número. Por ejemplo: `transacciones-bvs-01`.

> ⚠️ Ese nombre es **único en todo el mundo**, no solo en tu cuenta. Si te dice
> que ya existe, cambia el número.

**1.4** Comprueba que *AWS Region* dice **US East (N. Virginia)**.

**1.5** Baja hasta *Block Public Access settings* y deja marcado
**Block all public access**. (Ya viene así.)

**1.6** Baja del todo y pulsa **Create bucket**.

### Crear las carpetas

Entra al bucket haciendo clic en su nombre. Vas a crear **seis carpetas**.

Para cada una: botón **Create folder** → escribe el nombre → **Create folder**.

| Carpeta | Para qué sirve |
|---|---|
| `raw` | El archivo tal como llega |
| `curated` | El resultado ya limpio |
| `quarantine` | Las filas sospechosas |
| `scripts` | El código |
| `temp` | Espacio de trabajo interno |
| `_metrics` | El informe de cada ejecución |

Después **entra en `raw`** y crea dentro otra llamada `transacciones`.

### ✅ Punto de control

Entra a tu bucket. Debes ver seis carpetas, y dentro de `raw` otra llamada
`transacciones`. Si es así, sigue.

> **¿Por qué tantas carpetas?**
>
> `raw` guarda el archivo **tal como llegó, y nunca se modifica**. Si mañana
> descubres que tu programa tenía un error, puedes volver a procesar desde el
> original. Si pisas el archivo crudo, el error ya no se puede deshacer.
>
> Y `quarantine` es igual de importante: ahí van las filas raras **en vez de
> borrarlas**. Un importe negativo puede ser un error del sistema... o una
> devolución mal registrada que alguien de finanzas lleva meses buscando.
> El programa no lo decide: lo deja a la vista para que lo decida una persona.

---

# Parte 2 · Los datos

**10 minutos**

### Crear el archivo

Abre un editor de texto y escribe exactamente esto:

```
transaction_id,customer_id,amount
TRX-001,CUST-100,150.50
TRX-001,CUST-100,150.50
TRX-002,CUST-101,89.90
TRX-003,CUST-102,0
TRX-004,,45.00
TRX-005,CUST-103,-20.00
TRX-006,CUST-104,abc
```

Guárdalo como **`transacciones.csv`**.

> ⚠️ **Si usas Mac con TextEdit:** antes de escribir nada, ve al menú
> *Formato → Convertir en texto normal*. Si no, el archivo se guarda con
> formato y no sirve.
>
> ⚠️ **Si usas Windows con Bloc de notas:** al guardar, en *Tipo* elige
> **Todos los archivos**, para que no te quede `transacciones.csv.txt`.

### Mira lo que acabas de escribir

Ese archivo tiene **errores a propósito**. Búscalos:

| Fila | Qué tiene de raro |
|---|---|
| `TRX-001` | Aparece **dos veces**, idéntica |
| `TRX-003` | El importe es **cero** |
| `TRX-004` | **No tiene cliente** |
| `TRX-005` | El importe es **negativo** |
| `TRX-006` | Donde va un número, dice `abc` |

Son siete filas. **Solo dos van a salir publicadas.** Apúntalo: lo vas a
comprobar al final.

### Subirlo

**2.1** En S3, entra a tu bucket → `raw` → `transacciones`.

**2.2** Botón **Upload** → **Add files** → elige `transacciones.csv` → **Upload**.

**2.3** Cuando diga *Upload succeeded*, pulsa **Close**.

### ✅ Punto de control

Dentro de `raw/transacciones/` tiene que aparecer `transacciones.csv`.

---

# Parte 3 · 🔑 El permiso del programa

**15 minutos**

Aquí empieza la parte que más cuesta. Léela con calma.

> **El concepto**
>
> Cuando tú entras a AWS, usas **tu** usuario. Pero los programas no pueden usar
> tu usuario: necesitan una identidad propia. Eso se llama **rol**.
>
> Y un rol tiene **dos partes**, que son cosas distintas:
>
> | | Responde a |
> |---|---|
> | **Relación de confianza** | ¿*Quién* puede usar este rol? |
> | **Políticas** | ¿*Qué* puede hacer quien lo usa? |
>
> Confundir las dos es el error número uno de quien empieza en AWS.

### Crear el rol

**3.1** En el buscador escribe **IAM** y entra.

**3.2** Menú izquierdo → **Roles** → botón **Create role**.

**3.3** En *Trusted entity type* deja marcado **AWS service**.

**3.4** Baja hasta *Use case*, abre el desplegable, escribe `Glue` y selecciónalo.
Pulsa **Next**.

> *Acabas de definir la **relación de confianza**: le dijiste a AWS que este rol
> lo va a usar el servicio Glue, y nadie más.*

**3.5** En el buscador de permisos escribe `AWSGlueServiceRole` y **marca la
casilla** de la izquierda. Pulsa **Next**.

**3.6** En *Role name* escribe:

```
RolGlueTransacciones
```

Baja del todo y pulsa **Create role**.

### Darle acceso a tu bucket

El rol ya existe, pero todavía **no puede tocar tus datos**.

**3.7** En la lista de roles, clic en **RolGlueTransacciones**.

**3.8** Pestaña **Permissions** → desplegable **Add permissions** →
**Create inline policy**.

**3.9** Arriba a la derecha, cambia de la pestaña *Visual* a la pestaña **JSON**.

**3.10** Borra todo lo que haya y pega esto. **Cambia `transacciones-bvs-01` por
el nombre de tu bucket, en los dos sitios:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::transacciones-bvs-01/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::transacciones-bvs-01"
    }
  ]
}
```

**3.11** Pulsa **Next**, ponle de nombre `AccesoBucket` y **Create policy**.

> **Mira bien la diferencia entre los dos bloques**: está al final de la línea.
>
> El primero termina en **`/*`** → permiso sobre **los archivos** de dentro.
> El segundo **no lo lleva** → permiso sobre **el bucket**, para poder abrirlo
> y ver qué hay.
>
> Si falta cualquiera de los dos, el programa falla — y el mensaje de error no
> te dice cuál falta.

### ✅ Punto de control

En el rol, pestaña *Permissions*, deben aparecer **dos** políticas:
`AWSGlueServiceRole` y `AccesoBucket`.

---

# Parte 4 · El programa que transforma

**20 minutos**

### Subir el código

El archivo es **[`job_transacciones.py`](job_transacciones.py)**, y está en este
mismo repositorio.

> **Cómo descargarlo:** abre el enlace, pulsa el botón **Raw** (arriba a la
> derecha del código) y guarda la página con `Cmd+S` / `Ctrl+S`.
> Comprueba que el nombre queda **`job_transacciones.py`**, sin `.txt` al final.

**4.1** Ve a S3 → tu bucket → carpeta `scripts`.

**4.2** **Upload** → **Add files** → elige el archivo → **Upload**.

### Crear el trabajo

**4.3** En el buscador escribe **Glue** y entra.

**4.4** Menú izquierdo → **ETL jobs** → botón **Script editor** →
**Create script**.

**4.5** Pestaña **Job details**. Rellena:

| Campo | Qué poner |
|---|---|
| Name | `transacciones-etl` |
| IAM Role | `RolGlueTransacciones` |
| Type | Spark |
| Glue version | Glue 5.0 |
| Language | Python 3 |
| Worker type | **G.1X** |
| Requested number of workers | **2** |
| Job timeout (minutes) | **20** |
| Number of retries | **0** |

> **¿Por qué solo 2 máquinas?** Porque tu archivo tiene siete filas.
> Pedir diez máquinas no lo haría más rápido: pagarías por nueve que no hacen
> nada. **En la nube, cada ejecución tiene un precio, y lo decides tú.**

**4.6** Baja hasta **Advanced properties**, ábrelo, y busca **Job parameters**.

Pulsa **Add new parameter** una vez por cada fila. **Cambia el nombre del bucket
por el tuyo:**

| Key | Value |
|---|---|
| `--source_path` | `s3://transacciones-bvs-01/raw/transacciones/` |
| `--curated_path` | `s3://transacciones-bvs-01/curated/transacciones/` |
| `--quarantine_path` | `s3://transacciones-bvs-01/quarantine/transacciones/` |
| `--metrics_path` | `s3://transacciones-bvs-01/_metrics/transacciones/` |
| `--process_date` | `2026-01-01` |
| `--umbral_rechazo_pct` | `30` |

> **¿Por qué las rutas van aquí y no dentro del código?**
>
> Porque así puedes procesar otro día, o mover el programa a otro ambiente,
> **sin tocar ni una línea**. Un programa con las rutas escritas dentro solo
> sirve para un caso.
>
> Y fíjate en el último: `--umbral_rechazo_pct = 30`. Es una promesa:
> *"si más del 30% de las filas viene mal, no publico nada"*. Volveremos a él.

**4.7** Vuelve a la pestaña **Script** y pega ahí todo el contenido de
**[`job_transacciones.py`](job_transacciones.py)**.

> Ábrelo, pulsa **Raw**, selecciona todo (`Cmd+A` / `Ctrl+A`), copia, y pégalo
> en el editor de Glue **reemplazando lo que haya**.

**4.8** Pulsa **Save** (arriba a la derecha).

### Probarlo a mano

**4.9** Pulsa **Run**.

**4.10** Ve a la pestaña **Runs**. Verás la ejecución. Refresca cada poco:

```
Running   →   Succeeded
```

Tarda **2 o 3 minutos**.

### ✅ Punto de control · importante

**No sigas hasta que diga `Succeeded`.**

> Si automatizas algo que no funciona, lo único que consigues es **automatizar
> el fallo**.

**Si dice `Failed`:** clic en la ejecución → **Error logs**.

| Mensaje | Qué revisar |
|---|---|
| `AccessDeniedException` | La política del paso 3.10 apunta a otro bucket |
| `iam:PassRole` | A tu usuario le falta `IAMFullAccess`. Avisa al instructor |
| `Path does not exist` | Falta la barra `/` al final de `--source_path` |

---

# Parte 5 · La función que dispara

**10 minutos**

Hasta ahora, **tú** aprietas *Run*. Eso se acabó.

**Lambda** es una función que AWS ejecuta sola cuando pasa algo. En este caso:
cuando llegue un archivo.

### Crearla

**5.1** En el buscador escribe **Lambda** y entra.

**5.2** Botón **Create function**.

**5.3** Rellena:

| Campo | Qué poner |
|---|---|
| | Deja **Author from scratch** |
| Function name | `disparar-transacciones` |
| Runtime | **Python 3.12** |

Pulsa **Create function**.

**5.4** Baja hasta el recuadro de código. Haz doble clic en `lambda_function.py`,
**borra todo** y pega:

```python
import boto3

glue = boto3.client("glue")

def lambda_handler(event, context):
    # S3 nos dice qué archivo acaba de llegar
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key    = event["Records"][0]["s3"]["object"]["key"]
    print(f"Llego: s3://{bucket}/{key}")

    # Lanzamos el trabajo de Glue
    run = glue.start_job_run(JobName="transacciones-etl")
    print(f"Trabajo lanzado: {run['JobRunId']}")

    return {"job_run_id": run["JobRunId"]}
```

**5.5** ⚠️ **Pulsa `Deploy`** (encima del código).

> **Este es el olvido número uno.** Sin pulsar *Deploy*, el código que ves en
> pantalla **no es** el que se ejecuta. Si más adelante algo no funciona como
> esperas, vuelve a mirar si pulsaste *Deploy*.

### Darle más tiempo

**5.6** Pestaña **Configuration** → **General configuration** → **Edit**.

**5.7** Cambia *Timeout* de `3` a **`30`** segundos. **Save**.

> Viene con tres segundos. No alcanzan ni para saludar a Glue.

---

# Parte 6 · 🔑 El permiso de la función

**10 minutos**

Tu función ya existe, pero **no puede lanzar nada todavía**. Una función recién
creada solo sabe escribir mensajes de registro.

**6.1** En tu función, pestaña **Configuration** → menú izquierdo **Permissions**.

**6.2** En *Execution role*, clic en el **nombre azul del rol**. Se abre IAM en
otra pestaña.

**6.3** Desplegable **Add permissions** → **Create inline policy**.

**6.4** Pestaña **JSON**, borra todo y pega:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns"
    ],
    "Resource": "*"
  }]
}
```

**6.5** **Next** → nombre `LanzarGlue` → **Create policy**.

### ✅ Punto de control

El rol de tu función debe tener ahora **dos** políticas: la que trae de serie
(para los registros) y `LanzarGlue`.

---

# Parte 7 · 🔑 Conectar el archivo con la función

**5 minutos**

**7.1** Vuelve a la pestaña de tu función Lambda.

**7.2** En el diagrama de arriba, botón **+ Add trigger**.

**7.3** Rellena:

| Campo | Qué poner |
|---|---|
| Source | **S3** |
| Bucket | el tuyo |
| Event types | **All object create events** |
| Prefix | `raw/transacciones/` |
| Suffix | `.csv` |

**7.4** Abajo hay un aviso sobre recursión. **Márcalo** y pulsa **Add**.

> **Acabas de crear un segundo permiso, aunque no lo hayas notado.**
>
> Son **dos permisos distintos**, y es lo que más confunde al empezar:
>
> | | Responde a | Si falta |
> |---|---|---|
> | El del **paso 6** | ¿Qué **puede hacer** mi función? | Sale un error en el registro |
> | El de **ahora** | ¿**Quién puede llamarla**? | **No pasa nada de nada** |
>
> El segundo es el peor de los dos. Sin él, subes el archivo y **no ocurre
> absolutamente nada**: ni error, ni mensaje, ni rastro. Como lo hiciste por
> consola, AWS lo puso solo.

### 🔴 Una comprobación que evita un disgusto

Tu programa escribe en `curated/`. **Nunca debe escribir en
`raw/transacciones/`**, que es la carpeta que dispara la función.

```
raw/transacciones/   ← dispara la función
curated/             ← escribe el programa    ✅ carpeta distinta
```

Si escribieran en la misma, cada ejecución se dispararía a sí misma, una y otra
vez. Se llama **bucle infinito** y es el error que genera facturas enormes.

---

# Parte 8 · Probarlo entero

**10 minutos**

**8.1** S3 → tu bucket → `raw` → `transacciones`.

**8.2** **Upload** → **Add files** → `transacciones.csv` → **Upload**.

> Súbelo aunque ya esté: cada subida cuenta como archivo nuevo.

### 🚫 A partir de aquí, no toques nada

Observa.

**8.3 · ¿Reaccionó la función?**

Lambda → `disparar-transacciones` → pestaña **Monitor** →
**View CloudWatch logs** → clic en el registro más reciente.

Debe decir:
```
Llego: s3://transacciones-bvs-01/raw/transacciones/transacciones.csv
Trabajo lanzado: jr_a1b2c3...
```

**8.4 · ¿Arrancó el programa?**

Glue → **ETL jobs** → `transacciones-etl` → pestaña **Runs**.

Debe haber una ejecución nueva que tú **no lanzaste**.

**8.5 · ¿Están los datos?**

S3 → tu bucket → `curated` → `transacciones` → debe haber archivos.
Y en `quarantine` → `transacciones` → las filas sospechosas.

**8.6 · ¿Qué hizo exactamente?**

S3 → `_metrics` → `transacciones` → entra en la carpeta → marca `metrics.json`
→ botón **Open**.

```json
{
  "filas_leidas": 7,
  "filas_rechazadas": 1,
  "filas_cuarentena": 3,
  "filas_duplicadas": 1,
  "filas_escritas": 2,
  "tasa_rechazo_pct": 14.286
}
```

### ✅ Punto de control final

Compara con lo que anotaste en la Parte 2:

| | Cuál | Qué pasó |
|---|---|---|
| **Rechazada** (1) | `TRX-004` | Sin cliente: no se puede atribuir a nadie. **Se descarta** |
| **En cuarentena** (3) | `TRX-003`, `TRX-005`, `TRX-006` | Cero, negativo y texto. **Se guardan para revisar** |
| **Duplicada** (1) | la segunda `TRX-001` | Era válida, pero repetida. **Se quedó una** |
| **Publicadas** (2) | `TRX-001`, `TRX-002` | Las buenas |

> **Fíjate en la diferencia entre las tres primeras filas de esta tabla.**
>
> La **rechazada** se tira: sin cliente no sirve ni para investigar.
> Las de **cuarentena** se guardan: un importe negativo puede ser un error...
> o una devolución real que alguien tiene que mirar.
> La **duplicada** se elimina, pero el pipeline **cuenta cuántas eran**. Si ese
> número empieza a crecer, el problema está en el origen, no en tu código.

> **Fíjate en una cosa: no entraste a mirar la consola de Glue para saber todo
> esto. El pipeline te lo contó.**
>
> Un programa que no dice cuántas filas procesó es un programa que no se puede
> vigilar. Y lo que no se puede vigilar, no está en producción: está en
> esperanza.

### Si no pasó nada

| Qué ves | Qué revisar |
|---|---|
| Ningún registro en CloudWatch | El disparador. Bórralo y vuelve a crearlo (Parte 7) |
| `AccessDeniedException` | Falta el permiso de la Parte 6 |
| `Task timed out after 3.00 seconds` | El tiempo límite. Paso 5.7 |
| El código no hace lo que escribiste | Olvidaste **Deploy**. Paso 5.5 |

---

# Parte 9 · Dejarlo limpio

**5 minutos**

### Lo primero · apagar el disparador

Lambda → `disparar-transacciones` → en el diagrama, clic en **S3** → **Delete**.

> Si queda encendido y alguien sube un archivo, se lanza un trabajo **sin que
> nadie esté mirando**.

### Si ya no vas a seguir

| Qué borrar | Dónde |
|---|---|
| La función | Lambda → marcarla → *Actions → Delete* |
| El trabajo | Glue → *ETL jobs* → marcarlo → *Actions → Delete job* |
| Los archivos | S3 → entrar al bucket → marcar todo → *Delete* |
| El bucket | S3 → marcarlo → *Delete* (tiene que estar vacío) |

### Lo que puedes dejar sin problema

El bucket vacío, el trabajo sin ejecutar, la función sin usar y los roles.
**Nada de eso cuesta si no se ejecuta.**

---

# Para pensar

Respóndete estas tres antes de la próxima sesión:

**1.** El umbral que pusiste era `30`. Si alguien lo cambiara a `90` desde la
consola, ¿qué pasaría con tus datos? ¿Se enteraría alguien?

**2.** Las filas de `quarantine` no se borraron ni se publicaron.
¿Quién debería revisarlas, y cada cuánto?

**3.** Subiste el archivo una vez y funcionó. ¿Qué pasaría si lo subieras
**dos veces seguidas**?

---

# Glosario

| Palabra | Qué significa aquí |
|---|---|
| **Bucket** | Carpeta grande en la nube donde se guardan archivos |
| **Rol** | Identidad que usan los programas, en vez de tu usuario |
| **Política** | Lista de lo que una identidad puede hacer |
| **Trigger** / disparador | La conexión que hace que algo arranque cuando pasa otra cosa |
| **Job** / trabajo | El programa que transforma los datos |
| **Deploy** | Guardar el código de verdad, para que se ejecute |
| **Parquet** | Formato de archivo comprimido, mucho más rápido de consultar que un CSV |
| **Cuarentena** | Carpeta donde van las filas sospechosas, en vez de borrarlas |
