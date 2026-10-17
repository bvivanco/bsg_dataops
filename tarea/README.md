# Tarea · Cadena de frío
### Monta tú solo un pipeline completo en AWS

**Entrega: antes de la próxima sesión** · Tiempo estimado: 2 horas

---

## El caso

Una cadena de supermercados tiene sensores en sus cámaras frigoríficas. Cada
sensor manda lecturas de **temperatura** y **humedad**.

Si una cámara sube de temperatura y nadie se entera, se pierde la mercancía.
Tu trabajo es montar el pipeline que vigila eso.

```
  llega el .csv        S3              Lambda            Glue             S3           Athena
  ───────────▶    raw/sensores/  ──▶  dispara  ──▶   transforma  ──▶  curated/  ──▶  consultas
```

## Lo que tienes que entregar

**Un pipeline que funcione solo**, igual que el del laboratorio pero con datos
distintos, **más las consultas en Athena** que demuestren que funcionó.

---

## Los datos · son DOS proveedores

La cadena tiene sensores de dos fabricantes distintos. **Mandan lo mismo, pero
cada uno a su manera.**

| Archivo | Proveedor | Formato | Lecturas |
|---|---|---|---|
| **[`lecturas_sensores.csv`](lecturas_sensores.csv)** | El antiguo | CSV | 37 |
| **[`lecturas_sensores.json`](lecturas_sensores.json)** | El nuevo | JSON Lines | 15 |

**52 lecturas en total.**

### Los mismos datos, otros nombres

Esto es lo que vas a tener que resolver:

| CSV | JSON | Qué es |
|---|---|---|
| `lectura_id` | `id` | Identificador de la lectura |
| `sensor_id` | `sensor` | Qué sensor la tomó |
| `sucursal` | `local` | En qué local está |
| `fecha_lectura` | `ts` | Cuándo se tomó |
| `temperatura_c` | `temp` | Grados centígrados |
| `humedad_pct` | `hum` | Porcentaje de humedad |

> **Fíjate en lo que NO cambia: las reglas de calidad.**
>
> Una temperatura de -19 grados es válida venga del CSV o del JSON. Lo que
> cambia es la **traducción de la entrada**, no el criterio.
>
> Por eso lo primero que hace tu pipeline es **unificar las dos fuentes a un
> solo esquema**. Después, las reglas se aplican una sola vez.

### ⚠️ Ábrelo y míralo antes de escribir nada

Tiene problemas puestos a propósito. **Encuéntralos tú.** Algunos pistas:

- La misma sucursal está escrita de **varias formas distintas**, en los dos archivos
- Las fechas vienen en **tres formatos mezclados** — dos en el CSV y otro en el JSON
- Hay temperaturas que **no son números**
- Hay temperaturas que sí son números, pero **ningún sensor real las daría**
- Falta algún identificador
- Hay una lectura **repetida**

> **Cuidado con una trampa:** en el laboratorio, un número negativo era
> sospechoso. Aquí **no**. Una cámara de congelados tiene que estar a **-20
> grados**: lo raro sería un número positivo.
>
> **No copies las reglas del laboratorio.** Piensa estas.

---

## Lo que tiene que hacer tu pipeline

### 1 · Unificar las dos fuentes

Lee el CSV y el JSON, renombra las columnas del JSON para que coincidan, y
únelos en un solo conjunto de datos.

> Pista: `unionByName()` empareja por **nombre** de columna.
> `union()` a secas empareja por **posición**, y si los dos orígenes no están
> en el mismo orden te mezcla columnas sin avisar.

Añade una columna `_origen` con el valor `csv` o `json`, para saber de dónde
vino cada lectura.

### 2 · Castear los tipos

Cada columna tiene que llegar con su tipo correcto, no como texto.
Piensa cuál corresponde a cada una, y por qué.

### 3 · Limpiar

- Dejar la sucursal escrita siempre igual
- Convertir las fechas de los **dos formatos** a un solo tipo
- Separar las lecturas en tres destinos:

| Destino | Cuáles | Qué se hace |
|---|---|---|
| **Rechazo** | Sin `lectura_id` o sin `sensor_id` | Se descartan |
| **Cuarentena** | Temperatura ilegible, o fuera del rango **-40 a +40** | Se guardan para revisar |
| **Limpias** | El resto | Se publican |

Los motivos de cuarentena tienen que llamarse exactamente
`temperatura_ilegible` y `sensor_averiado`.

### 4 · Transformar

Añade una columna **`estado_cadena`**:

| Temperatura | Estado |
|---|---|
| `<= -18` | `OK` |
| entre `-18` y `-12` | `ALERTA` |
| `> -12` | `CRITICO` |

Y una columna **`fecha`** con solo la fecha, sin la hora. Es la que usarás
para particionar.

---

## Por dónde empezar

Tienes un **esqueleto** con la estructura ya montada:
**[`job_sensores_ESQUELETO.py`](job_sensores_ESQUELETO.py)**

Lleva **nueve bloques marcados con `TODO`** que tienes que escribir tú.
Lo demás (lectura, escritura, métricas, deduplicación) ya está hecho.

> Está comentado con pistas, no con respuestas. Si te atascas más de veinte
> minutos en un bloque, escribe en el foro: probablemente no eres el único.

---

## Las actividades · las mismas del laboratorio

Si hiciste el laboratorio, ya sabes hacer todo esto. Si te trabas, vuelve a
**[el manual del laboratorio](../README.md)** y mira la parte equivalente.

| # | Qué hacer | Equivale a |
|---|---|---|
| 1 | Crear el bucket con sus carpetas, usando `sensores` en vez de `transacciones` | Parte 1 |
| 2 | Subir **los dos archivos**: el CSV a `raw/sensores/csv/` y el JSON a `raw/sensores/json/` | Parte 2 |
| 3 | Crear el rol de Glue (**si ya lo tienes del laboratorio, reutilízalo**) | Parte 3 |
| 4 | Crear el trabajo de Glue con tu código completado | Parte 4 |
| 5 | Crear la función Lambda que lo dispare | Parte 5 |
| 6 | Darle permiso para lanzar Glue | Parte 6 |
| 7 | Conectar el evento de S3 | Parte 7 |
| 8 | Probarlo: subir el archivo y que todo pase solo | Parte 8 |
| 9 | **Consultar los resultados con Athena** | Parte 9 |

> El trabajo de Glue se tiene que llamar `sensores-etl`, y la función
> `disparar-sensores`. Así los nombres no chocan con los del laboratorio.

---

## Las consultas de Athena

Cataloga `curated/sensores/` con un rastreador (Parte 9 del manual) y ejecuta
**estas tres consultas**. Son las que vamos a revisar.

### Consulta 1 · Cuántas lecturas hay de cada estado

```sql
SELECT estado_cadena, COUNT(*) AS lecturas
FROM tu_base.sensores
GROUP BY estado_cadena
ORDER BY lecturas DESC;
```

### Consulta 2 · Las sucursales con problemas

```sql
SELECT sucursal,
       COUNT(*) AS total,
       SUM(CASE WHEN estado_cadena = 'CRITICO' THEN 1 ELSE 0 END) AS criticas
FROM tu_base.sensores
GROUP BY sucursal
ORDER BY criticas DESC;
```

### Consulta 3 · ¿De dónde vino cada lectura?

```sql
SELECT _origen, COUNT(*) AS lecturas
FROM tu_base.sensores
GROUP BY _origen;
```

> Si solo aparece uno de los dos orígenes, la unión no funcionó.

### Consulta 4 · La comprobación de calidad

```sql
SELECT COUNT(*) AS filas,
       COUNT(DISTINCT lectura_id) AS ids_unicos,
       MIN(temperatura_c) AS temp_minima,
       MAX(temperatura_c) AS temp_maxima
FROM tu_base.sensores;
```

> **En la tercera, `filas` e `ids_unicos` tienen que dar el mismo número.**
> Si no coinciden, la deduplicación no funcionó.
>
> Y `temp_minima` y `temp_maxima` tienen que estar dentro de -40 y +40.
> Si se sale alguna, la regla de cuarentena no funcionó.

---

## Qué entregar

Sube al LMS **un documento** con estas siete capturas:

| # | Captura de | Qué tiene que verse |
|---|---|---|
| 1 | Tu trabajo de Glue, pestaña **Runs** | Una ejecución en `Succeeded` que tú **no lanzaste** |
| 2 | Los **registros de CloudWatch** de tu Lambda | El mensaje de que llegó el archivo y lanzó el trabajo |
| 3 | El archivo **`metrics.json`** de `_metrics/` | Abierto, con los números |
| 4 | **Athena, consulta 1** | El resultado |
| 5 | **Athena, consulta 2** | El resultado |
| 6 | **Athena, consulta 3** | El resultado |
| 7 | **Athena, consulta 4** | El resultado |

Y **tu archivo `.py` completado**.

### Más tres preguntas, en un párrafo cada una

**1.** En el laboratorio, un importe negativo iba a cuarentena. Aquí, una
temperatura negativa es lo normal. ¿Qué dice eso sobre dónde viven de verdad
las reglas de calidad?

**2.** Mira el resultado de la consulta 2. Si fueras el responsable de
operaciones, ¿qué harías mañana por la mañana?

**3.** Tu pipeline manda las lecturas raras a `quarantine/` en vez de borrarlas.
¿Quién debería revisar esa carpeta, y cada cuánto? ¿Qué pasa si nadie lo hace?

**4.** Mañana aparece un tercer proveedor con otro formato más. ¿Qué parte de
tu código tendrías que tocar, y qué parte se quedaría igual? ¿Qué te dice eso
sobre dónde conviene poner las reglas de negocio?

---

## Cómo se califica

| | Peso |
|---|---|
| El pipeline funciona de punta a punta, sin que nadie lo lance | 25 % |
| **Las dos fuentes se unifican correctamente** | **15 %** |
| El casteo de tipos es correcto y está justificado | 10 % |
| Las reglas de calidad separan bien los tres destinos | 15 % |
| La transformación `estado_cadena` es correcta | 15 % |
| Las cuatro consultas de Athena funcionan y los números cuadran | 10 % |
| Las cuatro preguntas escritas | 10 % |

> **Lo que más pesa no es que el código sea bonito, sino que el pipeline
> funcione sin ti.** Es la diferencia entre un análisis y un pipeline.

---

## 🧹 Y al terminar, borra

Lo mismo que en el laboratorio (Parte 10):

1. **Desactiva el disparador** de la Lambda ← lo primero
2. Borra el rastreador y la base del catálogo
3. Borra el trabajo, la función y el bucket si ya no los necesitas

> Un rastreador programado o un disparador olvidado siguen generando costo.

---

## Si te atascas

| Problema | Dónde mirar |
|---|---|
| No sé qué tipo poner en el esquema | Bloque 1 del esqueleto, tiene pistas |
| Las fechas me salen todas en NULL | Hay **tres** formatos. Pista: encadena tres `to_timestamp` dentro de un `F.coalesce()` |
| Al unir las fuentes se me mezclan las columnas | Usa `unionByName()`, no `union()` |
| El JSON me sale con una sola columna rara | El archivo es JSON Lines, un objeto por línea. `spark.read.json()` lo lee así por defecto |
| La sucursal no se normaliza | `F.trim()`, `F.upper()`, `F.regexp_replace()` |
| El trabajo falla y no entiendo el error | Glue → la ejecución → **Error logs** |
| Subo el archivo y no pasa nada | Falta el permiso de invocación. Manual, Parte 7 |
| Athena no me deja ejecutar | Falta la carpeta de resultados. Manual, Parte 9.1 |

**Y si sigues trabado:** escribe en el foro del LMS con la captura del error.

---

## ¿Dudas?

Escríbeme a **bryan.vivanco@hotmail.com** con:

- Qué paso estás haciendo
- La captura del error completo
- Qué has probado ya

Cuanto más concreto, más rápido te respondo. Y si la duda es general, mejor
en el foro del LMS: así le sirve a todos.
