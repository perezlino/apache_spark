// Databricks notebook source


// DBTITLE 1,1. Librerías
//Objetos para definir la metadata
import org.apache.spark.sql.types.{StructType, StructField}

//Importamos los tipos de datos que usaremos
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}

//Podemos importar todos los utilitarios con la siguiente sentencia
import org.apache.spark.sql.types._

//Importamos todos los objetos utilitarios dentro de una variable
import org.apache.spark.sql.{functions => f}

//Importamos las librerías para implementar UDFs
import org.apache.spark.sql.functions.udf

//Reserva de recursos computacionales
import org.apache.spark.sql.SparkSession

//Importamos la librería que define el tipo de dato de un Dataframe
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// DBTITLE 1,2. Lectura
//Leemos el archivo de persona
var dfPersona = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        Array(
            StructField("ID", StringType, true),
            StructField("NOMBRE", StringType, true),
            StructField("TELEFONO", StringType, true),
            StructField("CORREO", StringType, true),
            StructField("FECHA_INGRESO", StringType, true),
            StructField("EDAD", IntegerType, true),
            StructField("SALARIO", DoubleType, true),
            StructField("ID_EMPRESA", StringType, true)
        )
    )
).load("dbfs:///FileStore/_spark/persona.data")

//Mostramos los datos
dfPersona.show()

+---+---------+--------------+--------------------+-------------+----+-------+----------+
| ID|   NOMBRE|      TELEFONO|              CORREO|FECHA_INGRESO|EDAD|SALARIO|ID_EMPRESA|
+---+---------+--------------+--------------------+-------------+----+-------+----------+
|  1|     Carl|1-745-633-9145|arcu.Sed.et@ante....|   2004-04-23|  32|20095.0|         5|
|  2|Priscilla|      155-2498|Donec.egestas.Ali...|   2019-02-17|  34| 9298.0|         2|
|  3|  Jocelyn|1-204-956-8594|amet.diam@loborti...|   2002-08-01|  27|10853.0|         3|
|  4|    Aidan|1-719-862-9385|euismod.et.commod...|   2018-11-06|  29| 3387.0|        10|
|  5|  Leandra|      839-8044|at@pretiumetrutru...|   2002-10-10|  41|22102.0|         1|
|  6|     Bert|      797-4453|a.felis.ullamcorp...|   2017-04-25|  70| 7800.0|         7|
|  7|     Mark|1-680-102-6792|Quisque.ac@placer...|   2006-04-21|  52| 8112.0|         5|
|  8|    Jonah|      214-2975|eu.ultrices.sit@v...|   2017-10-07|  23|17040.0|         5|
|  9|    Hanae|      935-2277|          eu@Nunc.ca|   2003-05-25|  69| 6834.0|         3|
| 10|   Cadman|1-866-561-2701|orci.adipiscing.n...|   2001-05-19|  19| 7996.0|         7|
| 11|  Melyssa|      596-7736|vel@vulputateposu...|   2008-10-14|  48| 4913.0|         8|
| 12|   Tanner|1-739-776-7897|arcu.Aliquam.ultr...|   2011-05-10|  24|19943.0|         8|
| 13|   Trevor|      512-1955|Nunc.quis.arcu@eg...|   2010-08-06|  34| 9501.0|         5|
| 14|    Allen|      733-2795|felis.Donec@necle...|   2005-03-07|  59|16289.0|         2|
| 15|    Wanda|      359-6973|Nam.nulla.magna@I...|   2005-08-21|  27| 1539.0|         5|
| 16|    Alden|      341-8522|odio@morbitristiq...|   2006-12-05|  26| 3377.0|         2|
| 17|     Omar|      720-1543|Phasellus.vitae.m...|   2014-06-24|  60| 6851.0|         6|
| 18|     Owen|1-167-335-7541|     sociis@erat.com|   2002-04-09|  34| 4759.0|         7|
| 19|    Laura|1-974-623-2057|    mollis@ornare.ca|   2017-03-09|  70|17403.0|         4|
| 20|    Emery|1-672-840-0264|     at.nisi@vel.org|   2004-02-27|  24|18752.0|         9|
+---+---------+--------------+--------------------+-------------+----+-------+----------+
only showing top 20 rows


//Leemos las transacciones
var dfTransaccion = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(
    StructType(
        Array(
            StructField("ID_PERSONA", StringType, true),
            StructField("ID_EMPRESA", StringType, true),
            StructField("MONTO", DoubleType, true),
            StructField("FECHA", StringType, true)
        )
    )
).load("dbfs:///FileStore/_spark/transaccion")

//Mostramos los datos
dfTransaccion.show()

+----------+----------+------+----------+
|ID_PERSONA|ID_EMPRESA| MONTO|     FECHA|
+----------+----------+------+----------+
|        26|         7|2244.0|2021-01-23|
|        24|         6|1745.0|2021-01-23|
|         1|        10| 238.0|2021-01-23|
|        19|         2|3338.0|2021-01-23|
|        95|         8|2656.0|2021-01-23|
|        37|         6|3811.0|2021-01-23|
|        40|         2| 297.0|2021-01-23|
|        27|         3|2896.0|2021-01-23|
|        65|         2|4097.0|2021-01-23|
|        21|         8|2799.0|2021-01-23|
|        71|         8|3148.0|2021-01-23|
|        71|         4|2712.0|2021-01-23|
|        34|         6| 513.0|2021-01-23|
|        71|         5|1548.0|2021-01-23|
|        81|         3|1343.0|2021-01-23|
|        90|         1|4224.0|2021-01-23|
|        87|         8|1571.0|2021-01-23|
|        82|         4|3411.0|2021-01-23|
|        61|         4| 871.0|2021-01-23|
|        48|         6| 783.0|2021-01-23|
+----------+----------+------+----------+
only showing top 20 rows


// DBTITLE 1,3. Implementemos un proceso que crea tres reportes
//Seleccionamos las personas con más de 30 años
var df1 = dfPersona.filter(dfPersona.col("EDAD") > 30)

//Seleccionamos las transacciones con más de 1000 dólares
var df2 = dfTransaccion.filter(dfTransaccion.col("MONTO") > 1000)

//Cruzamos las transacciones con los datos de las personas
var df3 = df2.join(
  df1, 
  df2.col("ID_PERSONA") === df1.col("ID"),
  "inner"
).select(
  df1.col("NOMBRE"), 
  df1.col("EDAD"), 
  df1.col("SALARIO"), 
  df2.col("MONTO"), 
  df2.col("FECHA")
)

//Filtramos las transacciones que sean del día "2021-01-23"
var df4 = df3.filter(df3.col("FECHA") === "2021-01-23")

//Mostramos los datos
df4.show()

+---------+----+-------+------+----------+
|   NOMBRE|EDAD|SALARIO| MONTO|     FECHA|
+---------+----+-------+------+----------+
|  Brenden|  33|20549.0|2244.0|2021-01-23|
|    Laura|  70|17403.0|3338.0|2021-01-23|
|    Jayme|  58|23975.0|2656.0|2021-01-23|
|     Inga|  41| 8562.0|3811.0|2021-01-23|
|Alexander|  55|13813.0|2896.0|2021-01-23|
|    Nehru|  34|12423.0|4097.0|2021-01-23|
|  Carissa|  31| 1952.0|2799.0|2021-01-23|
|Sigourney|  42| 9145.0|4224.0|2021-01-23|
|    Cally|  67|24575.0|3411.0|2021-01-23|
| Tallulah|  46| 9867.0|1580.0|2021-01-23|
|  Brenden|  33|20549.0|2628.0|2021-01-23|
|Alexander|  55|13813.0|1436.0|2021-01-23|
|Sigourney|  42| 9145.0|3483.0|2021-01-23|
|     Ross|  67|14285.0|3342.0|2021-01-23|
|    Damon|  49| 2669.0|3478.0|2021-01-23|
|  Carissa|  31| 1952.0|4261.0|2021-01-23|
|  Carolyn|  64|22838.0|3052.0|2021-01-23|
|    Keith|  33|13348.0|4071.0|2021-01-23|
|   Gisela|  67| 6497.0|1601.0|2021-01-23|
|    Nehru|  34|12423.0|3123.0|2021-01-23|
+---------+----+-------+------+----------+
only showing top 20 rows


//Para entender este tunning, supongamos que esta cadena de procesos hasta el df4 demora 4 horas
// E imaginemos que se tiene el Garbage Collector activado e irá borrando todo desde el df1 hasta el final de
// cada subproceso. Por ejemplo, Se ejecutará desde df1 hasta dfReporte1 y luego el Garbage Collector borrara 
// todo. Luego para ejecutar dfReporte2 ejecutará nuevamente el primer subproceso (df1 hasta dfReporte1) y
// luego el segundo subproceso desde df1 hasta dfReporte2 y asi sucesivamente. En total sumando 12 horas 30 minutos


//Reporte 1: Transacciones realizadas por personas entre 30 a 39 años
//TIEMPO DE PROCESAMIENTO: 4 HORAS (df4) + 10 MIN (filter) = 4 HORAS CON 10 MINUTOS
var dfReporte1 = df4.filter(
  (df4.col("EDAD") >= 30) &&
  (df4.col("EDAD") < 40)
)

//Mostramos los datos
dfReporte1.show()

+-------+----+-------+------+----------+
| NOMBRE|EDAD|SALARIO| MONTO|     FECHA|
+-------+----+-------+------+----------+
|Brenden|  33|20549.0|2244.0|2021-01-23|
|  Nehru|  34|12423.0|4097.0|2021-01-23|
|Carissa|  31| 1952.0|2799.0|2021-01-23|
|Brenden|  33|20549.0|2628.0|2021-01-23|
|Carissa|  31| 1952.0|4261.0|2021-01-23|
|  Keith|  33|13348.0|4071.0|2021-01-23|
|  Nehru|  34|12423.0|3123.0|2021-01-23|
|   Ross|  31|19092.0|1377.0|2021-01-23|
|   Abel|  33|15070.0|3141.0|2021-01-23|
|   Carl|  32|20095.0|3120.0|2021-01-23|
|   Igor|  37| 6191.0|2360.0|2021-01-23|
|Brenden|  33|20549.0|2702.0|2021-01-23|
|  Wynne|  31|19522.0|4066.0|2021-01-23|
|   Jana|  39| 6483.0|1554.0|2021-01-23|
|  Nehru|  34|12423.0|4033.0|2021-01-23|
|  Hayes|  31| 7523.0|3928.0|2021-01-23|
|   Owen|  34| 4759.0|3316.0|2021-01-23|
|   Jana|  39| 6483.0|2442.0|2021-01-23|
|   Igor|  37| 6191.0|2558.0|2021-01-23|
| Amelia|  35| 6042.0|1239.0|2021-01-23|
+-------+----+-------+------+----------+
only showing top 20 rows


//Reporte 2: Transacciones realizadas por personas entre 40 a 59 años
//TIEMPO DE PROCESAMIENTO: 4 HORAS (df4) + 10 MIN (filter) = 4 HORAS CON 10 MINUTOS
var dfReporte2 = df4.filter(
  (df4.col("EDAD") >= 40) &&
  (df4.col("EDAD") < 60)
)

//Mostramos los datos
dfReporte2.show()

+---------+----+-------+------+----------+
|   NOMBRE|EDAD|SALARIO| MONTO|     FECHA|
+---------+----+-------+------+----------+
|    Jayme|  58|23975.0|2656.0|2021-01-23|
|     Inga|  41| 8562.0|3811.0|2021-01-23|
|Alexander|  55|13813.0|2896.0|2021-01-23|
|Sigourney|  42| 9145.0|4224.0|2021-01-23|
| Tallulah|  46| 9867.0|1580.0|2021-01-23|
|Alexander|  55|13813.0|1436.0|2021-01-23|
|Sigourney|  42| 9145.0|3483.0|2021-01-23|
|    Damon|  49| 2669.0|3478.0|2021-01-23|
|   Philip|  51| 3919.0|1603.0|2021-01-23|
|  Giselle|  45| 2503.0|2233.0|2021-01-23|
|     Amos|  42|15855.0|2887.0|2021-01-23|
|     Irma|  58| 6747.0|1328.0|2021-01-23|
|     Yuli|  57| 1336.0|2177.0|2021-01-23|
|    Fiona|  42| 9960.0|1429.0|2021-01-23|
|      Jin|  42|22038.0|2972.0|2021-01-23|
|    Keely|  41|10373.0|1895.0|2021-01-23|
|    Allen|  59|16289.0|3385.0|2021-01-23|
|    Nyssa|  53| 2284.0|3301.0|2021-01-23|
|    Ebony|  59| 3600.0|3514.0|2021-01-23|
|  Cynthia|  57| 8682.0|2698.0|2021-01-23|
+---------+----+-------+------+----------+
only showing top 20 rows


//Reporte 3: Transacciones realizadas por personas de 60 en adelante
//TIEMPO DE PROCESAMIENTO: 4 HORAS (df4) + 10 MIN (filter) = 4 HORAS CON 10 MINUTOS
var dfReporte3 = df4.filter(
  (df4.col("EDAD") >= 60)
)

//Mostramos los datos
dfReporte3.show()

+--------+----+-------+------+----------+
|  NOMBRE|EDAD|SALARIO| MONTO|     FECHA|
+--------+----+-------+------+----------+
|   Laura|  70|17403.0|3338.0|2021-01-23|
|   Cally|  67|24575.0|3411.0|2021-01-23|
|    Ross|  67|14285.0|3342.0|2021-01-23|
| Carolyn|  64|22838.0|3052.0|2021-01-23|
|  Gisela|  67| 6497.0|1601.0|2021-01-23|
|    Omar|  60| 6851.0|3162.0|2021-01-23|
|   Cally|  67|24575.0|1129.0|2021-01-23|
|   Hanae|  69| 6834.0|1916.0|2021-01-23|
|Jennifer|  64|19013.0|3429.0|2021-01-23|
|    Ross|  67|14285.0|2114.0|2021-01-23|
|   Hanae|  69| 6834.0|4129.0|2021-01-23|
|   Yetta|  61|21452.0|3707.0|2021-01-23|
|  Gisela|  67| 6497.0|4020.0|2021-01-23|
|    Bert|  70| 7800.0|1575.0|2021-01-23|
| Carolyn|  64|22838.0|1738.0|2021-01-23|
|  Gisela|  67| 6497.0|1676.0|2021-01-23|
|  Gisela|  67| 6497.0|4493.0|2021-01-23|
|    Omar|  60| 6851.0|2067.0|2021-01-23|
|    Bert|  70| 7800.0|2810.0|2021-01-23|
|   Yetta|  61|21452.0|4064.0|2021-01-23|
+--------+----+-------+------+----------+
only showing top 20 rows


//TIEMPO TOTAL = 12 HORAS CON 30 MINUTOS


// DBTITLE 1,4. Optimización caché
//Vemos que el "df4" es usado para calcular diferentes cadenas de procesos
//Se estará recalculando constanteneme
//Vamos a indicar que sólo se cree la primera vez que se le referencia
//Luego, si otra cadena de procesos lo referencia, ya no se volverá a recalcular

//Seleccionamos las personas con más de 30 años
var df1 = dfPersona.filter(dfPersona.col("EDAD") > 30)

//Seleccionamos las transacciones con más de 1000 dólares
var df2 = dfTransaccion.filter(dfTransaccion.col("MONTO") > 1000)

//Cruzamos las transacciones con los datos de las personas
var df3 = df2.join(
  df1, 
  df2.col("ID_PERSONA") === df1.col("ID"),
  "inner"
).select(
  df1.col("NOMBRE"), 
  df1.col("EDAD"), 
  df1.col("SALARIO"), 
  df2.col("MONTO"), 
  df2.col("FECHA")
)

//Filtramos las transacciones que sean del día "2021-01-23"
var df4 = df3.filter(df3.col("FECHA") === "2021-01-23")

//Mostramos los datos
df4.show()

//Para entender este tunning, supongamos que esta cadena de procesos hasta el df4 demora 4 horas


//Lo guardamos en la caché
df4.cache()


//Reporte 1: Transacciones realizadas por personas entre 30 a 39 años
//TIEMPO DE PROCESAMIENTO: 4 HORAS (df4) + 10 MIN (filter) = 4 HORAS CON 10 MINUTOS
var dfReporte1 = df4.filter(
  (df4.col("EDAD") >= 30) &&
  (df4.col("EDAD") < 40)
)

//Mostramos los datos
dfReporte1.show()

//Reporte 2: Transacciones realizadas por personas entre 40 a 59 años
//TIEMPO DE PROCESAMIENTO: 10 MIN (filter)
var dfReporte2 = df4.filter(
  (df4.col("EDAD") >= 40) &&
  (df4.col("EDAD") < 60)
)

//Mostramos los datos
dfReporte2.show()

//Reporte 3: Transacciones realizadas por personas de 60 en adelante
//TIEMPO DE PROCESAMIENTO: 10 MIN (filter)
var dfReporte3 = df4.filter(
  (df4.col("EDAD") >= 60)
)

//Mostramos los datos
dfReporte3.show()


//TIEMPO TOTAL = 4 HORAS CON 30 MINUTOS


// DBTITLE 1,5. Funciones utilitarias
//FUNCION PARA ALMACENAR EN CACHÉ UN DATAFRAME
def cache(df : DataFrame) = {
  print("Almacenando en cache...")
  df.cache()
  println(", almacenado en cache!")
}


//Ejemplo de uso
cache(df4)


//FUNCION PARA LIBERAR DEL CACHÉ UN DATAFRAME
def liberarCache(df : DataFrame) = {
  print("Liberando cache...")
  df.unpersist(blocking = true)
  println(", cache liberado!")
}


//Ejemplo de uso
liberarCache(df4)


//FUNCION PARA LIBERAR TODOS LOS DATAFRAMES ALMACENADOS EN EL CACHE
def liberarTodoElCache(spark : SparkSession) = {
  print("Liberando todo el cache...")
  spark.sqlContext.clearCache()
  println(", todo el cache liberado!")
}


//Ejemplo de uso
liberarTodoElCache(spark)
