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


// DBTITLE 1,2. Reserva del clúster
//Reserva de recursos
var spark = SparkSession.builder.
appName("Mi Aplicacion").
config("spark.driver.memory", "1g").
config("spark.dynamicAllocation.maxExecutors", "20").
config("spark.executor.cores", "2").
config("spark.executor.memory", "5g").
config("spark.executor.memoryOverhead", "500m").
config("spark.default.parallelism", "100").
enableHiveSupport().
getOrCreate()

// COMMAND ----------

// DBTITLE 1,3. Lectura
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


//PASO 1: Agrupamos los datos según la edad [USO DE RAM: 10 GB]
var df1 = dfPersona.groupBy(dfPersona.col("EDAD")).agg(
	f.count(dfPersona.col("EDAD")).alias("CANTIDAD"), 
	f.min(dfPersona.col("FECHA_INGRESO")).alias("FECHA_CONTRATO_MAS_RECIENTE"), 
	f.sum(dfPersona.col("SALARIO")).alias("SUMA_SALARIOS"), 
	f.max(dfPersona.col("SALARIO")).alias("SALARIO_MAYOR")
)

//Mostramos los datos
df1.show()

+----+--------+---------------------------+-------------+-------------+
|EDAD|CANTIDAD|FECHA_CONTRATO_MAS_RECIENTE|SUMA_SALARIOS|SALARIO_MAYOR|
+----+--------+---------------------------+-------------+-------------+
|  18|       2|                 2001-02-11|      22434.0|      20980.0|
|  19|       4|                 2001-05-19|      48295.0|      23547.0|
|  22|       5|                 2001-01-06|      77911.0|      23820.0|
|  23|       2|                 2005-04-28|      28578.0|      17040.0|
|  24|       4|                 2000-09-18|      49314.0|      19943.0|
|  25|       2|                 2005-06-22|      24288.0|      20573.0|
|  26|       3|                 2006-12-05|      21039.0|      12092.0|
|  27|       5|                 2002-08-01|      53425.0|      16735.0|
|  28|       1|                 2010-06-10|      22037.0|      22037.0|
|  29|       2|                 2000-09-15|      24943.0|      21556.0|
|  30|       2|                 2003-10-19|      24642.0|      16782.0|
|  31|       4|                 2007-07-24|      48089.0|      19522.0|
|  32|       2|                 2004-04-23|      29029.0|      20095.0|
|  33|       3|                 2000-03-17|      48967.0|      20549.0|
|  34|       5|                 2002-04-09|      48144.0|      12423.0|
|  35|       2|                 2000-11-23|      13151.0|       7109.0|
|  37|       1|                 2003-07-18|       6191.0|       6191.0|
|  38|       1|                 2005-10-21|      15116.0|      15116.0|
|  39|       1|                 2008-10-18|       6483.0|       6483.0|
|  41|       4|                 2002-10-10|      48309.0|      22102.0|
+----+--------+---------------------------+-------------+-------------+
only showing top 20 rows


//PASO 2: Filtramos por una EDAD [USO DE RAM: 10 GB]
var df2 = df1.filter(df1.col("EDAD") > 35)

//Mostramos los datos
df2.show()

+----+--------+---------------------------+-------------+-------------+
|EDAD|CANTIDAD|FECHA_CONTRATO_MAS_RECIENTE|SUMA_SALARIOS|SALARIO_MAYOR|
+----+--------+---------------------------+-------------+-------------+
|  37|       1|                 2003-07-18|       6191.0|       6191.0|
|  38|       1|                 2005-10-21|      15116.0|      15116.0|
|  39|       1|                 2008-10-18|       6483.0|       6483.0|
|  41|       4|                 2002-10-10|      48309.0|      22102.0|
|  42|       5|                 2002-05-02|      62417.0|      22038.0|
|  43|       1|                 2010-05-15|      12029.0|      12029.0|
|  45|       1|                 2002-10-31|       2503.0|       2503.0|
|  46|       2|                 2000-04-03|      32820.0|      22953.0|
|  47|       2|                 2001-09-17|      35036.0|      21591.0|
|  48|       2|                 2008-10-14|      29218.0|      24305.0|
|  49|       1|                 2016-08-11|       2669.0|       2669.0|
|  51|       2|                 2011-12-15|      12018.0|       8099.0|
|  52|       3|                 2006-04-21|      32373.0|      14756.0|
|  53|       2|                 2002-04-29|      11753.0|       9469.0|
|  54|       1|                 2017-10-21|       4588.0|       4588.0|
|  55|       2|                 2000-08-16|      23923.0|      13813.0|
|  56|       1|                 2009-05-22|       6515.0|       6515.0|
|  57|       3|                 2005-04-17|      11501.0|       8682.0|
|  58|       3|                 2002-05-31|      45195.0|      23975.0|
|  59|       2|                 2005-03-07|      19889.0|      16289.0|
+----+--------+---------------------------+-------------+-------------+
only showing top 20 rows


//Se llenó la RAM, los siguientes dataframes tratarán de ejecutarse desde la memoria virtual
//La memoria virtual es memoria RAM emulada desde el disco duro
//El problema con el procesamiento con disco duro es que es muy lento (100 veces más lento)
//Generalmente el la memoria virtual es el 10% de la RAM reservada (para el ejemplo 2 GB)
//Si el siguiente paso necesita más de esos 2 GB, el proceso termina colapsando

//PASO 3: Filtramos por SUMA_SALARIOS [10GB: ¡¡¡¡¡¡¡¡COLAPSA POR FALTA DE MEMORIA!!!!!!!!]
//var df3 = df2.filter(df2.col("SUMA_SALARIOS") > 20000)

//Mostramos los datos
//df3.show()


// DBTITLE 1,4. Implementación de patrón checkpoint
//Para evitar el problema anterior, deberemos de forzar la cadena de ejecución de transformations
//Llamaremos al "action" save para ejecutar la cadena de procesos de el último dataframe antes de que colapsara

// COMMAND ----------

//Definimos la ruta en donde almacenaremos el dataframe en disco duro
var carpeta = "dbfs:///FileStore/tmp/df2"


print("Aplicando checkpoint...")

//Almacenamos el dataframe en disco duro para forzar la ejecución de la cadena de procesos que crea el df2
df2.write.mode("overwrite").format("parquet").save(carpeta)

//Eliminamos el dataframe de memoria RAM
df2.unpersist(blocking = true)

println(", checkpoint aplicado!")


//Leemos el archivo generado y lo volvemos a cargar a la variable
//De esta manera la cadena de procesos ya se ejecutó y sólo estamos consulando la resultante desde disco duro
df2 = spark.read.format("parquet").load(carpeta)



//Mostramos los datos, el action "show" no ejecutará una cadena de procesos, ya que lee directamente desde disco duro
df2.show()


// DBTITLE 1,5. Implementación de patrón checkpoint en función re-utilizable
//Implementamos la función checkpoint
def checkpoint(df : DataFrame) : DataFrame = {
  var dfCheckpoint : DataFrame = null
  
  //Generamos un nombre aleatorio para la carpeta entre 0 y 100000000
  var carpeta = "dbfs:///FileStore/tmp/" + (math.random * 100000000).toString
  
  //Guardamos el dataframe en la carpeta para liberar memoria de la cadena de procesos
  print("Aplicando checkpoint...")
  df.write.mode("overwrite").format("parquet").save(carpeta)
  df.unpersist(blocking = true)
  println(", checkpoint aplicado!")
  
  //Volvemos a leerlo
  dfCheckpoint = spark.read.format("parquet").load(carpeta)
  
  return dfCheckpoint
}


//Gracias a la función, podemos aplicar el checkpoint en una línea
df2 = checkpoint(df2)


// DBTITLE 1,6. Ejemplo de uso
//PASO 1: Agrupamos los datos según la edad [USO DE RAM: 10 GB]
var df1 = dfPersona.groupBy(dfPersona.col("EDAD")).agg(
	f.count(dfPersona.col("EDAD")).alias("CANTIDAD"), 
	f.min(dfPersona.col("FECHA_INGRESO")).alias("FECHA_CONTRATO_MAS_RECIENTE"), 
	f.sum(dfPersona.col("SALARIO")).alias("SUMA_SALARIOS"), 
	f.max(dfPersona.col("SALARIO")).alias("SALARIO_MAYOR")
)

//Mostramos los datos
df1.show()



//PASO 2: Filtramos por una EDAD [USO DE RAM: 10 GB]
var df2 = df1.filter(df1.col("EDAD") > 35)

//Mostramos los datos
df2.show()


//Checkpoint
df2 = checkpoint(df2)


//PASO 3: Filtramos por SUMA_SALARIOS [10GB: ¡¡¡¡¡¡¡¡COLAPSA POR FALTA DE MEMORIA!!!!!!!!]
// Si no usaramos un Checkpoint colapsaria, pero como usamos Checkpoint hemos liberado memoria
// y podemos continuar trabajando sobre el df2
var df3 = df2.filter(df2.col("SUMA_SALARIOS") > 20000)

//Mostramos los datos
df3.show()


//PASO 4: Filtramos por SALARIO_MAYOR
var df4 = df3.filter(df3.col("SALARIO_MAYOR") > 1000)

//Mostramos los datos
df4.show()
