// Databricks notebook source


// COMMAND ----------

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

// COMMAND ----------

//PASO 1: Agrupamos los datos según la edad [USO DE RAM: 10 GB]
var df1 = dfPersona.groupBy(dfPersona.col("EDAD")).agg(
	f.count(dfPersona.col("EDAD")).alias("CANTIDAD"), 
	f.min(dfPersona.col("FECHA_INGRESO")).alias("FECHA_CONTRATO_MAS_RECIENTE"), 
	f.sum(dfPersona.col("SALARIO")).alias("SUMA_SALARIOS"), 
	f.max(dfPersona.col("SALARIO")).alias("SALARIO_MAYOR")
)

//Mostramos los datos
df1.show()

// COMMAND ----------

//PASO 2: Filtramos por una EDAD [USO DE RAM: 10 GB]
var df2 = df1.filter(df1.col("EDAD") > 35)

//Mostramos los datos
df2.show()

// COMMAND ----------

//Se llenó la RAM, los siguientes dataframes tratarán de ejecutarse desde la memoria virtual
//La memoria virtual es memoria RAM emulada desde el disco duro
//El problema con el procesamiento con disco duro es que es muy lento (100 veces más lento)
//Generalmente el la memoria virtual es el 10% de la RAM reservada (para el ejemplo 2 GB)
//Si el siguiente paso necesita más de esos 2 GB, el proceso termina colapsando

//PASO 3: Filtramos por SUMA_SALARIOS [10GB: ¡¡¡¡¡¡¡¡COLAPSA POR FALTA DE MEMORIA!!!!!!!!]
//var df3 = df2.filter(df2.col("SUMA_SALARIOS") > 20000)

//Mostramos los datos
//df3.show()

// COMMAND ----------

// DBTITLE 1,4. Implementación de patrón checkpoint
//Para evitar el problema anterior, deberemos de forzar la cadena de ejecución de transformations
//Llamaremos al "action" save para ejecutar la cadena de procesos de el último dataframe antes de que colapsara

// COMMAND ----------

//Definimos la ruta en donde almacenaremos el dataframe en disco duro
var carpeta = "dbfs:///FileStore/tmp/df2"

// COMMAND ----------

print("Aplicando checkpoint...")

//Almacenamos el dataframe en disco duro para forzar la ejecución de la cadena de procesos que crea el df2
df2.write.mode("overwrite").format("parquet").save(carpeta)

//Eliminamos el dataframe de memoria RAM
df2.unpersist(blocking = true)

println(", checkpoint aplicado!")

// COMMAND ----------

//Leemos el archivo generado y lo volvemos a cargar a la variable
//De esta manera la cadena de procesos ya se ejecutó y sólo estamos consulando la resultante desde disco duro
df2 = spark.read.format("parquet").load(carpeta)

// COMMAND ----------

//Mostramos los datos, el action "show" no ejecutará una cadena de procesos, ya que lee directamente desde disco duro
df2.show()

// COMMAND ----------

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

// COMMAND ----------

//Gracias a la función, podemos aplicar el checkpoint en una línea
df2 = checkpoint(df2)

// COMMAND ----------

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

// COMMAND ----------

//PASO 2: Filtramos por una EDAD [USO DE RAM: 10 GB]
var df2 = df1.filter(df1.col("EDAD") > 35)

//Mostramos los datos
df2.show()

// COMMAND ----------

//Checkpoint
df2 = checkpoint(df2)

// COMMAND ----------

//PASO 3: Filtramos por SUMA_SALARIOS [10GB: ¡¡¡¡¡¡¡¡COLAPSA POR FALTA DE MEMORIA!!!!!!!!]
var df3 = df2.filter(df2.col("SUMA_SALARIOS") > 20000)

//Mostramos los datos
df3.show()

// COMMAND ----------

//PASO 4: Filtramos por SALARIO_MAYOR
var df4 = df3.filter(df3.col("SALARIO_MAYOR") > 1000)

//Mostramos los datos
df4.show()
