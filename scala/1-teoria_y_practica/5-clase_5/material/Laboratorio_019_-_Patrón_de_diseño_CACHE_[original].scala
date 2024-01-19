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

// COMMAND ----------

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

// COMMAND ----------

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

//Para entender este tunning, supongamos que esta cadena de procesos hasta el df4 demora 4 horas

// COMMAND ----------

//Reporte 1: Transacciones realizadas por personas entre 30 a 39 años
//TIEMPO DE PROCESAMIENTO: 4 HORAS (df4) + 10 MIN (filter) = 4 HORAS CON 10 MINUTOS
var dfReporte1 = df4.filter(
  (df4.col("EDAD") >= 30) &&
  (df4.col("EDAD") < 40)
)

//Mostramos los datos
dfReporte1.show()

// COMMAND ----------

//Reporte 2: Transacciones realizadas por personas entre 40 a 59 años
//TIEMPO DE PROCESAMIENTO: 4 HORAS (df4) + 10 MIN (filter) = 4 HORAS CON 10 MINUTOS
var dfReporte2 = df4.filter(
  (df4.col("EDAD") >= 40) &&
  (df4.col("EDAD") < 60)
)

//Mostramos los datos
dfReporte2.show()

// COMMAND ----------

//Reporte 3: Transacciones realizadas por personas de 60 en adelante
//TIEMPO DE PROCESAMIENTO: 4 HORAS (df4) + 10 MIN (filter) = 4 HORAS CON 10 MINUTOS
var dfReporte3 = df4.filter(
  (df4.col("EDAD") >= 60)
)

//Mostramos los datos
dfReporte3.show()

// COMMAND ----------

//TIEMPO TOTAL = 12 HORAS CON 30 MINUTOS

// COMMAND ----------

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

// COMMAND ----------

//Lo guardamos en la caché
df4.cache()

// COMMAND ----------

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

// COMMAND ----------

//TIEMPO TOTAL = 4 HORAS CON 30 MINUTOS

// COMMAND ----------

// DBTITLE 1,5. Funciones utilitarias
//FUNCION PARA ALMACENAR EN CACHÉ UN DATAFRAME
def cache(df : DataFrame) = {
  print("Almacenando en cache...")
  df.cache()
  println(", almacenado en cache!")
}

// COMMAND ----------

//Ejemplo de uso
cache(df4)

// COMMAND ----------

//FUNCION PARA LIBERAR DEL CACHÉ UN DATAFRAME
def liberarCache(df : DataFrame) = {
  print("Liberando cache...")
  df.unpersist(blocking = true)
  println(", cache liberado!")
}

// COMMAND ----------

//Ejemplo de uso
liberarCache(df4)

// COMMAND ----------

//FUNCION PARA LIBERAR TODOS LOS DATAFRAMES ALMACENADOS EN EL CACHE
def liberarTodoElCache(spark : SparkSession) = {
  print("Liberando todo el cache...")
  spark.sqlContext.clearCache()
  println(", todo el cache liberado!")
}

// COMMAND ----------

//Ejemplo de uso
liberarTodoElCache(spark)
