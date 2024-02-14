// Databricks notebook source


// DBTITLE 1,1. Librerías
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame



// DBTITLE 1,2. Conexión a tópico real-time basado en Kafka
//Cambiar el valor por la IP asignada
var clusterKafka = "localhost:9092"



//Nos conectamos a un tópico que tenga interfaz Kafka
var dfStream = spark.readStream.format("kafka").
option("kafka.bootstrap.servers", clusterKafka). //Dirección y puerto del tópico
option("subscribe", "transaccion"). //Nombre del tópico
option("startingOffsets", "latest"). //Desde donde extraemos los datos del tópico
load()



//Importamos la librería para definir el tiempo de procesamiento
import org.apache.spark.sql.streaming.Trigger.ProcessingTime



//Vemos los datos
display(dfStream)



// DBTITLE 1,3. Extracción de datos binarizados
//Extraemos la columna "value" y casteamos el binario a "string"
var df1 = dfStream.select(
  dfStream.col("value").cast("string")
)



//Vemos los datos
display(df1)



// DBTITLE 1,4. Conversión a JSON
//Definimos el esquema JSON
var schema = StructType(Seq(
  StructField("PERSONA", StructType(Seq(
      StructField("ID_PERSONA", StringType, true),
      StructField("NOMBRE_PERSONA", StringType, true),
      StructField("EDAD", IntegerType, true),
      StructField("SALARIO", DoubleType, true),
      StructField("CONTACTO", ArrayType(StructType(Seq(
          StructField("PREFIJO", StringType, true),
          StructField("TELEFONO", StringType, true)
      ))), true)
  )), true),
  StructField("EMPRESA", StructType(Seq(
      StructField("ID_EMPRESA", StringType, true),
      StructField("NOMBRE_EMPRESA", StringType, true)
  )), true),
  StructField("TRANSACCION", StructType(Seq(
      StructField("MONTO", DoubleType, true),
      StructField("FECHA", StringType, true)
  )), true)
))



//Agregamos la columna "json_data"
//Con la función "from_json" convertimos la columna "value" a un json usando el "schema" definido
var df2 = df1.
withColumn("json_data", f.from_json(df1.col("value"), schema)).alias("P1").
select(
  f.col("P1.json_data.PERSONA.ID_PERSONA").alias("ID_PERSONA"),
  f.col("P1.json_data.EMPRESA.ID_EMPRESA").alias("ID_EMPRESA"),
  f.col("P1.json_data.TRANSACCION.MONTO").alias("MONTO")
)



//Vemos los datos
display(df2)



// DBTITLE 1,5. Patrón de conexión al stream
var dfStream = spark.readStream.format("kafka").
option("kafka.bootstrap.servers", clusterKafka). //Dirección y puerto del tópico
option("subscribe", "transaccion"). //Nombre del tópico
option("startingOffsets", "latest"). //Desde donde extraemos los datos del tópico
load().alias("P1"). //PASO 1: NOS CONECTAMOS AL STREAM DE DATOS
select(
  f.col("P1.value").cast("string")
).alias("P2"). //PASO 2: SELECCIONAMOS Y CASTEAMOS A STRING LA COLUMNA "value" QUE CONTIENE LOS DATOS
withColumn("json_data", 
  f.from_json(
    f.col("P2.value"), 
    StructType(Seq(
      StructField("PERSONA", StructType(Seq(
          StructField("ID_PERSONA", StringType, true),
          StructField("NOMBRE_PERSONA", StringType, true),
          StructField("EDAD", IntegerType, true),
          StructField("SALARIO", DoubleType, true),
          StructField("CONTACTO", ArrayType(StructType(Seq(
              StructField("PREFIJO", StringType, true),
              StructField("TELEFONO", StringType, true)
          ))), true)
      )), true),
      StructField("EMPRESA", StructType(Seq(
          StructField("ID_EMPRESA", StringType, true),
          StructField("NOMBRE_EMPRESA", StringType, true)
      )), true),
      StructField("TRANSACCION", StructType(Seq(
          StructField("MONTO", DoubleType, true),
          StructField("FECHA", StringType, true)
      )), true)
      ))
  )
).alias("P3"). //PASO 3: CONVERTIMOS EL STRING A JSON, INDICANDO EL ESQUEMA
select(
  f.col("P3.json_data.PERSONA.ID_PERSONA").alias("ID_PERSONA"),
  f.col("P3.json_data.EMPRESA.ID_EMPRESA").alias("ID_EMPRESA"),
  f.col("P3.json_data.TRANSACCION.MONTO").alias("MONTO")
) //PASO 4: ESTRUCTURAMOS EL JSON A UNA TABLA



//Vemos los datos
display(dfStream)
