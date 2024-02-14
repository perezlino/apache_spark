// Databricks notebook source


// DBTITLE 1,1. Librerías
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

// COMMAND ----------

// DBTITLE 1,2. Funciones utilitarias
//Variable que controla el show
//var PARAM_SHOW = false
var PARAM_SHOW = true

//Función para mostrar el stream
def show(df : DataFrame) = {
  if(PARAM_SHOW == true){
    display(df)
  }
}

// COMMAND ----------

// DBTITLE 1,3. Lectura de datos
//Leemos el archivo de persona
var dfCliente = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        Array(
            StructField("ID", StringType, true),
            StructField("NOMBRE", StringType, true),
            StructField("EDAD", IntegerType, true),
            StructField("SALARIO", DoubleType, true)
        )
    )
).load("dbfs:///FileStore/_spark/DATA_CLIENTE.TXT")

//Mostramos los datos
dfCliente.show()

// COMMAND ----------

//Leemos el archivo de persona
var dfProducto = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        Array(
            StructField("ID", StringType, true),
            StructField("NOMBRE", StringType, true)
        )
    )
).load("dbfs:///FileStore/_spark/DATA_PRODUCTO.TXT")

//Mostramos los datos
dfProducto.show()

// COMMAND ----------

// DBTITLE 1,4. Conexión al stream
//Conexion al stream de datos
var dfStream = spark.readStream.format("kafka").
option("kafka.bootstrap.servers", "localhost:9092"). //Dirección y puerto del tópico
option("subscribe", "compras"). //Nombre del tópico
option("startingOffsets", "latest"). //Desde donde extraemos los datos del tópico
load().alias("P1"). //PASO 1: NOS CONECTAMOS AL STREAM DE DATOS
select(
  f.col("P1.value").cast("string")
).alias("P2"). //PASO 2: SELECCIONAMOS Y CASTEAMOS A STRING LA COLUMNA "value" QUE CONTIENE LOS DATOS
withColumn("json_data", 
  f.from_json(
    f.col("P2.value"), 
    StructType(Seq(
      StructField("CABECERA", StructType(Seq(
        StructField("ESTADO", StringType, true),
        StructField("MENSAJE", StringType, true)
      )), true),
      StructField("DATA", StructType(Seq(
        StructField("ID_CLIENTE", StringType, true),
        StructField("ID_PRODUCTO", StringType, true),
        StructField("MONTO", DoubleType, true)
      )), true)
    ))
  )
).alias("P3"). //PASO 3: CONVERTIMOS EL STRING A JSON, INDICANDO EL ESQUEMA
select(
  f.col("P3.json_data.DATA.ID_CLIENTE").alias("ID_CLIENTE"),
  f.col("P3.json_data.DATA.ID_PRODUCTO").alias("ID_PRODUCTO"),
  f.col("P3.json_data.DATA.MONTO").alias("MONTO")
) //PASO 4: ESTRUCTURAMOS EL JSON A UNA TABLA

// COMMAND ----------

//Mostramos los datos
show(dfStream)

// COMMAND ----------

// DBTITLE 1,5. Procesamiento
//Obtenemos los datos de la persona que hizo la transacción
var df1 = dfStream.join(
  dfCliente,
  dfStream.col("ID_CLIENTE") === dfCliente.col("ID"),
  "inner"
).select(
  dfStream.col("ID_CLIENTE").alias("ID_CLIENTE"),
  dfCliente.col("NOMBRE").alias("NOMBRE_CLIENTE"), 
  dfCliente.col("EDAD").alias("EDAD_CLIENTE"), 
  dfCliente.col("SALARIO").alias("SALARIO_CLIENTE"), 
  dfStream.col("ID_PRODUCTO").alias("ID_PRODUCTO"), 
  dfStream.col("MONTO").alias("MONTO")
)

//Mostramos los datos
show(df1)

// COMMAND ----------

//Obtenemos los datos del producto que se compró
var dfResultado = df1.join(
  dfProducto,
  df1.col("ID_PRODUCTO") === dfProducto.col("ID"),
  "inner"
).select(
  df1.col("ID_CLIENTE").alias("ID_CLIENTE"),
  df1.col("NOMBRE_CLIENTE").alias("NOMBRE_CLIENTE"), 
  df1.col("EDAD_CLIENTE").alias("EDAD_CLIENTE"), 
  df1.col("SALARIO_CLIENTE").alias("SALARIO_CLIENTE"), 
  df1.col("ID_PRODUCTO").alias("ID_PRODUCTO"), 
  dfProducto.col("NOMBRE").alias("NOMBRE_PRODUCTO"), 
  df1.col("MONTO").alias("MONTO")
)

//Mostramos los datos
show(dfResultado)

// COMMAND ----------

// DBTITLE 1,6. Escritura de resultantes
// MAGIC %fs rm -r dbfs:///FileStore/_spark/output/COMPRA_ENRIQUECIDA

// COMMAND ----------

//Ejecutamos la cadena de procesos
dfResultado.writeStream.format("parquet").outputMode("append").
option("checkpointLocation","dbfs:///FileStore/_spark/output/COMPRA_ENRIQUECIDA/_checkpoints/COMPRA_ENRIQUECIDA").
trigger(ProcessingTime("5 seconds")).
start("dbfs:///FileStore/_spark/output/COMPRA_ENRIQUECIDA").
awaitTermination()
