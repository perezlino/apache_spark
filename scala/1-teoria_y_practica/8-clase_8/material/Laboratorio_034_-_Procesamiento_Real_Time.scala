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



// DBTITLE 1,3. Lectura de datos
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



//Leemos el archivo de persona
var dfEmpresa = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        Array(
            StructField("ID", StringType, true),
            StructField("NOMBRE", StringType, true)
        )
    )
).load("dbfs:///FileStore/_spark/empresa.data")

//Mostramos los datos
dfEmpresa.show()



// DBTITLE 1,4. Conexión al stream
//Conexion al stream de datos
var dfStream = spark.readStream.format("kafka").
option("kafka.bootstrap.servers", "localhost:9092"). //Dirección y puerto del tópico
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



//Mostramos los datos
show(dfStream)



// DBTITLE 1,5. Procesamiento
//Nos quedaremos con las transacciones mayores a 1500
var df1 = dfStream.filter(dfStream.col("MONTO") > 1500)

//Mostramos los datos
show(df1)



//Obtenemos los datos de la persona que hizo la transacción
var df2 = df1.join(
  dfPersona,
  df1.col("ID_PERSONA") === dfPersona.col("ID"),
  "inner"
).select(
  dfPersona.col("ID").alias("ID_PERSONA"),
  dfPersona.col("NOMBRE").alias("NOMBRE_PERSONA"), 
  dfPersona.col("EDAD").alias("EDAD_PERSONA"), 
  dfPersona.col("SALARIO").alias("SALARIO_PERSONA"), 
  df1.col("ID_EMPRESA").alias("ID_EMPRESA"), 
  df1.col("MONTO").alias("MONTO_TRANSACCION")
)

//Mostramos los datos
show(df2)



//Obtenemos los datos de la empresa en donde se realizó la transacción
var dfResultado = df2.join(
  dfEmpresa,
  df2.col("ID_EMPRESA") === dfEmpresa.col("ID"),
  "inner"
).select(
  df2.col("ID_PERSONA").alias("ID_PERSONA"),
  df2.col("NOMBRE_PERSONA").alias("NOMBRE_PERSONA"), 
  df2.col("EDAD_PERSONA").alias("EDAD_PERSONA"), 
  df2.col("SALARIO_PERSONA").alias("SALARIO_PERSONA"), 
  dfEmpresa.col("ID").alias("ID_EMPRESA"), 
  dfEmpresa.col("NOMBRE").alias("NOMBRE_EMPRESA"), 
  df2.col("MONTO_TRANSACCION").alias("MONTO_TRANSACCION")
)

//Mostramos los datos
show(dfResultado)



// DBTITLE 1,6. Escritura de resultantes
// MAGIC %fs rm -r dbfs:///FileStore/output/dfRealTime



//Ejecutamos la cadena de procesos
dfResultado.
writeStream
.format("delta")
.outputMode("append")
.option("checkpointLocation", "dbfs:///FileStore/_spark/output/dfRealTime/_checkpoints/dfRealTime")
.trigger(ProcessingTime("5 seconds"))
.start("dbfs:///FileStore/_spark/output/dfRealTime")
.awaitTermination()
