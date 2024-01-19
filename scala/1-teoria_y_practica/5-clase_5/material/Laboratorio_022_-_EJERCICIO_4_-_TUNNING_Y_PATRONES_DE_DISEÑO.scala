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



// DBTITLE 1,2. Reserva de potencia del clúster
//RESERVAR EL 10% DE LA POTENCIA DEL CLÚSTER ANTERIOR
//
// 1. RAM RESERVADA = 10% * 1000 GB = 100 GB
// 2. vCPU RESERVADA = 10% * 400 = 40 vCPU
//
// 3. vCPU POR EXECUTOR [ESTÁNDAR: 2 para procesos BATCH y 4 para procesos REAL TIME] = 2
// 4. NÚMERO DE EXECUTORS = vCPU RESERVADA / vCPU POR EXECUTOR = 40 vCPU / 2 = 20
// 5. RAM POR EXECUTOR = RAM RESERVADA / NÚMERO DE EXECUTORS = 100 GB / 20 = 5 GB
// 6. MEMORY OVERHEAD [ESTÁNDAR: 10% DE RAM POR EXECUTOR] = 10% * 5 GB = 10% * 5000 MB = 500 MB
// 7. NIVEL DE PARALELISMO = NÚMERO DE EXECUTORS * vCPU POR EXECUTOR * 2.5 = 20 * 2 * 2.5 = 100
// 8. MEMORIA RAM PARA EL LENGUAJE DE PROGRAMACIÓN [ESTÁNDAR: 1 GB] = 1 GB
// 9. COMPRESIÓN PARA TRANSFERENCIA DE RED EN JOINS = true
// 10. TIEMPO DE ESPERA DE BROADCAST = 5 MINUTOS = 5 * 60 SEGUNDOS = 300 SEGUNDOS
// 11. TAMAÑO MÁXIMO DE BROADCAST PARA DATAFRAME = 10% * RAM POR EXECUTOR (EN MB) * 1000000 (ya que se expresa en bytes) = 10% * 500 MB * 1000000 = 50000000 BYTES

// COMMAND ----------

//Creamos la sesión de SPARK reservado la potencia del clúster
var spark = SparkSession.builder.
appName("Mi Aplicacion").
config("spark.driver.memory", "1g").
config("spark.dynamicAllocation.maxExecutors", "20").
config("spark.executor.cores", "2").
config("spark.executor.memory", "5g").
config("spark.executor.memoryOverhead", "500m").
config("spark.default.parallelism", "100").
config("spark.sql.inMemoryColumnarStorage.compressed", "true").
config("spark.sql.broadcastTimeout", "300").
config("spark.sql.autoBroadcastJoinThreshold", "50000000").
enableHiveSupport().
getOrCreate()



// DBTITLE 1,3. Funciones utilitarias
//PATRÓN DE DISEÑO SHOW

//Definimos una varible de control
var PARAM_SHOW_HABILITADO = true

//Definimos la función
def show(df : DataFrame) = {
  if(PARAM_SHOW_HABILITADO == true){
    df.show()
  }
}



//PATRÓN DE DISEÑO CHECKPOINT
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



//PATRÓN DE DISEÑO CACHÉ

//Función para almacenar en la caché
def cache(df : DataFrame) = {
  print("Almacenando en cache...")
  df.cache()
  println(", almacenado en cache!")
}

//Función para liberar del caché un dataframe
def liberarCache(df : DataFrame) = {
  print("Liberando cache...")
  df.unpersist(blocking = true)
  println(", cache liberado!")
}

//Función para liberar todos los dataframes almacenados en la caché
def liberarTodoElCache(spark : SparkSession) = {
  print("Liberando todo el cache...")
  spark.sqlContext.clearCache()
  println(", todo el cache liberado!")
}



//PATRÓN DE DISEÑO REPARTITION

//Definimos el número de registros por partición
var REGISTROS_POR_PARTICION = 100000

//Función de reparticionamiento
def reparticionar(df : DataFrame) : DataFrame = {
  var dfReparticionado : DataFrame = null
  
  //Obtenemos el número de particiones actuales
  var numeroDeParticionesActuales = df.rdd.getNumPartitions
  
  //Obtenemos la cantidad de registros del dataframe
  var cantidadDeRegistros = df.count()
  
  //Obtenemos el nuevo número de particiones
  var nuevoNumeroDeParticiones = (cantidadDeRegistros / (REGISTROS_POR_PARTICION *1.0)).ceil.toInt
  
  //Reparticionamos
  print("Reparticionando a "+nuevoNumeroDeParticiones+ " particiones...")
  if(nuevoNumeroDeParticiones > numeroDeParticionesActuales){
    dfReparticionado = df.repartition(nuevoNumeroDeParticiones)
  }else{
    dfReparticionado = df.coalesce(nuevoNumeroDeParticiones)
  }
  println(", reparticionado!")
  
  return dfReparticionado
}



// DBTITLE 1,4. UDFs
//Implementamos la función
def calcularRiesgoPonderado(riesgoCentral1 : Double, riesgoCentral2 : Double, riesgoCentral3 : Double) : Double = {
  var resultado : Double = 0.0

  resultado = (1*riesgoCentral1 + 2*riesgoCentral2 + 1*riesgoCentral3) / 4

  return resultado
}

//Creamos la función personalizada
var udfCalcularRiesgoPonderado = udf(
  (
    riesgoCentral1 : Double, 
    riesgoCentral2 : Double,
    riesgoCentral3 : Double
  ) => calcularRiesgoPonderado(
    riesgoCentral1, 
    riesgoCentral2,
    riesgoCentral3
  )
)

//Registramos el UDF
spark.udf.register("udfCalcularRiesgoPonderado", udfCalcularRiesgoPonderado)



// DBTITLE 1,5. Lectura de datos
//Leemos los datos de transacciones
var dfJson = spark.read.format("json").load("dbfs:///FileStore/_spark/transacciones_bancarias.json")

//Reparticionamos
dfJson = reparticionar(dfJson)

//Almacenar en la caché
cache(dfJson)

//Vemos el esquema
dfJson.printSchema()

//Mostramos los datos
show(dfJson)



//Leemos el archivo de riesgo crediticio
var dfRiesgo = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(
    StructType(
        Array(
          StructField("ID_CLIENTE", StringType, true),
          StructField("RIESGO_CENTRAL_1", DoubleType, true),
          StructField("RIESGO_CENTRAL_2", DoubleType, true),
          StructField("RIESGO_CENTRAL_3", DoubleType, true)
        )
    )
).load("dbfs:///FileStore/_spark/RIESGO_CREDITICIO.csv")

//Reparticionamos
dfRiesgo = reparticionar(dfRiesgo)

//Almacenar en la caché
cache(dfRiesgo)

//Vemos el esquema
dfRiesgo.printSchema()

//Mostramos los datos
show(dfRiesgo)



// DBTITLE 1,6. Modelamiento
//Estructuramos dfEmpresa

//Seleccionamos los campos
var dfEmpresa = dfJson.select(
  dfJson.col("EMPRESA.ID_EMPRESA").alias("ID_EMPRESA"),
  dfJson.col("EMPRESA.NOMBRE_EMPRESA").alias("NOMBRE_EMPRESA")
).distinct()

//Vemos el esquema
dfEmpresa.printSchema()

//Mostramos los datos
show(dfEmpresa)



//Estructuramos dfPersona

//Seleccionamos los campos
var dfPersona = dfJson.select(
  dfJson.col("PERSONA.ID_PERSONA").alias("ID_PERSONA"),
  dfJson.col("PERSONA.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
  dfJson.col("PERSONA.EDAD").alias("EDAD"),
  dfJson.col("PERSONA.SALARIO").alias("SALARIO")
).distinct()

//Vemos el esquema
dfPersona.printSchema()

//Mostramos los datos
show(dfPersona)



//Estructuramos dfTransaccion

//Seleccionamos los campos
var dfTransaccion = dfJson.select(
  dfJson.col("PERSONA.ID_PERSONA").alias("ID_PERSONA"),
  dfJson.col("EMPRESA.ID_EMPRESA").alias("ID_EMPRESA"),
  dfJson.col("TRANSACCION.MONTO").alias("MONTO"),
  dfJson.col("TRANSACCION.FECHA").alias("FECHA")
)

//Vemos el esquema
dfTransaccion.printSchema()

//Mostramos los datos
show(dfTransaccion)



// DBTITLE 1,7. Reglas de calidad
//Aplicamos las reglas de calidad a dfTransaccion
var dfTransaccionLimpio = dfTransaccion.filter(
  (dfTransaccion.col("ID_EMPRESA").isNotNull) &&
  (dfTransaccion.col("ID_EMPRESA").isNotNull) &&
  (dfTransaccion.col("MONTO").isNotNull) &&
  (dfTransaccion.col("FECHA").isNotNull) &&
  (dfTransaccion.col("MONTO") > 0)
)

//Vemos el esquema
dfTransaccionLimpio.printSchema()

//Aplicamos checkpoint para liberar memoria de toda la cadena de procesos
dfTransaccionLimpio = checkpoint(dfTransaccionLimpio)

//Reparticionamos
dfTransaccionLimpio = reparticionar(dfTransaccionLimpio)

//Almacenamos en la caché
cache(dfTransaccionLimpio)

//Mostramos los datos
show(dfTransaccionLimpio)



//Aplicamos las reglas de calidad a dfPersona
var dfPersonaLimpio = dfPersona.filter(
  (dfPersona.col("ID_PERSONA").isNotNull) &&
  (dfPersona.col("NOMBRE_PERSONA").isNotNull) &&
  (dfPersona.col("SALARIO").isNotNull) &&
  (dfPersona.col("SALARIO") > 0)
)

//Vemos el esquema
dfPersonaLimpio.printSchema()

//Aplicamos checkpoint para liberar memoria de toda la cadena de procesos
dfPersonaLimpio = checkpoint(dfPersonaLimpio)

//Reparticionamos
dfPersonaLimpio = reparticionar(dfPersonaLimpio)

//Almacenamos en la caché
cache(dfPersonaLimpio)

//Mostramos los datos
show(dfPersonaLimpio)



//Aplicamos las reglas de calidad a dfEmpresa
var dfEmpresaLimpio = dfEmpresa.filter(
  (dfEmpresa.col("ID_EMPRESA").isNotNull) && 
  (dfEmpresa.col("NOMBRE_EMPRESA").isNotNull)
)

//Vemos el esquema
dfEmpresaLimpio.printSchema()

//Aplicamos checkpoint para liberar memoria de toda la cadena de procesos
dfEmpresaLimpio = checkpoint(dfEmpresaLimpio)

//Reparticionamos
dfEmpresaLimpio = reparticionar(dfEmpresaLimpio)

//Almacenamos en la caché
cache(dfEmpresaLimpio)

//Mostramos los datos
show(dfEmpresaLimpio)



//Aplicamos las reglas de calidad a dfRiesgo
var dfRiesgoLimpio = dfRiesgo.filter(
  (dfRiesgo.col("ID_CLIENTE").isNotNull) &&
  (dfRiesgo.col("RIESGO_CENTRAL_1").isNotNull) &&
  (dfRiesgo.col("RIESGO_CENTRAL_2").isNotNull) &&
  (dfRiesgo.col("RIESGO_CENTRAL_3").isNotNull) &&
  (dfRiesgo.col("RIESGO_CENTRAL_1") >= 0) &&
  (dfRiesgo.col("RIESGO_CENTRAL_2") >= 0) &&
  (dfRiesgo.col("RIESGO_CENTRAL_3") >= 0)
  
)

//Vemos el esquema
dfRiesgoLimpio.printSchema()

//Aplicamos checkpoint para liberar memoria de toda la cadena de procesos
dfRiesgoLimpio = checkpoint(dfRiesgoLimpio)

//Reparticionamos
dfRiesgoLimpio = reparticionar(dfRiesgoLimpio)

//Almacenamos en la caché
cache(dfRiesgoLimpio)

//Mostramos los datos
show(dfRiesgoLimpio)



// DBTITLE 1,8. Preparación de tablones
//PASO 1 (<<T_P>>): AGREGAR LOS DATOS DE LAS PERSONAS QUE REALIZARON LAS TRANSACCIONES
var df1 = dfTransaccionLimpio.join(
  dfPersonaLimpio,
  dfTransaccionLimpio.col("ID_PERSONA") === dfPersonaLimpio.col("ID_PERSONA"),
  "inner"
).select(
  dfPersonaLimpio.col("ID_PERSONA"),
  dfPersonaLimpio.col("NOMBRE_PERSONA"),
  dfPersonaLimpio.col("EDAD").alias("EDAD_PERSONA"),
  dfPersonaLimpio.col("SALARIO").alias("SALARIO_PERSONA"),
  dfTransaccionLimpio.col("ID_EMPRESA"),
  dfTransaccionLimpio.col("MONTO").alias("MONTO_TRANSACCION"),
  dfTransaccionLimpio.col("FECHA").alias("FECHA_TRANSACCION")
)

//Mostramos los datos
show(df1)



//PASO 2 (<<T_P_E>>): AGREGAR LOS DATOS DE LAS EMPRESAS EN DONDE SE REALIZARON LAS TRANSACCIONES
var df2 = df1.join(
  dfEmpresaLimpio,
  df1.col("ID_EMPRESA") === dfEmpresaLimpio.col("ID_EMPRESA"),
  "inner"
).select(
  df1.col("ID_PERSONA"),
  df1.col("NOMBRE_PERSONA"),
  df1.col("EDAD_PERSONA"),
  df1.col("SALARIO_PERSONA"),
  df1.col("ID_EMPRESA"),
  dfEmpresaLimpio.col("NOMBRE_EMPRESA"),
  df1.col("MONTO_TRANSACCION"),
  df1.col("FECHA_TRANSACCION")
)

//Mostramos los datos
show(df2)



//Aplicamos la función
var df3 = dfRiesgoLimpio.select(
	dfRiesgoLimpio.col("ID_CLIENTE").alias("ID_CLIENTE"),
	udfCalcularRiesgoPonderado(
      dfRiesgoLimpio.col("RIESGO_CENTRAL_1"),
      dfRiesgoLimpio.col("RIESGO_CENTRAL_2"),
      dfRiesgoLimpio.col("RIESGO_CENTRAL_3"),
	).alias("RIESGO_PONDERADO")
)

//Mostramos los datos
show(df3)



//PASO 4 (<<T_P_E_R>>): Agregamos el riesgo crediticio ponderado a cada persona que realizo la transacción
var dfTablon = df2.join(
  df3,
  df2.col("ID_PERSONA") === df3.col("ID_CLIENTE"),
  "inner"
).select(
  df2.col("ID_PERSONA"),
  df2.col("NOMBRE_PERSONA"),
  df2.col("EDAD_PERSONA"),
  df2.col("SALARIO_PERSONA"),
  df2.col("ID_EMPRESA"),
  df2.col("NOMBRE_EMPRESA"),
  df2.col("MONTO_TRANSACCION"),
  df2.col("FECHA_TRANSACCION"),
  df3.col("RIESGO_PONDERADO")
)

//Mostramos los datos
show(dfTablon)



// DBTITLE 1,6. Procesamiento
//REPORTE 1:
//
// - TRANSACCIONES MAYORES A 500 DÓLARES
// - CON RIESGO CREDITICIO PONDERADO MAYOR A 0.5
// - REALIZADAS EN AMAZON
// - POR PERSONAS ENTRE 30 A 39 AÑOS
// - CON UN SALARIO DE 1000 A 5000 DOLARES
//
//REPORTE 2:
//
// - TRANSACCIONES MAYORES A 500 DÓLARES
// - CON RIESGO CREDITICIO PONDERADO MAYOR A 0.5
// - REALIZADAS EN AMAZON
// - POR PERSONAS ENTRE 40 A 49 AÑOS
// - CON UN SALARIO DE 2500 A 7000 DOLARES
//
//REPORTE 3:
//
// - TRANSACCIONES MAYORES A 500 DÓLARES
// - CON RIESGO CREDITICIO PONDERADO MAYOR A 0.5
// - REALIZADAS EN AMAZON
// - POR PERSONAS ENTRE 50 A 60 AÑOS
// - CON UN SALARIO DE 3500 A 10000 DOLARES
//
//Notamos que todos los reportes comparten:
// - TRANSACCIONES MAYORES A 500 DÓLARES
// - CON RIESGO CREDITICIO PONDERADO MAYOR A 0.5
// - REALIZADAS EN AMAZON



//Calculamos las reglas comunes en un dataframe
var dfTablon1 = dfTablon.filter(
  (dfTablon.col("MONTO_TRANSACCION") > 500) &&
  (dfTablon.col("RIESGO_PONDERADO") > 0.5) &&
  (dfTablon.col("NOMBRE_EMPRESA") === "Amazon")
)

//Aplicamos checkpoint para liberar memoria de toda la cadena de procesos
dfTablon1 = checkpoint(dfTablon1)

//Reparticionamos
dfTablon1 = reparticionar(dfTablon1)

//Almacenamos en la caché
cache(dfTablon1)

//Mostramos los datos
show(dfTablon1)



//REPORTE 1:
// - POR PERSONAS ENTRE 30 A 39 AÑOS
// - CON UN SALARIO DE 1000 A 5000 DOLARES
var dfReporte1 = dfTablon1.filter(
  (dfTablon1.col("EDAD_PERSONA") >= 30) &&
  (dfTablon1.col("EDAD_PERSONA") <= 39) &&
  (dfTablon1.col("SALARIO_PERSONA") >= 1000) &&
  (dfTablon1.col("SALARIO_PERSONA") <= 5000)
)

//Almacenamos en la caché
cache(dfReporte1)

//Mostramos los datos
dfReporte1.show()



//REPORTE 2:
// - POR PERSONAS ENTRE 40 A 49 AÑOS
// - CON UN SALARIO DE 2500 A 7000 DOLARES
var dfReporte2 = dfTablon1.filter(
  (dfTablon1.col("EDAD_PERSONA") >= 40) &&
  (dfTablon1.col("EDAD_PERSONA") <= 49) &&
  (dfTablon1.col("SALARIO_PERSONA") >= 2500) &&
  (dfTablon1.col("SALARIO_PERSONA") <= 7000)
)

//Almacenamos en la caché
cache(dfReporte2)

//Mostramos los datos
dfReporte2.show()



//REPORTE 3:
// - POR PERSONAS ENTRE 50 A 60 AÑOS
// - CON UN SALARIO DE 3500 A 10000 DOLARES
var dfReporte3 = dfTablon1.filter(
  (dfTablon1.col("EDAD_PERSONA") >= 50) &&
  (dfTablon1.col("EDAD_PERSONA") <= 60) &&
  (dfTablon1.col("SALARIO_PERSONA") >= 3500) &&
  (dfTablon1.col("SALARIO_PERSONA") <= 10000)
)

//Almacenamos en la caché
cache(dfReporte3)

//Mostramos los datos
dfReporte3.show()



// DBTITLE 1,7. Almacenamiento
//Almacenamos el REPORTE 1
dfReporte1.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", ",").save("dbfs:///FileStore/_spark/output/REPORTE_1")



//Almacenamos el REPORTE 2
dfReporte2.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", ",").save("dbfs:///FileStore/_spark/output/REPORTE_2")



//Almacenamos el REPORTE 3
dfReporte3.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", ",").save("dbfs:///FileStore/_spark/output/REPORTE_3")



// DBTITLE 1,8. Verificacion
//Verificamos los archivos
var dfReporteLeido1 = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("dbfs:///FileStore/_spark/output/REPORTE_1")
dfReporteLeido1.show()



//Verificamos los archivos
var dfReporteLeido2 = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("dbfs:///FileStore/_spark/output/REPORTE_2")
dfReporteLeido2.show()



//Verificamos los archivos
var dfReporteLeido3 = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("dbfs:///FileStore/_spark/output/REPORTE_3")
dfReporteLeido3.show()
