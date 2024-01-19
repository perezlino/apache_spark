// Databricks notebook source


// COMMAND ----------

// DBTITLE 1,1. Tunning de compresión en transferencia de RED
//1. OPTIMIZACIÓN DE COMPRESIÓN PARA TRANSFERENCIA DE RED
//
// Hacer un JOIN significa comparar dos dataframes que pueden estar distribuidos entre diferentes executors de diferentes servidores
// Para hacer la comparación, es necesario enviar un parte del dataframe al servidor en donde está el otro dataframe
// Para que el envío se haga lo más rápido posible, hay que comprimir la porción del dataframe que se envía

// COMMAND ----------

// DBTITLE 1,2. Tunning de broadcast
//3. RESERVAR DE MEMORIA PARA DATAFRAMES BROADCAST
//
//Deberemos ver si hay alguna tabla "pequeña" que participe en el JOIN
//Estas tablas pequeñas las copiaremos en la memoria RAM en todos los executors para evitar los tiempos de transferencia en la red y las operaciones SHUFFLE
//Para evitar el colapso de memoria deberemos colocar el 10% de la memoria RAM reservada (RAM POR EXECUTOR = 5g => MEMORIA PARA BROADCAST = 500m)
//Esto significa que copiaremos las variables broadcasts en todos los executors, siempre y cuando no superen los 400m de peso
//La variable está en bytes, para convertirlo de megabytes a bytes, agregamos seis ceros al final: 500000000

// COMMAND ----------

// DBTITLE 1,3. Tunning de tiempo de espera de broadcast
//2. TIEMPO DE ESPERA PARA EL BROADCAST
//
// En ocasiones, puede haber latencia en la red al momento de mover los datos de los dataframes broadcasts para hacer el JOIN
// Si la red está libre, se mueven rápido, si la red está saturada, se mueven lento
// Hay que configurar el tiempo de espera del broadcast para que se muevan por la red, esperaremos por 5 minutos
// Si luego de los 10 minutos no se pudieron mover los datos por la red, el JOIN no convertirá en broadcast a la variable
// El tiempo se coloca en segundos: 5 min = 5 min * 60 seg = 300

// COMMAND ----------

// DBTITLE 1,4. Definición de la sesión de SPARK
//CLUSTER DE EJEMPLO:
//
// - RAM: 1000 GB
// - vCPU: 400

// COMMAND ----------

//ESTO YA LO VIMOS:
//EJEMPLO: RESERVAR EL 10% DE LA POTENCIA DEL CLÚSTER ANTERIOR
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
//
// ESTO ES LO NUEVO QUE ESTAMOS AGREGANDO:
//
// 9. COMPRESIÓN PARA TRANSFERENCIA DE RED EN JOINS = true
// 10. TIEMPO DE ESPERA DE BROADCAST = 5 MINUTOS = 5 * 60 SEGUNDOS = 300 SEGUNDOS
// 11. TAMAÑO MÁXIMO DE BROADCAST PARA DATAFRAME = 10% * RAM POR EXECUTOR (EN MB) * 1000000 (ya que se expresa en bytes) = 10% * 500 MB * 1000000 = 50000000 BYTES

// COMMAND ----------

//DEFINIMOS LOS PARÁMETROS (las unidades se colocan en un sólo caracter y en minúsculas [GB = g, MB = m]
//
// MEMORIA RAM PARA EL LENGUAJE DE PROGRAMACIÓN = spark.driver.memory = 1g
// NÚMERO DE EXECUTORS = spark.dynamicAllocation.maxExecutors = 20
// vCPU POR EXECUTOR = spark.executor.cores = 2
// RAM POR EXECUTOR = spark.executor.memory = 5g
// MEMORY OVERHEAD = spark.executor.memoryOverhead = 500m
// NIVEL DE PARALELISMO = spark.default.parallelism = 100
// COMPRESIÓN PARA TRANSFERENCIA DE RED EN JOINS = spark.sql.inMemoryColumnarStorage.compressed = true
// TIEMPO DE ESPERA DE BROADCAST = spark.sql.broadcastTimeout = 300
// TAMAÑO MÁXIMO DE BROADCAST PARA DATAFRAME (no se coloca la unidad, sólo el número) = spark.sql.autoBroadcastJoinThreshold = 50000000

// COMMAND ----------

//Importamos la librería de sesión
import org.apache.spark.sql.SparkSession

// COMMAND ----------

//CREAMOS NUESTRA SESIÓN RESERVANDO EL 10% DE LA POTENCIA DEL CLÚSTER CON EL TUNNING PARA JOINS
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
