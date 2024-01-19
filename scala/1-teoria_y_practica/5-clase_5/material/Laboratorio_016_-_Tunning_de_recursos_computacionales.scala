// Databricks notebook source

// COMMAND ----------

// DBTITLE 1,2. Cálculo de reserva de recursos del clúster
//Para reservar recursos computacionales del clúster
//
// - spark.driver.memory: Cantidad de memoria RAM asignada al driver, generalmente se le coloca 1gb
// - spark.dynamicAllocation.maxExecutors: Máximo número de executors que nuestra aplicación puede solicitar a YARN
// - spark.executor.cores: Número de CPUs por executor, generalmente se le coloca 2.
// - spark.executor.memory: Cantidad de memoria RAM separada para cada executor, se le coloca como mínimo la suma total del peso de todos los archivos con los que se trabaja
// - spark.executor.memoryOverhead: Cantidad de memoria RAM adicional por si se llenase el "spark.executor.memory", generalmente se coloca el 10% de "spark.executor.memory" o 1gb
// - spark.default.parallelism: Nivel de paralelismo en el procesamiento (cuantos hilos van a ver en este proceso)

// COMMAND ----------

//CLUSTER DE EJEMPLO:
//
// - RAM: 1000 GB
// - vCPU: 400

// COMMAND ----------

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

// COMMAND ----------

//DEFINIMOS LOS PARÁMETROS (las unidades se colocan en un sólo caracter y en minúsculas [GB = g, MB = m]
//
// MEMORIA RAM PARA EL LENGUAJE DE PROGRAMACIÓN = spark.driver.memory = 1g
// NÚMERO DE EXECUTORS = spark.dynamicAllocation.maxExecutors = 20
// vCPU POR EXECUTOR = spark.executor.cores = 2
// RAM POR EXECUTOR = spark.executor.memory = 5g
// MEMORY OVERHEAD = spark.executor.memoryOverhead = 500m
// NIVEL DE PARALELISMO = spark.default.parallelism = 100

// COMMAND ----------

// DBTITLE 1,2. Reserva de recursos del clúster
//Importamos la librería de sesión (nos permite crear la sesión de Spark)
import org.apache.spark.sql.SparkSession

// COMMAND ----------

//CREAMOS NUESTRA SESIÓN RESERVANDO EL 10% DE LA POTENCIA DEL CLÚSTER
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

// DBTITLE 1,3. El problema de la reserva
//¿Y si creamos la sesión poniendo números grandes?
var spark = SparkSession.builder.
appName("Mi Aplicacion").
config("spark.driver.memory", "9999999999999g").
config("spark.dynamicAllocation.maxExecutors", "9999999999999").
config("spark.executor.cores", "9999999999999").
config("spark.executor.memory", "9999999999999g").
config("spark.executor.memoryOverhead", "9999999999999g").
config("spark.default.parallelism", "9999999999999").
enableHiveSupport().
getOrCreate()

// COMMAND ----------

//SPARK lo permite, monopolizamos el clúster y dejamos sin recursos al resto de los desarrolladores
