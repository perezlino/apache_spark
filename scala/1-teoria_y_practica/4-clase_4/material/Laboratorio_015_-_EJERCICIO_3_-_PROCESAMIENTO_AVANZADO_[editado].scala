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


// DBTITLE 1,2. Lectura de datos
//Leemos los datos de transacciones
var dfJson = spark.read.format("json").load("dbfs:///FileStore/_spark/transacciones_bancarias.json")

//Vemos el esquema
dfJson.printSchema()

root
 |-- EMPRESA: struct (nullable = true)
 |    |-- ID_EMPRESA: string (nullable = true)
 |    |-- NOMBRE_EMPRESA: string (nullable = true)
 |-- PERSONA: struct (nullable = true)
 |    |-- EDAD: long (nullable = true)
 |    |-- ID_PERSONA: string (nullable = true)
 |    |-- NOMBRE_PERSONA: string (nullable = true)
 |    |-- SALARIO: double (nullable = true)
 |-- TRANSACCION: struct (nullable = true)
 |    |-- FECHA: string (nullable = true)
 |    |-- MONTO: double (nullable = true)
 

//Mostramos los datos
dfJson.show()

+--------------+--------------------------+--------------------+
|EMPRESA       |PERSONA                   |TRANSACCION         |
+--------------+--------------------------+--------------------+
|{6, Google}   |{24, 24, Amaya, 1801.0}   |{2018-12-05, 1745.0}|
|{10, Sony}    |{32, 1, Carl, 20095.0}    |{2018-04-19, 238.0} |
|{2, Microsoft}|{34, 65, Nehru, 12423.0}  |{2018-04-19, 4097.0}|
|{5, Amazon}   |{23, 71, Doris, 11538.0}  |{2018-12-05, 1548.0}|
|{5, Amazon}   |{45, 83, Giselle, 2503.0} |{2018-04-19, 2233.0}|
|{4, Toyota}   |{42, 96, Amos, 15855.0}   |{2018-04-19, 2887.0}|
|{9, IBM}      |{70, 19, Laura, 17403.0}  |{2018-04-19, 286.0} |
|{4, Toyota}   |{67, 40, Ross, 14285.0}   |{2018-04-19, 974.0} |
|{8, HP}       |{57, 100, Cynthia, 8682.0}|{2018-04-19, 2698.0}|
|{2, Microsoft}|{22, 22, Kibo, 7449.0}    |{2018-12-05, 1398.0}|
|{7, Samsung}  |{23, 8, Jonah, 17040.0}   |{2018-12-05, 2538.0}|
|{4, Toyota}   |{42, 73, Fiona, 9960.0}   |{2018-11-28, 3878.0}|
|{5, Amazon}   |{34, 76, Omar, 12163.0}   |{2018-11-28, 375.0} |
|{5, Amazon}   |{70, 63, Sade, 11112.0}   |{2018-11-28, 707.0} |
|{7, Samsung}  |{59, 80, Ebony, 3600.0}   |{2018-11-28, 4250.0}|
|{5, Amazon}   |{59, 80, Ebony, 3600.0}   |{2018-11-28, 2232.0}|
|{7, Samsung}  |{67, 32, Gisela, 6497.0}  |{2018-11-28, 4493.0}|
|{1, Walmart}  |{33, 84, Keith, 13348.0}  |{2018-12-05, 769.0} |
|{3, Apple}    |{54, 35, Aurora, 4588.0}  |{2018-04-19, 3546.0}|
|{10, Sony}    |{27, 60, Bernard, 10825.0}|{2018-11-28, 3399.0}|
+--------------+--------------------------+--------------------+
only showing top 20 rows


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

//Vemos el esquema
dfRiesgo.printSchema()

root
 |-- ID_CLIENTE: string (nullable = true)
 |-- RIESGO_CENTRAL_1: double (nullable = true)
 |-- RIESGO_CENTRAL_2: double (nullable = true)
 |-- RIESGO_CENTRAL_3: double (nullable = true)


//Mostramos los datos
dfRiesgo.show()

+----------+----------------+----------------+----------------+
|ID_CLIENTE|RIESGO_CENTRAL_1|RIESGO_CENTRAL_2|RIESGO_CENTRAL_3|
+----------+----------------+----------------+----------------+
|         1|             0.1|             0.6|             0.4|
|         2|             0.2|             0.8|             0.3|
|         3|             0.9|             0.8|             0.8|
|         4|             0.5|             0.4|             0.6|
|         5|             0.6|             0.6|             0.6|
|         6|             0.3|             0.4|             0.6|
|         7|             0.2|             0.4|             0.3|
|         8|             0.7|             0.9|             0.7|
|         9|             0.3|             1.0|             1.0|
|        10|             0.9|             1.0|             0.4|
|        11|             0.6|             0.1|             0.3|
|        12|             0.7|             0.6|             0.7|
|        13|             0.7|             0.1|             0.5|
|        14|             0.1|             0.2|             0.0|
|        15|             0.9|             0.7|             0.6|
|        16|             1.0|             0.4|             0.7|
|        17|             0.0|             0.2|             0.5|
|        18|             0.7|             0.5|             1.0|
|        19|             0.6|             0.4|             0.6|
|        20|             0.1|             0.6|             0.8|
+----------+----------------+----------------+----------------+
only showing top 20 rows


// DBTITLE 1,3. Modelamiento
//Estructuramos dfEmpresa

//Seleccionamos los campos
var dfEmpresa = dfJson.select(
  dfJson.col("EMPRESA.ID_EMPRESA").alias("ID_EMPRESA"),
  dfJson.col("EMPRESA.NOMBRE_EMPRESA").alias("NOMBRE_EMPRESA")
).distinct()

//Vemos el esquema
dfEmpresa.printSchema()

root
 |-- ID_EMPRESA: string (nullable = true)
 |-- NOMBRE_EMPRESA: string (nullable = true)


//Contamos los registros
println(dfEmpresa.count())

10

//Mostramos los datos
dfEmpresa.show()

+----------+--------------+
|ID_EMPRESA|NOMBRE_EMPRESA|
+----------+--------------+
|         9|           IBM|
|         5|        Amazon|
|        10|          Sony|
|         2|     Microsoft|
|         7|       Samsung|
|         1|       Walmart|
|         6|        Google|
|         4|        Toyota|
|         3|         Apple|
|         8|            HP|
+----------+--------------+


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

root
 |-- ID_PERSONA: string (nullable = true)
 |-- NOMBRE_PERSONA: string (nullable = true)
 |-- EDAD: long (nullable = true)
 |-- SALARIO: double (nullable = true)


//Contamos los registros
println(dfPersona.count())

100

//Mostramos los datos
dfPersona.show()

+----------+--------------+----+-------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD|SALARIO|
+----------+--------------+----+-------+
|        73|         Fiona|  42| 9960.0|
|        39|       Carolyn|  64|22838.0|
|        24|         Amaya|  24| 1801.0|
|        22|          Kibo|  22| 7449.0|
|        96|          Amos|  42|15855.0|
|        60|       Bernard|  27|10825.0|
|        35|        Aurora|  54| 4588.0|
|        19|         Laura|  70|17403.0|
|        80|         Ebony|  59| 3600.0|
|        71|         Doris|  23|11538.0|
|        32|        Gisela|  67| 6497.0|
|        84|         Keith|  33|13348.0|
|        72|      Tallulah|  46| 9867.0|
|        65|         Nehru|  34|12423.0|
|        83|       Giselle|  45| 2503.0|
|        76|          Omar|  34|12163.0|
|        40|          Ross|  67|14285.0|
|         8|         Jonah|  23|17040.0|
|       100|       Cynthia|  57| 8682.0|
|         1|          Carl|  32|20095.0|
+----------+--------------+----+-------+
only showing top 20 rows


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

root
 |-- ID_PERSONA: string (nullable = true)
 |-- ID_EMPRESA: string (nullable = true)
 |-- MONTO: double (nullable = true)
 |-- FECHA: string (nullable = true)


//Contamos los registros
println(dfTransaccion.count())

23483

//Mostramos los datos
dfTransaccion.show()

+----------+----------+------+----------+
|ID_PERSONA|ID_EMPRESA| MONTO|     FECHA|
+----------+----------+------+----------+
|        24|         6|1745.0|2018-12-05|
|         1|        10| 238.0|2018-04-19|
|        65|         2|4097.0|2018-04-19|
|        71|         5|1548.0|2018-12-05|
|        83|         5|2233.0|2018-04-19|
|        96|         4|2887.0|2018-04-19|
|        19|         9| 286.0|2018-04-19|
|        40|         4| 974.0|2018-04-19|
|       100|         8|2698.0|2018-04-19|
|        22|         2|1398.0|2018-12-05|
|         8|         7|2538.0|2018-12-05|
|        73|         4|3878.0|2018-11-28|
|        76|         5| 375.0|2018-11-28|
|        63|         5| 707.0|2018-11-28|
|        80|         7|4250.0|2018-11-28|
|        80|         5|2232.0|2018-11-28|
|        32|         7|4493.0|2018-11-28|
|        84|         1| 769.0|2018-12-05|
|        35|         3|3546.0|2018-04-19|
|        60|        10|3399.0|2018-11-28|
+----------+----------+------+----------+
only showing top 20 rows


// DBTITLE 1,4. Reglas de calidad
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

root
 |-- ID_PERSONA: string (nullable = true)
 |-- ID_EMPRESA: string (nullable = true)
 |-- MONTO: double (nullable = true)
 |-- FECHA: string (nullable = true)


//Mostramos los datos
dfTransaccionLimpio.show()

+----------+----------+------+----------+
|ID_PERSONA|ID_EMPRESA| MONTO|     FECHA|
+----------+----------+------+----------+
|        24|         6|1745.0|2018-12-05|
|         1|        10| 238.0|2018-04-19|
|        65|         2|4097.0|2018-04-19|
|        71|         5|1548.0|2018-12-05|
|        83|         5|2233.0|2018-04-19|
|        96|         4|2887.0|2018-04-19|
|        19|         9| 286.0|2018-04-19|
|        40|         4| 974.0|2018-04-19|
|       100|         8|2698.0|2018-04-19|
|        22|         2|1398.0|2018-12-05|
|         8|         7|2538.0|2018-12-05|
|        73|         4|3878.0|2018-11-28|
|        76|         5| 375.0|2018-11-28|
|        63|         5| 707.0|2018-11-28|
|        80|         7|4250.0|2018-11-28|
|        80|         5|2232.0|2018-11-28|
|        32|         7|4493.0|2018-11-28|
|        84|         1| 769.0|2018-12-05|
|        35|         3|3546.0|2018-04-19|
|        60|        10|3399.0|2018-11-28|
+----------+----------+------+----------+
only showing top 20 rows


//Aplicamos las reglas de calidad a dfPersona
var dfPersonaLimpio = dfPersona.filter(
  (dfPersona.col("ID_PERSONA").isNotNull) &&
  (dfPersona.col("NOMBRE_PERSONA").isNotNull) &&
  (dfPersona.col("SALARIO").isNotNull) &&
  (dfPersona.col("SALARIO") > 0)
)

//Vemos el esquema
dfPersonaLimpio.printSchema()

root
 |-- ID_PERSONA: string (nullable = true)
 |-- NOMBRE_PERSONA: string (nullable = true)
 |-- EDAD: long (nullable = true)
 |-- SALARIO: double (nullable = true)


//Mostramos los datos
dfPersonaLimpio.show()

+----------+--------------+----+-------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD|SALARIO|
+----------+--------------+----+-------+
|        73|         Fiona|  42| 9960.0|
|        39|       Carolyn|  64|22838.0|
|        24|         Amaya|  24| 1801.0|
|        22|          Kibo|  22| 7449.0|
|        96|          Amos|  42|15855.0|
|        60|       Bernard|  27|10825.0|
|        35|        Aurora|  54| 4588.0|
|        19|         Laura|  70|17403.0|
|        80|         Ebony|  59| 3600.0|
|        71|         Doris|  23|11538.0|
|        32|        Gisela|  67| 6497.0|
|        84|         Keith|  33|13348.0|
|        72|      Tallulah|  46| 9867.0|
|        65|         Nehru|  34|12423.0|
|        83|       Giselle|  45| 2503.0|
|        76|          Omar|  34|12163.0|
|        40|          Ross|  67|14285.0|
|         8|         Jonah|  23|17040.0|
|       100|       Cynthia|  57| 8682.0|
|         1|          Carl|  32|20095.0|
+----------+--------------+----+-------+
only showing top 20 rows


//Aplicamos las reglas de calidad a dfEmpresa
var dfEmpresaLimpio = dfEmpresa.filter(
  (dfEmpresa.col("ID_EMPRESA").isNotNull) && 
  (dfEmpresa.col("NOMBRE_EMPRESA").isNotNull)
)

//Vemos el esquema
dfEmpresaLimpio.printSchema()

root
 |-- ID_EMPRESA: string (nullable = true)
 |-- NOMBRE_EMPRESA: string (nullable = true)


//Mostramos los datos
dfEmpresaLimpio.show()

+----------+--------------+
|ID_EMPRESA|NOMBRE_EMPRESA|
+----------+--------------+
|         9|           IBM|
|         5|        Amazon|
|        10|          Sony|
|         2|     Microsoft|
|         7|       Samsung|
|         1|       Walmart|
|         6|        Google|
|         4|        Toyota|
|         3|         Apple|
|         8|            HP|
+----------+--------------+


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

root
 |-- ID_CLIENTE: string (nullable = true)
 |-- RIESGO_CENTRAL_1: double (nullable = true)
 |-- RIESGO_CENTRAL_2: double (nullable = true)
 |-- RIESGO_CENTRAL_3: double (nullable = true)


//Mostramos los datos
dfRiesgoLimpio.show()

+----------+----------------+----------------+----------------+
|ID_CLIENTE|RIESGO_CENTRAL_1|RIESGO_CENTRAL_2|RIESGO_CENTRAL_3|
+----------+----------------+----------------+----------------+
|         1|             0.1|             0.6|             0.4|
|         2|             0.2|             0.8|             0.3|
|         3|             0.9|             0.8|             0.8|
|         4|             0.5|             0.4|             0.6|
|         5|             0.6|             0.6|             0.6|
|         6|             0.3|             0.4|             0.6|
|         7|             0.2|             0.4|             0.3|
|         8|             0.7|             0.9|             0.7|
|         9|             0.3|             1.0|             1.0|
|        10|             0.9|             1.0|             0.4|
|        11|             0.6|             0.1|             0.3|
|        12|             0.7|             0.6|             0.7|
|        13|             0.7|             0.1|             0.5|
|        14|             0.1|             0.2|             0.0|
|        15|             0.9|             0.7|             0.6|
|        16|             1.0|             0.4|             0.7|
|        17|             0.0|             0.2|             0.5|
|        18|             0.7|             0.5|             1.0|
|        19|             0.6|             0.4|             0.6|
|        20|             0.1|             0.6|             0.8|
+----------+----------------+----------------+----------------+
only showing top 20 rows


// DBTITLE 1,5. Preparación de tablones
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
df1.show()

df1:org.apache.spark.sql.DataFrame
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:long
SALARIO_PERSONA:double
ID_EMPRESA:string
MONTO_TRANSACCION:double
FECHA_TRANSACCION:string

+----------+--------------+------------+---------------+----------+-----------------+-----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|
+----------+--------------+------------+---------------+----------+-----------------+-----------------+
|        24|         Amaya|          24|         1801.0|         6|           1745.0|       2018-12-05|
|         1|          Carl|          32|        20095.0|        10|            238.0|       2018-04-19|
|        65|         Nehru|          34|        12423.0|         2|           4097.0|       2018-04-19|
|        71|         Doris|          23|        11538.0|         5|           1548.0|       2018-12-05|
|        83|       Giselle|          45|         2503.0|         5|           2233.0|       2018-04-19|
|        96|          Amos|          42|        15855.0|         4|           2887.0|       2018-04-19|
|        19|         Laura|          70|        17403.0|         9|            286.0|       2018-04-19|
|        40|          Ross|          67|        14285.0|         4|            974.0|       2018-04-19|
|       100|       Cynthia|          57|         8682.0|         8|           2698.0|       2018-04-19|
|        22|          Kibo|          22|         7449.0|         2|           1398.0|       2018-12-05|
|         8|         Jonah|          23|        17040.0|         7|           2538.0|       2018-12-05|
|        73|         Fiona|          42|         9960.0|         4|           3878.0|       2018-11-28|
|        76|          Omar|          34|        12163.0|         5|            375.0|       2018-11-28|
|        63|          Sade|          70|        11112.0|         5|            707.0|       2018-11-28|
|        80|         Ebony|          59|         3600.0|         7|           4250.0|       2018-11-28|
|        80|         Ebony|          59|         3600.0|         5|           2232.0|       2018-11-28|
|        32|        Gisela|          67|         6497.0|         7|           4493.0|       2018-11-28|
|        84|         Keith|          33|        13348.0|         1|            769.0|       2018-12-05|
|        35|        Aurora|          54|         4588.0|         3|           3546.0|       2018-04-19|
|        60|       Bernard|          27|        10825.0|        10|           3399.0|       2018-11-28|
+----------+--------------+------------+---------------+----------+-----------------+-----------------+
only showing top 20 rows


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
df2.show()

df2:org.apache.spark.sql.DataFrame
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:long
SALARIO_PERSONA:double
ID_EMPRESA:string
NOMBRE_EMPRESA:string
MONTO_TRANSACCION:double
FECHA_TRANSACCION:string

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|        24|         Amaya|          24|         1801.0|         6|        Google|           1745.0|       2018-12-05|
|         1|          Carl|          32|        20095.0|        10|          Sony|            238.0|       2018-04-19|
|        65|         Nehru|          34|        12423.0|         2|     Microsoft|           4097.0|       2018-04-19|
|        71|         Doris|          23|        11538.0|         5|        Amazon|           1548.0|       2018-12-05|
|        83|       Giselle|          45|         2503.0|         5|        Amazon|           2233.0|       2018-04-19|
|        96|          Amos|          42|        15855.0|         4|        Toyota|           2887.0|       2018-04-19|
|        19|         Laura|          70|        17403.0|         9|           IBM|            286.0|       2018-04-19|
|        40|          Ross|          67|        14285.0|         4|        Toyota|            974.0|       2018-04-19|
|       100|       Cynthia|          57|         8682.0|         8|            HP|           2698.0|       2018-04-19|
|        22|          Kibo|          22|         7449.0|         2|     Microsoft|           1398.0|       2018-12-05|
|         8|         Jonah|          23|        17040.0|         7|       Samsung|           2538.0|       2018-12-05|
|        73|         Fiona|          42|         9960.0|         4|        Toyota|           3878.0|       2018-11-28|
|        76|          Omar|          34|        12163.0|         5|        Amazon|            375.0|       2018-11-28|
|        63|          Sade|          70|        11112.0|         5|        Amazon|            707.0|       2018-11-28|
|        80|         Ebony|          59|         3600.0|         7|       Samsung|           4250.0|       2018-11-28|
|        80|         Ebony|          59|         3600.0|         5|        Amazon|           2232.0|       2018-11-28|
|        32|        Gisela|          67|         6497.0|         7|       Samsung|           4493.0|       2018-11-28|
|        84|         Keith|          33|        13348.0|         1|       Walmart|            769.0|       2018-12-05|
|        35|        Aurora|          54|         4588.0|         3|         Apple|           3546.0|       2018-04-19|
|        60|       Bernard|          27|        10825.0|        10|          Sony|           3399.0|       2018-11-28|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
only showing top 20 rows


//PASO 3 (<<RIESGO PONDERADO>>): OBTENEMOS EL RIESGO CREDITICIO PONDERADO (1*R_1 + 2*R_2 + 1*R_3) / 4

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
df3.show()

df3:org.apache.spark.sql.DataFrame
ID_CLIENTE:string
RIESGO_PONDERADO:double

+----------+-------------------+
|ID_CLIENTE|   RIESGO_PONDERADO|
+----------+-------------------+
|         1|0.42500000000000004|
|         2|              0.525|
|         3|              0.825|
|         4|              0.475|
|         5|                0.6|
|         6|0.42500000000000004|
|         7|              0.325|
|         8|                0.8|
|         9|              0.825|
|        10|              0.825|
|        11|              0.275|
|        12| 0.6499999999999999|
|        13|               0.35|
|        14|              0.125|
|        15|              0.725|
|        16|              0.625|
|        17|              0.225|
|        18|              0.675|
|        19|                0.5|
|        20|              0.525|
+----------+-------------------+
only showing top 20 rows


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
dfTablon.show()

dfTablon:org.apache.spark.sql.DataFrame
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:long
SALARIO_PERSONA:double
ID_EMPRESA:string
NOMBRE_EMPRESA:string
MONTO_TRANSACCION:double
FECHA_TRANSACCION:string
RIESGO_PONDERADO:double

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|RIESGO_PONDERADO|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
|        94|          Amir|          18|        20980.0|         8|            HP|           1933.0|       2018-12-05|           0.575|
|        94|          Amir|          18|        20980.0|        10|          Sony|           2680.0|       2018-12-05|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           2234.0|       2018-04-19|           0.575|
|        94|          Amir|          18|        20980.0|         9|           IBM|           2771.0|       2018-11-28|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           1186.0|       2018-04-19|           0.575|
|        94|          Amir|          18|        20980.0|         9|           IBM|           1155.0|       2018-12-05|           0.575|
|        94|          Amir|          18|        20980.0|         2|     Microsoft|           2529.0|       2018-12-05|           0.575|
|        94|          Amir|          18|        20980.0|         3|         Apple|           3195.0|       2018-11-28|           0.575|
|        94|          Amir|          18|        20980.0|         6|        Google|           3177.0|       2018-04-19|           0.575|
|        94|          Amir|          18|        20980.0|         2|     Microsoft|           1877.0|       2018-11-28|           0.575|
|        94|          Amir|          18|        20980.0|         3|         Apple|           1476.0|       2018-04-19|           0.575|
|        94|          Amir|          18|        20980.0|         7|       Samsung|           2011.0|       2018-12-05|           0.575|
|        94|          Amir|          18|        20980.0|         2|     Microsoft|           3136.0|       2018-11-28|           0.575|
|        94|          Amir|          18|        20980.0|         8|            HP|           2572.0|       2018-12-05|           0.575|
|        94|          Amir|          18|        20980.0|        10|          Sony|            466.0|       2018-12-05|           0.575|
|        94|          Amir|          18|        20980.0|        10|          Sony|           3392.0|       2018-11-28|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           2718.0|       2018-12-05|           0.575|
|        94|          Amir|          18|        20980.0|         1|       Walmart|           1257.0|       2018-11-28|           0.575|
|        94|          Amir|          18|        20980.0|         7|       Samsung|           3218.0|       2018-11-28|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           1217.0|       2018-12-05|           0.575|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
only showing top 20 rows


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

//Mostramos los datos
dfTablon1.show()

dfTablon1:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:long
SALARIO_PERSONA:double
ID_EMPRESA:string
NOMBRE_EMPRESA:string
MONTO_TRANSACCION:double
FECHA_TRANSACCION:string
RIESGO_PONDERADO:double

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|RIESGO_PONDERADO|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
|        94|          Amir|          18|        20980.0|         5|        Amazon|           2234.0|       2018-04-19|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           1186.0|       2018-04-19|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           2718.0|       2018-12-05|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           1217.0|       2018-12-05|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           3118.0|       2018-04-19|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           2233.0|       2018-11-28|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           2011.0|       2018-12-05|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           2954.0|       2018-12-05|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           1527.0|       2018-11-28|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           3894.0|       2018-04-19|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           1656.0|       2018-04-19|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           3643.0|       2018-12-05|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|            961.0|       2018-12-05|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           4212.0|       2018-04-19|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           1556.0|       2018-04-19|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           4021.0|       2018-04-19|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           2151.0|       2018-11-28|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           2496.0|       2018-04-19|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|           2088.0|       2018-04-19|           0.575|
|        94|          Amir|          18|        20980.0|         5|        Amazon|            683.0|       2018-11-28|           0.575|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
only showing top 20 rows


//REPORTE 1:
// - POR PERSONAS ENTRE 30 A 39 AÑOS
// - CON UN SALARIO DE 1000 A 5000 DOLARES
var dfReporte1 = dfTablon1.filter(
  (dfTablon1.col("EDAD_PERSONA") >= 30) &&
  (dfTablon1.col("EDAD_PERSONA") <= 39) &&
  (dfTablon1.col("SALARIO_PERSONA") >= 1000) &&
  (dfTablon1.col("SALARIO_PERSONA") <= 5000)
)

//Mostramos los datos
dfReporte1.show()

dfReporte1:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:long
SALARIO_PERSONA:double
ID_EMPRESA:string
NOMBRE_EMPRESA:string
MONTO_TRANSACCION:double
FECHA_TRANSACCION:string
RIESGO_PONDERADO:double

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|RIESGO_PONDERADO|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
|        18|          Owen|          34|         4759.0|         5|        Amazon|           2573.0|       2018-04-19|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|            815.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3385.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|            886.0|       2018-04-19|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3622.0|       2018-11-28|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3206.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3024.0|       2018-11-28|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3748.0|       2018-11-28|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3724.0|       2018-11-28|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           4040.0|       2018-04-19|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           2844.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           1498.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           1734.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3470.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3310.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           1396.0|       2018-04-19|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           1026.0|       2018-11-28|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           2475.0|       2018-04-19|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           2171.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3870.0|       2018-12-05|           0.675|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
only showing top 20 rows


//REPORTE 2:
// - POR PERSONAS ENTRE 40 A 49 AÑOS
// - CON UN SALARIO DE 2500 A 7000 DOLARES
var dfReporte2 = dfTablon1.filter(
  (dfTablon1.col("EDAD_PERSONA") >= 40) &&
  (dfTablon1.col("EDAD_PERSONA") <= 49) &&
  (dfTablon1.col("SALARIO_PERSONA") >= 2500) &&
  (dfTablon1.col("SALARIO_PERSONA") <= 7000)
)

//Mostramos los datos
dfReporte2.show()

dfReporte2:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:long
SALARIO_PERSONA:double
ID_EMPRESA:string
NOMBRE_EMPRESA:string
MONTO_TRANSACCION:double
FECHA_TRANSACCION:string
RIESGO_PONDERADO:double

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+------------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|  RIESGO_PONDERADO|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+------------------+
|        51|         Damon|          49|         2669.0|         5|        Amazon|           2426.0|       2018-11-28|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           2329.0|       2018-12-05|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1515.0|       2018-11-28|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1299.0|       2018-12-05|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           4400.0|       2018-12-05|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           3503.0|       2018-11-28|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1320.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           2585.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           3461.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1149.0|       2018-11-28|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1716.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|            596.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           3120.0|       2018-12-05|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           4461.0|       2018-11-28|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           2590.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           3225.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           2224.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|            775.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           4275.0|       2018-11-28|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           3382.0|       2018-12-05|0.6749999999999999|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+------------------+
only showing top 20 rows


//REPORTE 3:
// - POR PERSONAS ENTRE 50 A 60 AÑOS
// - CON UN SALARIO DE 3500 A 10000 DOLARES
var dfReporte3 = dfTablon1.filter(
  (dfTablon1.col("EDAD_PERSONA") >= 50) &&
  (dfTablon1.col("EDAD_PERSONA") <= 60) &&
  (dfTablon1.col("SALARIO_PERSONA") >= 3500) &&
  (dfTablon1.col("SALARIO_PERSONA") <= 10000)
)

//Mostramos los datos
dfReporte3.show()

dfReporte3:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:long
SALARIO_PERSONA:double
ID_EMPRESA:string
NOMBRE_EMPRESA:string
MONTO_TRANSACCION:double
FECHA_TRANSACCION:string
RIESGO_PONDERADO:double

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|RIESGO_PONDERADO|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
|        38|          Irma|          58|         6747.0|         5|        Amazon|           3385.0|       2018-12-05|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           1163.0|       2018-04-19|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           3174.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           1997.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2714.0|       2018-04-19|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2829.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           4158.0|       2018-04-19|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2906.0|       2018-04-19|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2308.0|       2018-12-05|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           3215.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           1711.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2537.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           1438.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           1269.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           3221.0|       2018-04-19|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           4355.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2803.0|       2018-12-05|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           1626.0|       2018-04-19|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2652.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2295.0|       2018-04-19|           0.525|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+


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

dfReporteLeido1:org.apache.spark.sql.DataFrame
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:string
SALARIO_PERSONA:string
ID_EMPRESA:string
NOMBRE_EMPRESA:string
MONTO_TRANSACCION:string
FECHA_TRANSACCION:string
RIESGO_PONDERADO:string

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|RIESGO_PONDERADO|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
|        18|          Owen|          34|         4759.0|         5|        Amazon|           2573.0|       2018-04-19|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|            815.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3385.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|            886.0|       2018-04-19|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3622.0|       2018-11-28|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3206.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3024.0|       2018-11-28|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3748.0|       2018-11-28|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3724.0|       2018-11-28|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           4040.0|       2018-04-19|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           2844.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           1498.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           1734.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3470.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3310.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           1396.0|       2018-04-19|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           1026.0|       2018-11-28|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           2475.0|       2018-04-19|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           2171.0|       2018-12-05|           0.675|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3870.0|       2018-12-05|           0.675|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
only showing top 20 rows


//Verificamos los archivos
var dfReporteLeido2 = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("dbfs:///FileStore/_spark/output/REPORTE_2")
dfReporteLeido2.show()

dfReporteLeido2:org.apache.spark.sql.DataFrame
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:string
SALARIO_PERSONA:string
ID_EMPRESA:string
NOMBRE_EMPRESA:string
MONTO_TRANSACCION:string
FECHA_TRANSACCION:string
RIESGO_PONDERADO:string

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+------------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|  RIESGO_PONDERADO|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+------------------+
|        51|         Damon|          49|         2669.0|         5|        Amazon|           2426.0|       2018-11-28|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           2329.0|       2018-12-05|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1515.0|       2018-11-28|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1299.0|       2018-12-05|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           4400.0|       2018-12-05|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           3503.0|       2018-11-28|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1320.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           2585.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           3461.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1149.0|       2018-11-28|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1716.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|            596.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           3120.0|       2018-12-05|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           4461.0|       2018-11-28|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           2590.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           3225.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           2224.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|            775.0|       2018-04-19|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           4275.0|       2018-11-28|0.6749999999999999|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           3382.0|       2018-12-05|0.6749999999999999|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+------------------+
only showing top 20 rows


//Verificamos los archivos
var dfReporteLeido3 = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("dbfs:///FileStore/_spark/output/REPORTE_3")
dfReporteLeido3.show()

dfReporteLeido3:org.apache.spark.sql.DataFrame
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:string
SALARIO_PERSONA:string
ID_EMPRESA:string
NOMBRE_EMPRESA:string
MONTO_TRANSACCION:string
FECHA_TRANSACCION:string
RIESGO_PONDERADO:string

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|RIESGO_PONDERADO|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+
|        38|          Irma|          58|         6747.0|         5|        Amazon|           3385.0|       2018-12-05|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           1163.0|       2018-04-19|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           3174.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           1997.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2714.0|       2018-04-19|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2829.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           4158.0|       2018-04-19|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2906.0|       2018-04-19|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2308.0|       2018-12-05|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           3215.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           1711.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2537.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           1438.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           1269.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           3221.0|       2018-04-19|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           4355.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2803.0|       2018-12-05|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           1626.0|       2018-04-19|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2652.0|       2018-11-28|           0.525|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           2295.0|       2018-04-19|           0.525|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+----------------+