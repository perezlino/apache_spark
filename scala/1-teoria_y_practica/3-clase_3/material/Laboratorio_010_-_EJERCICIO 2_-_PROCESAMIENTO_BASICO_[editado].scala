// Databricks notebook source
//COPYRIGHT: ALFONSO PEREZ [perezlino@gmail.com]
//AUTHOR: ALFONSO PEREZ [perezlino@gmail.com]


// DBTITLE 1,1. Librerías
//Objetos para definir la metadata
import org.apache.spark.sql.types.{StructType, StructField}

//Importamos los tipos de datos que usaremos
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}

//Podemos importar todos los utilitarios con la siguiente sentencia
import org.apache.spark.sql.types._

//Importamos todos los objetos utilitarios dentro de una variable
import org.apache.spark.sql.{functions => f}


// DBTITLE 1,2. Lectura de datos
//Leemos el archivo indicando el esquema
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
).load("dbfs:///FileStore/_spark/DATA_PERSONA.txt")

//Vemos el esquema
dfPersona.printSchema()

root
 |-- ID: string (nullable = true)
 |-- NOMBRE: string (nullable = true)
 |-- TELEFONO: string (nullable = true)
 |-- CORREO: string (nullable = true)
 |-- FECHA_INGRESO: string (nullable = true)
 |-- EDAD: integer (nullable = true)
 |-- SALARIO: double (nullable = true)
 |-- ID_EMPRESA: string (nullable = true)


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


//Leemos el archivo indicando el esquema
var dfEmpresa = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        Array(
            StructField("ID", StringType, true),
            StructField("NOMBRE", StringType, true)
        )
    )
).load("dbfs:///FileStore/_spark/DATA_EMPRESA.txt")

//Vemos el esquema
dfEmpresa.printSchema()

root
 |-- ID: string (nullable = true)
 |-- NOMBRE: string (nullable = true)


//Mostramos los datos
dfEmpresa.show()

+---+---------+
| ID|   NOMBRE|
+---+---------+
|  1|  Walmart|
|  2|Microsoft|
|  3|    Apple|
|  4|   Toyota|
|  5|   Amazon|
|  6|   Google|
|  7|  Samsung|
|  8|       HP|
|  9|      IBM|
| 10|     Sony|
+---+---------+


//Leemos el archivo indicando el esquema
var dfTransaccion = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        Array(
          StructField("ID_PERSONA", StringType, true),
          StructField("ID_EMPRESA", StringType, true),
          StructField("MONTO", DoubleType, true),
          StructField("FECHA", StringType, true)
        )
    )
).load("dbfs:///FileStore/_spark/DATA_TRANSACCION.txt")

//Vemos el esquema
dfTransaccion.printSchema()

root
 |-- ID_PERSONA: string (nullable = true)
 |-- ID_EMPRESA: string (nullable = true)
 |-- MONTO: double (nullable = true)
 |-- FECHA: string (nullable = true)


//Mostramos los datos
dfTransaccion.show()

+----------+----------+------+----------+
|ID_PERSONA|ID_EMPRESA| MONTO|     FECHA|
+----------+----------+------+----------+
|        18|         3|1383.0|2018-01-21|
|        30|         6|2331.0|2018-01-21|
|        47|         2|2280.0|2018-01-21|
|        28|         1| 730.0|2018-01-21|
|        91|         4|3081.0|2018-01-21|
|        74|         8|2409.0|2018-01-21|
|        41|         2|3754.0|2018-01-22|
|        42|         9|4079.0|2018-01-22|
|        24|         6|4475.0|2018-01-22|
|        67|         9| 561.0|2018-01-22|
|         9|         4|3765.0|2018-01-22|
|        97|         3|3669.0|2018-01-22|
|        91|         5|3497.0|2018-01-22|
|        61|         3| 735.0|2018-01-23|
|        15|         5| 367.0|2018-01-23|
|        20|         9|2039.0|2018-01-23|
|        11|         4| 719.0|2018-01-23|
|        36|         2|2659.0|2018-01-23|
|        12|         4| 467.0|2018-01-23|
|        38|         9|2411.0|2018-01-23|
+----------+----------+------+----------+
only showing top 20 rows


// DBTITLE 1,3. Reglas de calidad
//Aplicamos las reglas de calidad a dfTransaccion
var dfTransaccionLimpio = dfTransaccion.filter(
  (dfTransaccion.col("ID_PERSONA").isNotNull) &&
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
|        18|         3|1383.0|2018-01-21|
|        30|         6|2331.0|2018-01-21|
|        47|         2|2280.0|2018-01-21|
|        28|         1| 730.0|2018-01-21|
|        91|         4|3081.0|2018-01-21|
|        74|         8|2409.0|2018-01-21|
|        41|         2|3754.0|2018-01-22|
|        42|         9|4079.0|2018-01-22|
|        24|         6|4475.0|2018-01-22|
|        67|         9| 561.0|2018-01-22|
|         9|         4|3765.0|2018-01-22|
|        97|         3|3669.0|2018-01-22|
|        91|         5|3497.0|2018-01-22|
|        61|         3| 735.0|2018-01-23|
|        15|         5| 367.0|2018-01-23|
|        20|         9|2039.0|2018-01-23|
|        11|         4| 719.0|2018-01-23|
|        36|         2|2659.0|2018-01-23|
|        12|         4| 467.0|2018-01-23|
|        38|         9|2411.0|2018-01-23|
+----------+----------+------+----------+
only showing top 20 rows


//Aplicamos las reglas de calidad a dfPersona
var dfPersonaLimpio = dfPersona.filter(
  (dfPersona.col("ID").isNotNull) &&
  (dfPersona.col("NOMBRE").isNotNull) &&
  (dfPersona.col("SALARIO").isNotNull) &&
  (dfPersona.col("SALARIO") > 0)
)

//Vemos el esquema
dfPersonaLimpio.printSchema()

root
 |-- ID: string (nullable = true)
 |-- NOMBRE: string (nullable = true)
 |-- TELEFONO: string (nullable = true)
 |-- CORREO: string (nullable = true)
 |-- FECHA_INGRESO: string (nullable = true)
 |-- EDAD: integer (nullable = true)
 |-- SALARIO: double (nullable = true)
 |-- ID_EMPRESA: string (nullable = true)


//Mostramos los datos
dfPersonaLimpio.show()

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


//Aplicamos las reglas de calidad a dfEmpresa
var dfEmpresaLimpio = dfEmpresa.filter(
  (dfEmpresa.col("ID").isNotNull) && 
  (dfEmpresa.col("NOMBRE").isNotNull)
)

//Vemos el esquema
dfEmpresaLimpio.printSchema()

root
 |-- ID: string (nullable = true)
 |-- NOMBRE: string (nullable = true)


//Mostramos los datos
dfEmpresaLimpio.show()

+---+---------+
| ID|   NOMBRE|
+---+---------+
|  1|  Walmart|
|  2|Microsoft|
|  3|    Apple|
|  4|   Toyota|
|  5|   Amazon|
|  6|   Google|
|  7|  Samsung|
|  8|       HP|
|  9|      IBM|
| 10|     Sony|
+---+---------+


// DBTITLE 1,4. Preparación de tablones
//PASO 1 (<<T_P>>): AGREGAR LOS DATOS DE LAS PERSONAS QUE REALIZARON LAS TRANSACCIONES
var df1 = dfTransaccionLimpio.join(
  dfPersonaLimpio,
  dfTransaccionLimpio.col("ID_PERSONA") === dfPersonaLimpio.col("ID"),
  "inner"
).select(
  dfTransaccionLimpio.col("ID_PERSONA"),
  dfPersonaLimpio.col("NOMBRE").alias("NOMBRE_PERSONA"),
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
EDAD_PERSONA:integer
SALARIO_PERSONA:double
ID_EMPRESA:string
MONTO_TRANSACCION:double
FECHA_TRANSACCION:string

+----------+--------------+------------+---------------+----------+-----------------+-----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|
+----------+--------------+------------+---------------+----------+-----------------+-----------------+
|        18|          Owen|          34|         4759.0|         3|           1383.0|       2018-01-21|
|        30|       Clayton|          52|         9505.0|         6|           2331.0|       2018-01-21|
|        47|        Vernon|          35|         7109.0|         2|           2280.0|       2018-01-21|
|        28|       Stephen|          53|         9469.0|         1|            730.0|       2018-01-21|
|        91|         Erica|          32|         8934.0|         4|           3081.0|       2018-01-21|
|        74|       Kaitlin|          56|         6515.0|         8|           2409.0|       2018-01-21|
|        41|         Wynne|          31|        19522.0|         2|           3754.0|       2018-01-22|
|        42|         Wanda|          42|         5419.0|         9|           4079.0|       2018-01-22|
|        24|         Amaya|          24|         1801.0|         6|           4475.0|       2018-01-22|
|        67|         Buffy|          38|        15116.0|         9|            561.0|       2018-01-22|
|         9|         Hanae|          69|         6834.0|         4|           3765.0|       2018-01-22|
|        97|        Flavia|          27|        13473.0|         3|           3669.0|       2018-01-22|
|        91|         Erica|          32|         8934.0|         5|           3497.0|       2018-01-22|
|        61|          Abel|          33|        15070.0|         3|            735.0|       2018-01-23|
|        15|         Wanda|          27|         1539.0|         5|            367.0|       2018-01-23|
|        20|         Emery|          24|        18752.0|         9|           2039.0|       2018-01-23|
|        11|       Melyssa|          48|         4913.0|         4|            719.0|       2018-01-23|
|        36|         Keely|          41|        10373.0|         2|           2659.0|       2018-01-23|
|        12|        Tanner|          24|        19943.0|         4|            467.0|       2018-01-23|
|        38|          Irma|          58|         6747.0|         9|           2411.0|       2018-01-23|
+----------+--------------+------------+---------------+----------+-----------------+-----------------+
only showing top 20 rows


//PASO 2 (<<T_P_E>>): AGREGAR LOS DATOS DE LAS EMPRESAS EN DONDE SE REALIZARON LAS TRANSACCIONES
var df2 = df1.join(
  dfEmpresaLimpio,
  df1.col("ID_EMPRESA") === dfEmpresaLimpio.col("ID"),
  "inner"
).select(
  df1.col("ID_PERSONA"),
  df1.col("NOMBRE_PERSONA"),
  df1.col("EDAD_PERSONA"),
  df1.col("SALARIO_PERSONA"),
  df1.col("ID_EMPRESA"),
  dfEmpresaLimpio.col("NOMBRE").alias("NOMBRE_EMPRESA"),
  df1.col("MONTO_TRANSACCION"),
  df1.col("FECHA_TRANSACCION")
)

//Mostramos los datos
df2.show()

df2:org.apache.spark.sql.DataFrame
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:integer
SALARIO_PERSONA:double
ID_EMPRESA:string
NOMBRE_EMPRESA:string
MONTO_TRANSACCION:double
FECHA_TRANSACCION:string

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|        18|          Owen|          34|         4759.0|         3|         Apple|           1383.0|       2018-01-21|
|        30|       Clayton|          52|         9505.0|         6|        Google|           2331.0|       2018-01-21|
|        47|        Vernon|          35|         7109.0|         2|     Microsoft|           2280.0|       2018-01-21|
|        28|       Stephen|          53|         9469.0|         1|       Walmart|            730.0|       2018-01-21|
|        91|         Erica|          32|         8934.0|         4|        Toyota|           3081.0|       2018-01-21|
|        74|       Kaitlin|          56|         6515.0|         8|            HP|           2409.0|       2018-01-21|
|        41|         Wynne|          31|        19522.0|         2|     Microsoft|           3754.0|       2018-01-22|
|        42|         Wanda|          42|         5419.0|         9|           IBM|           4079.0|       2018-01-22|
|        24|         Amaya|          24|         1801.0|         6|        Google|           4475.0|       2018-01-22|
|        67|         Buffy|          38|        15116.0|         9|           IBM|            561.0|       2018-01-22|
|         9|         Hanae|          69|         6834.0|         4|        Toyota|           3765.0|       2018-01-22|
|        97|        Flavia|          27|        13473.0|         3|         Apple|           3669.0|       2018-01-22|
|        91|         Erica|          32|         8934.0|         5|        Amazon|           3497.0|       2018-01-22|
|        61|          Abel|          33|        15070.0|         3|         Apple|            735.0|       2018-01-23|
|        15|         Wanda|          27|         1539.0|         5|        Amazon|            367.0|       2018-01-23|
|        20|         Emery|          24|        18752.0|         9|           IBM|           2039.0|       2018-01-23|
|        11|       Melyssa|          48|         4913.0|         4|        Toyota|            719.0|       2018-01-23|
|        36|         Keely|          41|        10373.0|         2|     Microsoft|           2659.0|       2018-01-23|
|        12|        Tanner|          24|        19943.0|         4|        Toyota|            467.0|       2018-01-23|
|        38|          Irma|          58|         6747.0|         9|           IBM|           2411.0|       2018-01-23|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
only showing top 20 rows


// DBTITLE 1,5. Procesamiento
//REPORTE 1:
//
// - TRANSACCIONES MAYORES A 500 DÓLARES
// - REALIZADAS EN AMAZON
// - POR PERSONAS ENTRE 30 A 39 AÑOS
// - CON UN SALARIO DE 1000 A 5000 DOLARES
//
//REPORTE 2:
//
// - TRANSACCIONES MAYORES A 500 DÓLARES
// - REALIZADAS EN AMAZON
// - POR PERSONAS ENTRE 40 A 49 AÑOS
// - CON UN SALARIO DE 2500 A 7000 DOLARES
//
//REPORTE 3:
//
// - TRANSACCIONES MAYORES A 500 DÓLARES
// - REALIZADAS EN AMAZON
// - POR PERSONAS ENTRE 50 A 60 AÑOS
// - CON UN SALARIO DE 3500 A 10000 DOLARES
//
//Notamos que todos los reportes comparten:
// - TRANSACCIONES MAYORES A 500 DÓLARES
// - REALIZADAS EN AMAZON



//Calculamos las reglas comunes en un dataframe
var dfTablon = df2.filter(
  (df2.col("MONTO_TRANSACCION") > 500) &&
  (df2.col("NOMBRE_EMPRESA") === "Amazon")
)

//Mostramos los datos
dfTablon.show()

dfTablon:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:integer
SALARIO_PERSONA:double
ID_EMPRESA:string
NOMBRE_EMPRESA:string
MONTO_TRANSACCION:double
FECHA_TRANSACCION:string

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|        91|         Erica|          32|         8934.0|         5|        Amazon|           3497.0|       2018-01-22|
|        83|       Giselle|          45|         2503.0|         5|        Amazon|           2079.0|       2018-01-21|
|        57|       Jillian|          47|        13445.0|         5|        Amazon|           1992.0|       2018-01-23|
|         7|          Mark|          52|         8112.0|         5|        Amazon|           3352.0|       2018-01-21|
|        56|         Colby|          19|        15496.0|         5|        Amazon|            511.0|       2018-01-21|
|         7|          Mark|          52|         8112.0|         5|        Amazon|           1174.0|       2018-01-21|
|        57|       Jillian|          47|        13445.0|         5|        Amazon|           3002.0|       2018-01-22|
|        56|         Colby|          19|        15496.0|         5|        Amazon|            796.0|       2018-01-23|
|        62|        Amelia|          35|         6042.0|         5|        Amazon|            820.0|       2018-01-21|
|        96|          Amos|          42|        15855.0|         5|        Amazon|           2898.0|       2018-01-22|
|        35|        Aurora|          54|         4588.0|         5|        Amazon|            547.0|       2018-01-22|
|        12|        Tanner|          24|        19943.0|         5|        Amazon|           2947.0|       2018-01-21|
|        86|          Jack|          58|        14473.0|         5|        Amazon|           2337.0|       2018-01-21|
|        76|          Omar|          34|        12163.0|         5|        Amazon|           4464.0|       2018-01-22|
|        84|         Keith|          33|        13348.0|         5|        Amazon|           1493.0|       2018-01-23|
|        45|        Kylynn|          22|         7040.0|         5|        Amazon|            735.0|       2018-01-22|
|        36|         Keely|          41|        10373.0|         5|        Amazon|           1269.0|       2018-01-22|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1971.0|       2018-01-23|
|         5|       Leandra|          41|        22102.0|         5|        Amazon|           1973.0|       2018-01-21|
|        32|        Gisela|          67|         6497.0|         5|        Amazon|            642.0|       2018-01-22|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
only showing top 20 rows


//REPORTE 1:
// - POR PERSONAS ENTRE 30 A 39 AÑOS
// - CON UN SALARIO DE 1000 A 5000 DOLARES
var dfReporte1 = dfTablon.filter(
  (dfTablon.col("EDAD_PERSONA") >= 30) &&
  (dfTablon.col("EDAD_PERSONA") <= 39) &&
  (dfTablon.col("SALARIO_PERSONA") >= 1000) &&
  (dfTablon.col("SALARIO_PERSONA") <= 5000)
)

//Mostramos los datos
dfReporte1.show()

dfReporte1:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:integer
SALARIO_PERSONA:double
ID_EMPRESA:string
NOMBRE_EMPRESA:string
MONTO_TRANSACCION:double
FECHA_TRANSACCION:string

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           1518.0|       2018-01-23|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3406.0|       2018-01-23|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           3211.0|       2018-01-23|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           1292.0|       2018-01-23|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           1146.0|       2018-01-23|
|        18|          Owen|          34|         4759.0|         5|        Amazon|            743.0|       2018-01-21|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           2136.0|       2018-01-21|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           1767.0|       2018-01-21|
|        18|          Owen|          34|         4759.0|         5|        Amazon|            850.0|       2018-01-22|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           4148.0|       2018-01-23|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           4308.0|       2018-01-23|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           1699.0|       2018-01-23|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           1320.0|       2018-01-22|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           3771.0|       2018-01-23|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           2242.0|       2018-01-23|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           2513.0|       2018-01-23|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           1281.0|       2018-01-22|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           1482.0|       2018-01-21|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           3498.0|       2018-01-22|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           1614.0|       2018-01-22|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
only showing top 20 rows


//REPORTE 2:
// - POR PERSONAS ENTRE 40 A 49 AÑOS
// - CON UN SALARIO DE 2500 A 7000 DOLARES
var dfReporte2 = dfTablon.filter(
  (dfTablon.col("EDAD_PERSONA") >= 40) &&
  (dfTablon.col("EDAD_PERSONA") <= 49) &&
  (dfTablon.col("SALARIO_PERSONA") >= 2500) &&
  (dfTablon.col("SALARIO_PERSONA") <= 7000)
)

//Mostramos los datos
dfReporte2.show()

dfReporte2:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:integer
SALARIO_PERSONA:double
ID_EMPRESA:string
NOMBRE_EMPRESA:string
MONTO_TRANSACCION:double
FECHA_TRANSACCION:string

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|        83|       Giselle|          45|         2503.0|         5|        Amazon|           2079.0|       2018-01-21|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1971.0|       2018-01-23|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1149.0|       2018-01-22|
|        83|       Giselle|          45|         2503.0|         5|        Amazon|           1890.0|       2018-01-23|
|        11|       Melyssa|          48|         4913.0|         5|        Amazon|           3111.0|       2018-01-21|
|        42|         Wanda|          42|         5419.0|         5|        Amazon|           2772.0|       2018-01-23|
|        83|       Giselle|          45|         2503.0|         5|        Amazon|           3487.0|       2018-01-21|
|        42|         Wanda|          42|         5419.0|         5|        Amazon|           2044.0|       2018-01-22|
|        11|       Melyssa|          48|         4913.0|         5|        Amazon|           2629.0|       2018-01-21|
|        11|       Melyssa|          48|         4913.0|         5|        Amazon|           1789.0|       2018-01-23|
|        42|         Wanda|          42|         5419.0|         5|        Amazon|           1529.0|       2018-01-22|
|        83|       Giselle|          45|         2503.0|         5|        Amazon|           2397.0|       2018-01-23|
|        11|       Melyssa|          48|         4913.0|         5|        Amazon|           2329.0|       2018-01-23|
|        83|       Giselle|          45|         2503.0|         5|        Amazon|           3131.0|       2018-01-23|
|        83|       Giselle|          45|         2503.0|         5|        Amazon|           3472.0|       2018-01-23|
|        42|         Wanda|          42|         5419.0|         5|        Amazon|           1602.0|       2018-01-22|
|        11|       Melyssa|          48|         4913.0|         5|        Amazon|            727.0|       2018-01-21|
|        42|         Wanda|          42|         5419.0|         5|        Amazon|           3706.0|       2018-01-22|
|        11|       Melyssa|          48|         4913.0|         5|        Amazon|            881.0|       2018-01-21|
|        42|         Wanda|          42|         5419.0|         5|        Amazon|            991.0|       2018-01-23|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
only showing top 20 rows


//REPORTE 3:
// - POR PERSONAS ENTRE 50 A 60 AÑOS
// - CON UN SALARIO DE 3500 A 10000 DOLARES
var dfReporte3 = dfTablon.filter(
  (dfTablon.col("EDAD_PERSONA") >= 50) &&
  (dfTablon.col("EDAD_PERSONA") <= 60) &&
  (dfTablon.col("SALARIO_PERSONA") >= 3500) &&
  (dfTablon.col("SALARIO_PERSONA") <= 10000)
)

//Mostramos los datos
dfReporte3.show()

dfReporte3:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:integer
SALARIO_PERSONA:double
ID_EMPRESA:string
NOMBRE_EMPRESA:string
MONTO_TRANSACCION:double
FECHA_TRANSACCION:string

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|         7|          Mark|          52|         8112.0|         5|        Amazon|           3352.0|       2018-01-21|
|         7|          Mark|          52|         8112.0|         5|        Amazon|           1174.0|       2018-01-21|
|        35|        Aurora|          54|         4588.0|         5|        Amazon|            547.0|       2018-01-22|
|       100|       Cynthia|          57|         8682.0|         5|        Amazon|           3148.0|       2018-01-23|
|        30|       Clayton|          52|         9505.0|         5|        Amazon|            567.0|       2018-01-22|
|        30|       Clayton|          52|         9505.0|         5|        Amazon|           2389.0|       2018-01-23|
|        79|        Philip|          51|         3919.0|         5|        Amazon|           3909.0|       2018-01-22|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           1327.0|       2018-01-23|
|         7|          Mark|          52|         8112.0|         5|        Amazon|           2071.0|       2018-01-22|
|        74|       Kaitlin|          56|         6515.0|         5|        Amazon|           4220.0|       2018-01-23|
|         7|          Mark|          52|         8112.0|         5|        Amazon|           3127.0|       2018-01-21|
|        23|        Samson|          51|         8099.0|         5|        Amazon|           1223.0|       2018-01-22|
|        30|       Clayton|          52|         9505.0|         5|        Amazon|           3234.0|       2018-01-22|
|        74|       Kaitlin|          56|         6515.0|         5|        Amazon|           1135.0|       2018-01-21|
|        30|       Clayton|          52|         9505.0|         5|        Amazon|           2225.0|       2018-01-23|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           3874.0|       2018-01-22|
|         7|          Mark|          52|         8112.0|         5|        Amazon|            546.0|       2018-01-23|
|       100|       Cynthia|          57|         8682.0|         5|        Amazon|           1499.0|       2018-01-22|
|        35|        Aurora|          54|         4588.0|         5|        Amazon|           2497.0|       2018-01-23|
|        23|        Samson|          51|         8099.0|         5|        Amazon|           1048.0|       2018-01-23|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
only showing top 20 rows


// DBTITLE 1,6. Almacenamiento
//Almacenamos el REPORTE 1
dfReporte1.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", ",").save("dbfs:///FileStore/_spark/output/REPORTE_1")


//Almacenamos el REPORTE 2
dfReporte2.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", ",").save("dbfs:///FileStore/_spark/output/REPORTE_2")


//Almacenamos el REPORTE 3
dfReporte3.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", ",").save("dbfs:///FileStore/_spark/output/REPORTE_3")


// DBTITLE 1,7. Verificacion
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

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           1518.0|       2018-01-23|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           3406.0|       2018-01-23|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           3211.0|       2018-01-23|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           1292.0|       2018-01-23|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           1146.0|       2018-01-23|
|        18|          Owen|          34|         4759.0|         5|        Amazon|            743.0|       2018-01-21|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           2136.0|       2018-01-21|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           1767.0|       2018-01-21|
|        18|          Owen|          34|         4759.0|         5|        Amazon|            850.0|       2018-01-22|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           4148.0|       2018-01-23|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           4308.0|       2018-01-23|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           1699.0|       2018-01-23|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           1320.0|       2018-01-22|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           3771.0|       2018-01-23|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           2242.0|       2018-01-23|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           2513.0|       2018-01-23|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           1281.0|       2018-01-22|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           1482.0|       2018-01-21|
|        21|       Carissa|          31|         1952.0|         5|        Amazon|           3498.0|       2018-01-22|
|        18|          Owen|          34|         4759.0|         5|        Amazon|           1614.0|       2018-01-22|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
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

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|        83|       Giselle|          45|         2503.0|         5|        Amazon|           2079.0|       2018-01-21|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1971.0|       2018-01-23|
|        51|         Damon|          49|         2669.0|         5|        Amazon|           1149.0|       2018-01-22|
|        83|       Giselle|          45|         2503.0|         5|        Amazon|           1890.0|       2018-01-23|
|        11|       Melyssa|          48|         4913.0|         5|        Amazon|           3111.0|       2018-01-21|
|        42|         Wanda|          42|         5419.0|         5|        Amazon|           2772.0|       2018-01-23|
|        83|       Giselle|          45|         2503.0|         5|        Amazon|           3487.0|       2018-01-21|
|        42|         Wanda|          42|         5419.0|         5|        Amazon|           2044.0|       2018-01-22|
|        11|       Melyssa|          48|         4913.0|         5|        Amazon|           2629.0|       2018-01-21|
|        11|       Melyssa|          48|         4913.0|         5|        Amazon|           1789.0|       2018-01-23|
|        42|         Wanda|          42|         5419.0|         5|        Amazon|           1529.0|       2018-01-22|
|        83|       Giselle|          45|         2503.0|         5|        Amazon|           2397.0|       2018-01-23|
|        11|       Melyssa|          48|         4913.0|         5|        Amazon|           2329.0|       2018-01-23|
|        83|       Giselle|          45|         2503.0|         5|        Amazon|           3131.0|       2018-01-23|
|        83|       Giselle|          45|         2503.0|         5|        Amazon|           3472.0|       2018-01-23|
|        42|         Wanda|          42|         5419.0|         5|        Amazon|           1602.0|       2018-01-22|
|        11|       Melyssa|          48|         4913.0|         5|        Amazon|            727.0|       2018-01-21|
|        42|         Wanda|          42|         5419.0|         5|        Amazon|           3706.0|       2018-01-22|
|        11|       Melyssa|          48|         4913.0|         5|        Amazon|            881.0|       2018-01-21|
|        42|         Wanda|          42|         5419.0|         5|        Amazon|            991.0|       2018-01-23|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
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

+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|SALARIO_PERSONA|ID_EMPRESA|NOMBRE_EMPRESA|MONTO_TRANSACCION|FECHA_TRANSACCION|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
|         7|          Mark|          52|         8112.0|         5|        Amazon|           3352.0|       2018-01-21|
|         7|          Mark|          52|         8112.0|         5|        Amazon|           1174.0|       2018-01-21|
|        35|        Aurora|          54|         4588.0|         5|        Amazon|            547.0|       2018-01-22|
|       100|       Cynthia|          57|         8682.0|         5|        Amazon|           3148.0|       2018-01-23|
|        30|       Clayton|          52|         9505.0|         5|        Amazon|            567.0|       2018-01-22|
|        30|       Clayton|          52|         9505.0|         5|        Amazon|           2389.0|       2018-01-23|
|        79|        Philip|          51|         3919.0|         5|        Amazon|           3909.0|       2018-01-22|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           1327.0|       2018-01-23|
|         7|          Mark|          52|         8112.0|         5|        Amazon|           2071.0|       2018-01-22|
|        74|       Kaitlin|          56|         6515.0|         5|        Amazon|           4220.0|       2018-01-23|
|         7|          Mark|          52|         8112.0|         5|        Amazon|           3127.0|       2018-01-21|
|        23|        Samson|          51|         8099.0|         5|        Amazon|           1223.0|       2018-01-22|
|        30|       Clayton|          52|         9505.0|         5|        Amazon|           3234.0|       2018-01-22|
|        74|       Kaitlin|          56|         6515.0|         5|        Amazon|           1135.0|       2018-01-21|
|        30|       Clayton|          52|         9505.0|         5|        Amazon|           2225.0|       2018-01-23|
|        38|          Irma|          58|         6747.0|         5|        Amazon|           3874.0|       2018-01-22|
|         7|          Mark|          52|         8112.0|         5|        Amazon|            546.0|       2018-01-23|
|       100|       Cynthia|          57|         8682.0|         5|        Amazon|           1499.0|       2018-01-22|
|        35|        Aurora|          54|         4588.0|         5|        Amazon|           2497.0|       2018-01-23|
|        23|        Samson|          51|         8099.0|         5|        Amazon|           1048.0|       2018-01-23|
+----------+--------------+------------+---------------+----------+--------------+-----------------+-----------------+
only showing top 20 rows