// Databricks notebook source
//COPYRIGHT: ALFONSO PEREZ [perezlino@gmail.com]
//AUTHOR: ALFONSO PEREZ [perezlino@gmail.com]

// DBTITLE 1,1. Importamos las librerías
//Objetos para definir la metadata
import org.apache.spark.sql.types.{StructType, StructField}

//Importamos los tipos de datos que usaremos
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}

//Podemos importar todos los utilitarios con la siguiente sentencia
import org.apache.spark.sql.types._

//Importamos todos los objetos utilitarios dentro de una variable
import org.apache.spark.sql.{functions => f}


// DBTITLE 1,2. Lectura de datos
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
dfPersona.show(truncate=false)

dfPersona:org.apache.spark.sql.DataFrame
ID:string
NOMBRE:string
TELEFONO:string
CORREO:string
FECHA_INGRESO:string
EDAD:integer
SALARIO:double
ID_EMPRESA:string

+---+---------+--------------+---------------------------------------+-------------+----+-------+----------+
|ID |NOMBRE   |TELEFONO      |CORREO                                 |FECHA_INGRESO|EDAD|SALARIO|ID_EMPRESA|
+---+---------+--------------+---------------------------------------+-------------+----+-------+----------+
|1  |Carl     |1-745-633-9145|arcu.Sed.et@ante.co.uk                 |2004-04-23   |32  |20095.0|5         |
|2  |Priscilla|155-2498      |Donec.egestas.Aliquam@volutpatnunc.edu |2019-02-17   |34  |9298.0 |2         |
|3  |Jocelyn  |1-204-956-8594|amet.diam@lobortis.co.uk               |2002-08-01   |27  |10853.0|3         |
|4  |Aidan    |1-719-862-9385|euismod.et.commodo@nibhlaciniaorci.edu |2018-11-06   |29  |3387.0 |10        |
|5  |Leandra  |839-8044      |at@pretiumetrutrum.com                 |2002-10-10   |41  |22102.0|1         |
|6  |Bert     |797-4453      |a.felis.ullamcorper@arcu.org           |2017-04-25   |70  |7800.0 |7         |
|7  |Mark     |1-680-102-6792|Quisque.ac@placerat.ca                 |2006-04-21   |52  |8112.0 |5         |
|8  |Jonah    |214-2975      |eu.ultrices.sit@vitae.ca               |2017-10-07   |23  |17040.0|5         |
|9  |Hanae    |935-2277      |eu@Nunc.ca                             |2003-05-25   |69  |6834.0 |3         |
|10 |Cadman   |1-866-561-2701|orci.adipiscing.non@semperNam.ca       |2001-05-19   |19  |7996.0 |7         |
|11 |Melyssa  |596-7736      |vel@vulputateposuerevulputate.net      |2008-10-14   |48  |4913.0 |8         |
|12 |Tanner   |1-739-776-7897|arcu.Aliquam.ultrices@sociis.com       |2011-05-10   |24  |19943.0|8         |
|13 |Trevor   |512-1955      |Nunc.quis.arcu@egestasa.org            |2010-08-06   |34  |9501.0 |5         |
|14 |Allen    |733-2795      |felis.Donec@necleo.org                 |2005-03-07   |59  |16289.0|2         |
|15 |Wanda    |359-6973      |Nam.nulla.magna@In.org                 |2005-08-21   |27  |1539.0 |5         |
|16 |Alden    |341-8522      |odio@morbitristiquesenectus.ca         |2006-12-05   |26  |3377.0 |2         |
|17 |Omar     |720-1543      |Phasellus.vitae.mauris@sollicitudin.net|2014-06-24   |60  |6851.0 |6         |
|18 |Owen     |1-167-335-7541|sociis@erat.com                        |2002-04-09   |34  |4759.0 |7         |
|19 |Laura    |1-974-623-2057|mollis@ornare.ca                       |2017-03-09   |70  |17403.0|4         |
|20 |Emery    |1-672-840-0264|at.nisi@vel.org                        |2004-02-27   |24  |18752.0|9         |
+---+---------+--------------+---------------------------------------+-------------+----+-------+----------+
only showing top 20 rows


// DBTITLE 1,3. Procesamiento
//Definimos los pasos de procesamiento en una única cadena de proceso
var dfResultado = dfPersona.groupBy(dfPersona.col("EDAD")).agg(
	f.count(dfPersona.col("EDAD")).alias("CANTIDAD"), 
	f.min(dfPersona.col("FECHA_INGRESO")).alias("FECHA_CONTRATO_MAS_RECIENTE"), 
	f.sum(dfPersona.col("SALARIO")).alias("SUMA_SALARIOS"), 
	f.max(dfPersona.col("SALARIO")).alias("SALARIO_MAYOR")
).alias("P1").
filter(f.col("P1.EDAD") > 35).alias("P2").
filter(f.col("P2.SUMA_SALARIOS") > 20000).alias("P3").
filter(f.col("P3.SALARIO_MAYOR") > 1000)

//Visualizamos los datos
dfResultado.show()

dfResultado:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
EDAD:integer
CANTIDAD:long
FECHA_CONTRATO_MAS_RECIENTE:string
SUMA_SALARIOS:double
SALARIO_MAYOR:double

+----+--------+---------------------------+-------------+-------------+
|EDAD|CANTIDAD|FECHA_CONTRATO_MAS_RECIENTE|SUMA_SALARIOS|SALARIO_MAYOR|
+----+--------+---------------------------+-------------+-------------+
|  41|       4|                 2002-10-10|      48309.0|      22102.0|
|  42|       5|                 2002-05-02|      62417.0|      22038.0|
|  46|       2|                 2000-04-03|      32820.0|      22953.0|
|  47|       2|                 2001-09-17|      35036.0|      21591.0|
|  48|       2|                 2008-10-14|      29218.0|      24305.0|
|  52|       3|                 2006-04-21|      32373.0|      14756.0|
|  55|       2|                 2000-08-16|      23923.0|      13813.0|
|  58|       3|                 2002-05-31|      45195.0|      23975.0|
|  61|       1|                 2008-03-24|      21452.0|      21452.0|
|  64|       2|                 2011-10-19|      41851.0|      22838.0|
|  67|       3|                 2002-08-21|      45357.0|      24575.0|
|  70|       3|                 2012-04-05|      36315.0|      17403.0|
+----+--------+---------------------------+-------------+-------------+


// DBTITLE 1,4. Almacenamiento en TEXTFILE
//Anteriormente habíamos almacenado un dataframe en un archivo de CSV, es decir delimitado
dfResultado.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", "|").save("dbfs:///FileStore/_spark/output/dfResultado")


//Verificamos la ruta para ver si el archivo se escribió, veremos el archivo CSV

%fs ls dbfs:///FileStore/_spark/output/dfResultado

_SUCCESS
_committed_1559419047534218211
_started_1559419047534218211
part-00000-tid-1559419047534218211-7be6b1b8-3136-45ef-a8b1-4c3315135652-32-1-c000.csv


//Para verificar, leemos el directorio del dataframe en una variable
var dfResultadoLeido = spark.read.format("csv").option("header", "true").option("delimiter", "|").load("dbfs:///FileStore/_spark/output/dfResultado")

//Mostramos los datos
dfResultadoLeido.show()

dfResultadoLeido:org.apache.spark.sql.DataFrame
EDAD:string
CANTIDAD:string
FECHA_CONTRATO_MAS_RECIENTE:string
SUMA_SALARIOS:string
SALARIO_MAYOR:string

+----+--------+---------------------------+-------------+-------------+
|EDAD|CANTIDAD|FECHA_CONTRATO_MAS_RECIENTE|SUMA_SALARIOS|SALARIO_MAYOR|
+----+--------+---------------------------+-------------+-------------+
|  41|       4|                 2002-10-10|      48309.0|      22102.0|
|  42|       5|                 2002-05-02|      62417.0|      22038.0|
|  46|       2|                 2000-04-03|      32820.0|      22953.0|
|  47|       2|                 2001-09-17|      35036.0|      21591.0|
|  48|       2|                 2008-10-14|      29218.0|      24305.0|
|  52|       3|                 2006-04-21|      32373.0|      14756.0|
|  55|       2|                 2000-08-16|      23923.0|      13813.0|
|  58|       3|                 2002-05-31|      45195.0|      23975.0|
|  61|       1|                 2008-03-24|      21452.0|      21452.0|
|  64|       2|                 2011-10-19|      41851.0|      22838.0|
|  67|       3|                 2002-08-21|      45357.0|      24575.0|
|  70|       3|                 2012-04-05|      36315.0|      17403.0|
+----+--------+---------------------------+-------------+-------------+


//Si consultamos el esquema asociado, el archivo habrá escrito todos los campos como "STRING"
dfResultadoLeido.printSchema()

root
 |-- EDAD: string (nullable = true)
 |-- CANTIDAD: string (nullable = true)
 |-- FECHA_CONTRATO_MAS_RECIENTE: string (nullable = true)
 |-- SUMA_SALARIOS: string (nullable = true)
 |-- SALARIO_MAYOR: string (nullable = true)


//Estos archivos delimitados se conocen como archivos TEXTFILE (texto plano)
//Generalmente cuando queramos procesar algo, el archivo o los archivos a procesar nos los dejarán como TEXTFILE
//Cargaremos los TEXTFILE a DATAFRAMES y comenzaremos a procesarlos como lo hemos estado haciendo
//Cuando termina el procesamiento, el DATAFRAME resultante lo podemos escribir en un archivo TEXTFILE
//Gracias a esto, podemos descargar el archivo TEXTFILE y compartirselo a alguien para que lo cargue a un excel o una tabla clásica de base de datos
//También ese archivo TEXTFILE resultante puede servir para ser analizado por otra herramienta de Big Data orientada a procesar datos en Disco Duro como Hive o Impala
//El problema de los TEXTFILE es que son extremadamente lentos, si una herramienta de Big Data que ejecuta parte de su proceso en disco duro trata de procesar un archivo TEXTFILE, no podrá procesarlo rápidamente
//Es mejor usar formatos de archivo orientados a Big Data, como por ejemplo PARQUET y AVRO


// DBTITLE 1,5. Almacenamiento en PARQUET
//Escribiremos el mismo dataframe, pero en formato PARQUET
//Lo escribiremos en otra ruta (dfResultadoParquet)
dfResultado.write.format("parquet").mode("overwrite").option("compression", "snappy").save("dbfs:///FileStore/_spark/output/dfResultadoParquet")


//Verificamos la ruta para ver si el archivo se escribió, veremos el archivo PARQUET

%fs ls dbfs:///FileStore/_spark/output/dfResultadoParquet

_SUCCESS
_committed_4278820346301987657
_started_4278820346301987657
part-00000-tid-4278820346301987657-30a2a881-3b6e-40f4-8dc6-48bea272446e-36-1-c000.snappy.parquet


//Para verificar, leemos el directorio del dataframe en una variable
var dfResultadoLeidoParquet = spark.read.format("parquet").load("dbfs:///FileStore/_spark/output/dfResultadoParquet")

//Mostramos los datos
dfResultadoLeidoParquet.show()

dfResultadoLeidoParquet:org.apache.spark.sql.DataFrame
EDAD:integer
CANTIDAD:long
FECHA_CONTRATO_MAS_RECIENTE:string
SUMA_SALARIOS:double
SALARIO_MAYOR:double
+----+--------+---------------------------+-------------+-------------+
|EDAD|CANTIDAD|FECHA_CONTRATO_MAS_RECIENTE|SUMA_SALARIOS|SALARIO_MAYOR|
+----+--------+---------------------------+-------------+-------------+
|  41|       4|                 2002-10-10|      48309.0|      22102.0|
|  42|       5|                 2002-05-02|      62417.0|      22038.0|
|  46|       2|                 2000-04-03|      32820.0|      22953.0|
|  47|       2|                 2001-09-17|      35036.0|      21591.0|
|  48|       2|                 2008-10-14|      29218.0|      24305.0|
|  52|       3|                 2006-04-21|      32373.0|      14756.0|
|  55|       2|                 2000-08-16|      23923.0|      13813.0|
|  58|       3|                 2002-05-31|      45195.0|      23975.0|
|  61|       1|                 2008-03-24|      21452.0|      21452.0|
|  64|       2|                 2011-10-19|      41851.0|      22838.0|
|  67|       3|                 2002-08-21|      45357.0|      24575.0|
|  70|       3|                 2012-04-05|      36315.0|      17403.0|
+----+--------+---------------------------+-------------+-------------+


//Si consultamos el esquema asociado, el archivo almacena metadata de los tipos de datos
dfResultadoLeidoParquet.printSchema()

root
 |-- EDAD: integer (nullable = true)
 |-- CANTIDAD: long (nullable = true)
 |-- FECHA_CONTRATO_MAS_RECIENTE: string (nullable = true)
 |-- SUMA_SALARIOS: double (nullable = true)
 |-- SALARIO_MAYOR: double (nullable = true)


//Si descargamos el archivo parquet:
//
// - RUTA_A_MI_ARCHIVO: /output/dfResultadoParquet/
// - NOMBRE_ARCHIVO: part-00000-tid-1156910903102427309-a43a026a-d609-4a35-a50e-bee584306355-45-1-c000.snappy.parquet
// - ID_CUENTA: 4458995983973520
//
//la URL sería:
//
// https://community.cloud.databricks.com/files/output/dfResultadoParquet/part-00000-tid-1156910903102427309-a43a026a-d609-4a35-a50e-bee584306355-45-1-c000.snappy.parquet?o=4458995983973520



//A diferencia de un TEXTFILE, al ser PARQUET un formato binarizado, ya no podremos ver el contenido del archivo



// DBTITLE 1,6. Almacenamiento en AVRO
//Escribiremos el mismo dataframe, pero en formato AVRO
//Lo escribiremos en otra ruta (dfResultadoAvro)
dfResultado.write.format("avro").mode("overwrite").option("compression", "snappy").save("dbfs:///FileStore/_spark/output/dfResultadoAvro")


//Verificamos la ruta para ver si el archivo se escribió, veremos el archivo AVRO

%fs ls dbfs:///FileStore/_spark/output/dfResultadoAvro

_SUCCESS
_committed_953579043164171325
_started_953579043164171325
part-00000-tid-953579043164171325-b11d8b33-b690-4093-a17f-04a8d66b19e5-40-1-c000.avro


//Para verificar, leemos el directorio del dataframe en una variable
var dfResultadoLeidoAvro = spark.read.format("avro").load("dbfs:///FileStore/_spark/output/dfResultadoAvro")

//Mostramos los datos
dfResultadoLeidoAvro.show()

dfResultadoLeidoAvro:org.apache.spark.sql.DataFrame
EDAD:integer
CANTIDAD:long
FECHA_CONTRATO_MAS_RECIENTE:string
SUMA_SALARIOS:double
SALARIO_MAYOR:double
+----+--------+---------------------------+-------------+-------------+
|EDAD|CANTIDAD|FECHA_CONTRATO_MAS_RECIENTE|SUMA_SALARIOS|SALARIO_MAYOR|
+----+--------+---------------------------+-------------+-------------+
|  41|       4|                 2002-10-10|      48309.0|      22102.0|
|  42|       5|                 2002-05-02|      62417.0|      22038.0|
|  46|       2|                 2000-04-03|      32820.0|      22953.0|
|  47|       2|                 2001-09-17|      35036.0|      21591.0|
|  48|       2|                 2008-10-14|      29218.0|      24305.0|
|  52|       3|                 2006-04-21|      32373.0|      14756.0|
|  55|       2|                 2000-08-16|      23923.0|      13813.0|
|  58|       3|                 2002-05-31|      45195.0|      23975.0|
|  61|       1|                 2008-03-24|      21452.0|      21452.0|
|  64|       2|                 2011-10-19|      41851.0|      22838.0|
|  67|       3|                 2002-08-21|      45357.0|      24575.0|
|  70|       3|                 2012-04-05|      36315.0|      17403.0|
+----+--------+---------------------------+-------------+-------------+


//Si consultamos el esquema asociado, el archivo almacena metadata de los tipos de datos
dfResultadoLeidoAvro.printSchema()

root
 |-- EDAD: integer (nullable = true)
 |-- CANTIDAD: long (nullable = true)
 |-- FECHA_CONTRATO_MAS_RECIENTE: string (nullable = true)
 |-- SUMA_SALARIOS: double (nullable = true)
 |-- SALARIO_MAYOR: double (nullable = true)


//De la misma manera que PARQUET, a diferencia de un TEXTFILE, al ser AVRO un formato binarizado, ya no podremos ver el contenido del archivo



// DBTITLE 1,7. ¿Cuándo usar PARQUET y AVRO?
//Debemos saber que:
//
// - Parquet es el formato de procesamiento más rápido
// - Avro es el formato de procesamiento más flexible
//
//Generalmente se utilizan para la construcción de tablas de datos en un Datalake, en una clase dedicada a todas las herramientas de Big Data se estudiarían a detalle
//Para el caso del procesamiento SPARK, lo usaremos para almacenar resultantes intermedias en Disco Duro y liberar memoria RAM
//Usaremos el formato más rápido: PARQUET