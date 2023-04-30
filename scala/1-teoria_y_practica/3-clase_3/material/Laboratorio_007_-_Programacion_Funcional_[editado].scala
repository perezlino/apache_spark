// Databricks notebook source
//COPYRIGHT: ALFONSO PEREZ [perezlino@gmail.com]
//AUTHOR: ALFONSO PEREZ [perezlino@gmail.com]


// DBTITLE 1,1. Programación Funcional
//Para entender los primeros conceptos hemos estado usando SQL como lenguaje de programación
//SQL es un buen lenguaje para procesamiento estructurado batch, pero es muy limitado
//La programación funcional nos permite utilizar la sintaxis de un lenguaje de programación
//Nos permitirá implementar nuestras propias funciones, por eso se le llama "Programación Funcional"


// DBTITLE 1,2. Importamos las librerías
//Objetos para definir la metadata
import org.apache.spark.sql.types.{StructType, StructField}

//Importamos los tipos de datos que usaremos
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}

//Podemos importar todos los utilitarios con la siguiente sentencia
import org.apache.spark.sql.types._


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


// DBTITLE 1,4. Transformations y actions
//Vamos a empezar a procesar los dataframes con SCALA
//Existen dos tipos de funciones para procesar, los transformations y los actions
//Los transformations permiten definir las operaciones que queremos realizar, pero no las ejecutan
//Los actions ejecutan las operaciones que hemos definido


// DBTITLE 1,5. Transformation "select"
//Por ejemplo, vamos a crear un nuevo dataframe con algunos campos del dataframe "dfPersona"

//EQUIVALENTE SQL: SELECT ID, NOMBRE, EDAD FROM dfPersona
var df1 = dfPersona.select("ID", "NOMBRE", "EDAD")

//Vemos los datos
//Para verlos, llamamos al "action" show
//Más adelante detallaremos los "actions", por ahora sólo los usaremos para ver los datos
df1.show(truncate=false)

df1:org.apache.spark.sql.DataFrame
ID:string
NOMBRE:string
EDAD:integer

+---+---------+----+
|ID |NOMBRE   |EDAD|
+---+---------+----+
|1  |Carl     |32  |
|2  |Priscilla|34  |
|3  |Jocelyn  |27  |
|4  |Aidan    |29  |
|5  |Leandra  |41  |
|6  |Bert     |70  |
|7  |Mark     |52  |
|8  |Jonah    |23  |
|9  |Hanae    |69  |
|10 |Cadman   |19  |
|11 |Melyssa  |48  |
|12 |Tanner   |24  |
|13 |Trevor   |34  |
|14 |Allen    |59  |
|15 |Wanda    |27  |
|16 |Alden    |26  |
|17 |Omar     |60  |
|18 |Owen     |34  |
|19 |Laura    |70  |
|20 |Emery    |24  |
+---+---------+----+
only showing top 20 rows


//Aunque el código anterior funciona, se recomienda SIEMPRE llamar a los campos incluyendo el nombre del dataframe de la 
//siguiente manera


//EQUIVALENTE SQL: SELECT ID, NOMBRE, EDAD FROM dfPersona
//Lo hacemos así porque en el futuro procesaremos varios dataframes a la vez y para tener trazabilidad del código debemos saber de qué dataframe proviene la columna
df1 = dfPersona.select(
  dfPersona.col("ID"), 
  dfPersona.col("NOMBRE"), 
  dfPersona.col("EDAD")
)

//Vemos los datos
df1.show(truncate=false)

+---+---------+----+
|ID |NOMBRE   |EDAD|
+---+---------+----+
|1  |Carl     |32  |
|2  |Priscilla|34  |
|3  |Jocelyn  |27  |
|4  |Aidan    |29  |
|5  |Leandra  |41  |
|6  |Bert     |70  |
|7  |Mark     |52  |
|8  |Jonah    |23  |
|9  |Hanae    |69  |
|10 |Cadman   |19  |
|11 |Melyssa  |48  |
|12 |Tanner   |24  |
|13 |Trevor   |34  |
|14 |Allen    |59  |
|15 |Wanda    |27  |
|16 |Alden    |26  |
|17 |Omar     |60  |
|18 |Owen     |34  |
|19 |Laura    |70  |
|20 |Emery    |24  |
+---+---------+----+
only showing top 20 rows


// DBTITLE 1,6. Transformation "filter"
//OPERACION EQUIVALENTE EN SQL:
//SELECT * FROM dfPersona WHERE EDAD > 60

//Hacemos un filtro
var df2 = dfPersona.filter(dfPersona.col("EDAD") > 60)

//Mostramos los datos
df2.show(truncate=false)

df2:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
ID:string
NOMBRE:string
TELEFONO:string
CORREO:string
FECHA_INGRESO:string
EDAD:integer
SALARIO:double
ID_EMPRESA:string
+---+--------+--------------+------------------------------------+-------------+----+-------+----------+
|ID |NOMBRE  |TELEFONO      |CORREO                              |FECHA_INGRESO|EDAD|SALARIO|ID_EMPRESA|
+---+--------+--------------+------------------------------------+-------------+----+-------+----------+
|6  |Bert    |797-4453      |a.felis.ullamcorper@arcu.org        |2017-04-25   |70  |7800.0 |7         |
|9  |Hanae   |935-2277      |eu@Nunc.ca                          |2003-05-25   |69  |6834.0 |3         |
|19 |Laura   |1-974-623-2057|mollis@ornare.ca                    |2017-03-09   |70  |17403.0|4         |
|32 |Gisela  |406-8031      |Praesent.luctus@dui.co.uk           |2002-08-21   |67  |6497.0 |1         |
|39 |Carolyn |846-7060      |metus.Aenean.sed@odiosempercursus.ca|2013-05-29   |64  |22838.0|6         |
|40 |Ross    |387-0945      |amet.faucibus@ipsum.edu             |2009-07-27   |67  |14285.0|3         |
|43 |Yetta   |986-0220      |vitae@dapibusrutrumjusto.co.uk      |2008-03-24   |61  |21452.0|2         |
|49 |Logan   |706-7700      |neque@sedpede.net                   |2008-03-07   |63  |4963.0 |7         |
|55 |Jennifer|755-6162      |id@ultrices.com                     |2011-10-19   |64  |19013.0|9         |
|63 |Sade    |112-5494      |In@utquam.com                       |2012-04-05   |70  |11112.0|6         |
|82 |Cally   |1-658-890-5167|id@dolor.edu                        |2008-12-13   |67  |24575.0|10        |
+---+--------+--------------+------------------------------------+-------------+----+-------+----------+


//Hacemos un filtro con un "and"
//SELECT * FROM dfPersona WHERE EDAD > 60 AND SALARIO > 20000
var df3 = dfPersona.filter(
  (dfPersona.col("EDAD") > 60) &&
  (dfPersona.col("SALARIO") > 20000)
)

//Mostramos los datos
df3.show(truncate=false)

df3:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
ID:string
NOMBRE:string
TELEFONO:string
CORREO:string
FECHA_INGRESO:string
EDAD:integer
SALARIO:double
ID_EMPRESA:string
+---+-------+--------------+------------------------------------+-------------+----+-------+----------+
|ID |NOMBRE |TELEFONO      |CORREO                              |FECHA_INGRESO|EDAD|SALARIO|ID_EMPRESA|
+---+-------+--------------+------------------------------------+-------------+----+-------+----------+
|39 |Carolyn|846-7060      |metus.Aenean.sed@odiosempercursus.ca|2013-05-29   |64  |22838.0|6         |
|43 |Yetta  |986-0220      |vitae@dapibusrutrumjusto.co.uk      |2008-03-24   |61  |21452.0|2         |
|82 |Cally  |1-658-890-5167|id@dolor.edu                        |2008-12-13   |67  |24575.0|10        |
+---+-------+--------------+------------------------------------+-------------+----+-------+----------+


//Hacemos un filtro con un "or"
//SELECT * FROM dfPersona WHERE EDAD > 60 OR EDAD < 20
var df4 = dfPersona.filter(
  (dfPersona.col("EDAD") > 60) || 
  (dfPersona.col("EDAD") < 20)
)

//Mostramos los datos
df4.show(truncate=false)

df4:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
ID:string
NOMBRE:string
TELEFONO:string
CORREO:string
FECHA_INGRESO:string
EDAD:integer
SALARIO:double
ID_EMPRESA:string

+---+--------+--------------+----------------------------------------------+-------------+----+-------+----------+
|ID |NOMBRE  |TELEFONO      |CORREO                                        |FECHA_INGRESO|EDAD|SALARIO|ID_EMPRESA|
+---+--------+--------------+----------------------------------------------+-------------+----+-------+----------+
|6  |Bert    |797-4453      |a.felis.ullamcorper@arcu.org                  |2017-04-25   |70  |7800.0 |7         |
|9  |Hanae   |935-2277      |eu@Nunc.ca                                    |2003-05-25   |69  |6834.0 |3         |
|10 |Cadman  |1-866-561-2701|orci.adipiscing.non@semperNam.ca              |2001-05-19   |19  |7996.0 |7         |
|19 |Laura   |1-974-623-2057|mollis@ornare.ca                              |2017-03-09   |70  |17403.0|4         |
|32 |Gisela  |406-8031      |Praesent.luctus@dui.co.uk                     |2002-08-21   |67  |6497.0 |1         |
|39 |Carolyn |846-7060      |metus.Aenean.sed@odiosempercursus.ca          |2013-05-29   |64  |22838.0|6         |
|40 |Ross    |387-0945      |amet.faucibus@ipsum.edu                       |2009-07-27   |67  |14285.0|3         |
|43 |Yetta   |986-0220      |vitae@dapibusrutrumjusto.co.uk                |2008-03-24   |61  |21452.0|2         |
|48 |Illiana |572-3648      |montes.nascetur.ridiculus@hendreritneque.co.uk|2017-07-25   |18  |1454.0 |8         |
|49 |Logan   |706-7700      |neque@sedpede.net                             |2008-03-07   |63  |4963.0 |7         |
|55 |Jennifer|755-6162      |id@ultrices.com                               |2011-10-19   |64  |19013.0|9         |
|56 |Colby   |1-517-827-8614|nec.ligula@euneque.ca                         |2014-03-05   |19  |15496.0|7         |
|63 |Sade    |112-5494      |In@utquam.com                                 |2012-04-05   |70  |11112.0|6         |
|81 |Joy     |1-379-384-0646|mi.Aliquam@nislNulla.org                      |2015-11-15   |19  |1256.0 |2         |
|82 |Cally   |1-658-890-5167|id@dolor.edu                                  |2008-12-13   |67  |24575.0|10        |
|92 |Lesley  |973-4902      |felis.adipiscing.fringilla@Proinvelnisl.co.uk |2004-02-29   |19  |23547.0|4         |
|94 |Amir    |1-221-717-0093|dapibus.ligula@faucibuslectus.com             |2001-02-11   |18  |20980.0|4         |
+---+--------+--------------+----------------------------------------------+-------------+----+-------+----------+


// DBTITLE 1,7. Transformation "groupBy"
//OPERACION EQUIVALENTE EN SQL:
//SELECT 
//	EDAD
//	COUNT(EDAD)
//	MIN(FECHA_INGRESO)
//	SUM(SALARIO)
//	MAX(SALARIO)
//FROM
//	dfData
//GROUP BY
//	EDAD


//Notemos que queremos usar algunas funciones "estándar" como:
//
// - COUNT: Para contar registros
// - MIN: Para saber el valor mínimo de un campo
// - SUM: Para sumar todos los valores de un campo
// - MAX: Para saber el valor máximo de un campo
//
//Todas estas funciones son funciones "clásicas" que los desarrolladores usan
//SPARK ya las tiene implementadas en una librería
import org.apache.spark.sql.functions._

// COMMAND ----------

//Hacemos un GROUP BY para agrupar a las personas por su edad
//Usamos las funciones estándar que tiene SPARK
var df5 = dfPersona.groupBy(dfPersona.col("EDAD")).agg(
	count(dfPersona.col("EDAD")), 
	min(dfPersona.col("FECHA_INGRESO")), 
	sum(dfPersona.col("SALARIO")), 
	max(dfPersona.col("SALARIO"))
)

//Mostramos los datos
df5.show(truncate=false)

df5:org.apache.spark.sql.DataFrame
EDAD:integer
count(EDAD):long
min(FECHA_INGRESO):string
sum(SALARIO):double
max(SALARIO):double

+----+-----------+------------------+------------+------------+
|EDAD|count(EDAD)|min(FECHA_INGRESO)|sum(SALARIO)|max(SALARIO)|
+----+-----------+------------------+------------+------------+
|18  |2          |2001-02-11        |22434.0     |20980.0     |
|19  |4          |2001-05-19        |48295.0     |23547.0     |
|22  |5          |2001-01-06        |77911.0     |23820.0     |
|23  |2          |2005-04-28        |28578.0     |17040.0     |
|24  |4          |2000-09-18        |49314.0     |19943.0     |
|25  |2          |2005-06-22        |24288.0     |20573.0     |
|26  |3          |2006-12-05        |21039.0     |12092.0     |
|27  |5          |2002-08-01        |53425.0     |16735.0     |
|28  |1          |2010-06-10        |22037.0     |22037.0     |
|29  |2          |2000-09-15        |24943.0     |21556.0     |
|30  |2          |2003-10-19        |24642.0     |16782.0     |
|31  |4          |2007-07-24        |48089.0     |19522.0     |
|32  |2          |2004-04-23        |29029.0     |20095.0     |
|33  |3          |2000-03-17        |48967.0     |20549.0     |
|34  |5          |2002-04-09        |48144.0     |12423.0     |
|35  |2          |2000-11-23        |13151.0     |7109.0      |
|37  |1          |2003-07-18        |6191.0      |6191.0      |
|38  |1          |2005-10-21        |15116.0     |15116.0     |
|39  |1          |2008-10-18        |6483.0      |6483.0      |
|41  |4          |2002-10-10        |48309.0     |22102.0     |
+----+-----------+------------------+------------+------------+
only showing top 20 rows


//En ocasiones podemos usar funciones que vengan desde diferentes librerías
//Podemos colocar todas las funciones en una variable para saber de qué librería viene
import org.apache.spark.sql.{functions => f}


//Y usarlas haciendo referencia a la variable
var df5 = dfPersona.groupBy(dfPersona.col("EDAD")).agg(
	f.count(dfPersona.col("EDAD")), 
	f.min(dfPersona.col("FECHA_INGRESO")), 
	f.sum(dfPersona.col("SALARIO")), 
	f.max(dfPersona.col("SALARIO"))
)

//Mostramos los datos
df5.show(truncate=false)

df5:org.apache.spark.sql.DataFrame
EDAD:integer
count(EDAD):long
min(FECHA_INGRESO):string
sum(SALARIO):double
max(SALARIO):double

+----+-----------+------------------+------------+------------+
|EDAD|count(EDAD)|min(FECHA_INGRESO)|sum(SALARIO)|max(SALARIO)|
+----+-----------+------------------+------------+------------+
|  18|          2|        2001-02-11|     22434.0|     20980.0|
|  19|          4|        2001-05-19|     48295.0|     23547.0|
|  22|          5|        2001-01-06|     77911.0|     23820.0|
|  23|          2|        2005-04-28|     28578.0|     17040.0|
|  24|          4|        2000-09-18|     49314.0|     19943.0|
|  25|          2|        2005-06-22|     24288.0|     20573.0|
|  26|          3|        2006-12-05|     21039.0|     12092.0|
|  27|          5|        2002-08-01|     53425.0|     16735.0|
|  28|          1|        2010-06-10|     22037.0|     22037.0|
|  29|          2|        2000-09-15|     24943.0|     21556.0|
|  30|          2|        2003-10-19|     24642.0|     16782.0|
|  31|          4|        2007-07-24|     48089.0|     19522.0|
|  32|          2|        2004-04-23|     29029.0|     20095.0|
|  33|          3|        2000-03-17|     48967.0|     20549.0|
|  34|          5|        2002-04-09|     48144.0|     12423.0|
|  35|          2|        2000-11-23|     13151.0|      7109.0|
|  37|          1|        2003-07-18|      6191.0|      6191.0|
|  38|          1|        2005-10-21|     15116.0|     15116.0|
|  39|          1|        2008-10-18|      6483.0|      6483.0|
|  41|          4|        2002-10-10|     48309.0|     22102.0|
+----+-----------+------------------+------------+------------+
only showing top 20 rows


//Revisemos el esquema, notamos que las columnas reciben nombre "extraños"
df5.printSchema()

root
 |-- EDAD: integer (nullable = true)
 |-- count(EDAD): long (nullable = false)
 |-- min(FECHA_INGRESO): string (nullable = true)
 |-- sum(SALARIO): double (nullable = true)
 |-- max(SALARIO): double (nullable = true)


//Colocaremos un alias a las columnas
var df6 = dfPersona.groupBy(dfPersona.col("EDAD")).agg(
	f.count(dfPersona.col("EDAD")).alias("CANTIDAD"), 
	f.min(dfPersona.col("FECHA_INGRESO")).alias("FECHA_CONTRATO_MAS_RECIENTE"), 
	f.sum(dfPersona.col("SALARIO")).alias("SUMA_SALARIOS"), 
	f.max(dfPersona.col("SALARIO")).alias("SALARIO_MAYOR")
)

//Mostramos los datos
df6.show()

df6:org.apache.spark.sql.DataFrame
EDAD:integer
CANTIDAD:long
FECHA_CONTRATO_MAS_RECIENTE:string
SUMA_SALARIOS:double
SALARIO_MAYOR:double

+----+--------+---------------------------+-------------+-------------+
|EDAD|CANTIDAD|FECHA_CONTRATO_MAS_RECIENTE|SUMA_SALARIOS|SALARIO_MAYOR|
+----+--------+---------------------------+-------------+-------------+
|  18|       2|                 2001-02-11|      22434.0|      20980.0|
|  19|       4|                 2001-05-19|      48295.0|      23547.0|
|  22|       5|                 2001-01-06|      77911.0|      23820.0|
|  23|       2|                 2005-04-28|      28578.0|      17040.0|
|  24|       4|                 2000-09-18|      49314.0|      19943.0|
|  25|       2|                 2005-06-22|      24288.0|      20573.0|
|  26|       3|                 2006-12-05|      21039.0|      12092.0|
|  27|       5|                 2002-08-01|      53425.0|      16735.0|
|  28|       1|                 2010-06-10|      22037.0|      22037.0|
|  29|       2|                 2000-09-15|      24943.0|      21556.0|
|  30|       2|                 2003-10-19|      24642.0|      16782.0|
|  31|       4|                 2007-07-24|      48089.0|      19522.0|
|  32|       2|                 2004-04-23|      29029.0|      20095.0|
|  33|       3|                 2000-03-17|      48967.0|      20549.0|
|  34|       5|                 2002-04-09|      48144.0|      12423.0|
|  35|       2|                 2000-11-23|      13151.0|       7109.0|
|  37|       1|                 2003-07-18|       6191.0|       6191.0|
|  38|       1|                 2005-10-21|      15116.0|      15116.0|
|  39|       1|                 2008-10-18|       6483.0|       6483.0|
|  41|       4|                 2002-10-10|      48309.0|      22102.0|
+----+--------+---------------------------+-------------+-------------+
only showing top 20 rows


//Revisamos el esquema
df6.printSchema()

root
 |-- EDAD: integer (nullable = true)
 |-- CANTIDAD: long (nullable = false)
 |-- FECHA_CONTRATO_MAS_RECIENTE: string (nullable = true)
 |-- SUMA_SALARIOS: double (nullable = true)
 |-- SALARIO_MAYOR: double (nullable = true)


// DBTITLE 1,8. Transformation "JOIN"
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

dfTransaccion:org.apache.spark.sql.DataFrame
ID_PERSONA:string
ID_EMPRESA:string
MONTO:double
FECHA:string

+----------+----------+------+----------+
|ID_PERSONA|ID_EMPRESA| MONTO|     FECHA|
+----------+----------+------+----------+
|        26|         7|2244.0|2021-01-23|
|        24|         6|1745.0|2021-01-23|
|         1|        10| 238.0|2021-01-23|
|        19|         2|3338.0|2021-01-23|
|        95|         8|2656.0|2021-01-23|
|        37|         6|3811.0|2021-01-23|
|        40|         2| 297.0|2021-01-23|
|        27|         3|2896.0|2021-01-23|
|        65|         2|4097.0|2021-01-23|
|        21|         8|2799.0|2021-01-23|
|        71|         8|3148.0|2021-01-23|
|        71|         4|2712.0|2021-01-23|
|        34|         6| 513.0|2021-01-23|
|        71|         5|1548.0|2021-01-23|
|        81|         3|1343.0|2021-01-23|
|        90|         1|4224.0|2021-01-23|
|        87|         8|1571.0|2021-01-23|
|        82|         4|3411.0|2021-01-23|
|        61|         4| 871.0|2021-01-23|
|        48|         6| 783.0|2021-01-23|
+----------+----------+------+----------+
only showing top 20 rows


//Implementacion del JOIN
var dfJoin = dfTransaccion.join(
  dfPersona, 
  dfTransaccion.col("ID_PERSONA") === dfPersona.col("ID"),
  "inner"
).select(
  dfTransaccion.col("ID_PERSONA"),
  dfPersona.col("NOMBRE"),
  dfPersona.col("EDAD"),
  dfPersona.col("SALARIO"),
  dfPersona.col("ID_EMPRESA").alias("ID_EMPRESA_TRABAJO"),
  dfTransaccion.col("ID_EMPRESA").alias("ID_EMPRESA_TRANSACCION"),
  dfTransaccion.col("MONTO"),
  dfTransaccion.col("FECHA")
)

//Mostramos los datos
dfJoin.show()

dfJoin:org.apache.spark.sql.DataFrame
ID_PERSONA:string
NOMBRE:string
EDAD:integer
SALARIO:double
ID_EMPRESA_TRABAJO:string
ID_EMPRESA_TRANSACCION:string
MONTO:double
FECHA:string

+----------+---------+----+-------+------------------+----------------------+------+----------+
|ID_PERSONA|   NOMBRE|EDAD|SALARIO|ID_EMPRESA_TRABAJO|ID_EMPRESA_TRANSACCION| MONTO|     FECHA|
+----------+---------+----+-------+------------------+----------------------+------+----------+
|        26|  Brenden|  33|20549.0|                 7|                     7|2244.0|2021-01-23|
|        24|    Amaya|  24| 1801.0|                 8|                     6|1745.0|2021-01-23|
|         1|     Carl|  32|20095.0|                 5|                    10| 238.0|2021-01-23|
|        19|    Laura|  70|17403.0|                 4|                     2|3338.0|2021-01-23|
|        95|    Jayme|  58|23975.0|                 4|                     8|2656.0|2021-01-23|
|        37|     Inga|  41| 8562.0|                 3|                     6|3811.0|2021-01-23|
|        40|     Ross|  67|14285.0|                 3|                     2| 297.0|2021-01-23|
|        27|Alexander|  55|13813.0|                 6|                     3|2896.0|2021-01-23|
|        65|    Nehru|  34|12423.0|                 4|                     2|4097.0|2021-01-23|
|        21|  Carissa|  31| 1952.0|                10|                     8|2799.0|2021-01-23|
|        71|    Doris|  23|11538.0|                 6|                     8|3148.0|2021-01-23|
|        71|    Doris|  23|11538.0|                 6|                     4|2712.0|2021-01-23|
|        34|     Lila|  48|24305.0|                 4|                     6| 513.0|2021-01-23|
|        71|    Doris|  23|11538.0|                 6|                     5|1548.0|2021-01-23|
|        81|      Joy|  19| 1256.0|                 2|                     3|1343.0|2021-01-23|
|        90|Sigourney|  42| 9145.0|                 6|                     1|4224.0|2021-01-23|
|        87|    Karly|  25| 3715.0|                 1|                     8|1571.0|2021-01-23|
|        82|    Cally|  67|24575.0|                10|                     4|3411.0|2021-01-23|
|        61|     Abel|  33|15070.0|                 9|                     4| 871.0|2021-01-23|
|        48|  Illiana|  18| 1454.0|                 8|                     6| 783.0|2021-01-23|
+----------+---------+----+-------+------------------+----------------------+------+----------+
only showing top 20 rows


// DBTITLE 1,6. Actions "show" y "save"
//Generalmente con los dataframes queremos hacer dos cosas, verlos o almacenarlos
//Estos son "actions" que ejecutan la cadena de procesos


//Al ejecutar "df1", la cadena de procesos asociada se ejecuta y se carga en memoria RAM
df1.show()

+---+---------+----+
| ID|   NOMBRE|EDAD|
+---+---------+----+
|  1|     Carl|  32|
|  2|Priscilla|  34|
|  3|  Jocelyn|  27|
|  4|    Aidan|  29|
|  5|  Leandra|  41|
|  6|     Bert|  70|
|  7|     Mark|  52|
|  8|    Jonah|  23|
|  9|    Hanae|  69|
| 10|   Cadman|  19|
| 11|  Melyssa|  48|
| 12|   Tanner|  24|
| 13|   Trevor|  34|
| 14|    Allen|  59|
| 15|    Wanda|  27|
| 16|    Alden|  26|
| 17|     Omar|  60|
| 18|     Owen|  34|
| 19|    Laura|  70|
| 20|    Emery|  24|
+---+---------+----+
only showing top 20 rows


//Si volvemos a llamar al "action", la cadena de procesos se ejecuta nuevamente
df1.show()

+---+---------+----+
| ID|   NOMBRE|EDAD|
+---+---------+----+
|  1|     Carl|  32|
|  2|Priscilla|  34|
|  3|  Jocelyn|  27|
|  4|    Aidan|  29|
|  5|  Leandra|  41|
|  6|     Bert|  70|
|  7|     Mark|  52|
|  8|    Jonah|  23|
|  9|    Hanae|  69|
| 10|   Cadman|  19|
| 11|  Melyssa|  48|
| 12|   Tanner|  24|
| 13|   Trevor|  34|
| 14|    Allen|  59|
| 15|    Wanda|  27|
| 16|    Alden|  26|
| 17|     Omar|  60|
| 18|     Owen|  34|
| 19|    Laura|  70|
| 20|    Emery|  24|
+---+---------+----+
only showing top 20 rows