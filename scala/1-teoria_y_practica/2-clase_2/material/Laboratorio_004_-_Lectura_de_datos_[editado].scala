// Databricks notebook source
//COPYRIGHT: BIG DATA ACADEMY [info@bigdataacademy.org]
//AUTHOR: ALONSO MELGAREJO [alonsoraulmgs@gmail.com]

// DBTITLE 1,1. Leyendo un archivo dentro de una variable en memoria RAM
//Leemos un ARCHIVO desde DISCO DURO y lo colocamos en una DATAFRAME en memoria RAM
var dfPersona = spark.read.format("csv").option("header", "true").option("delimiter", "|").load("dbfs:///FileStore/_spark/persona.data")

dfPersona:org.apache.spark.sql.DataFrame
ID:string
NOMBRE:string
TELEFONO:string
CORREO:string
FECHA_INGRESO:string
EDAD:string
SALARIO:string
ID_EMPRESA:string
dfPersona: org.apache.spark.sql.DataFrame = [ID: string, NOMBRE: string ... 6 more fields]


//Vemos el contenido
dfPersona.show(truncate=false)

+---+---------+--------------+---------------------------------------+-------------+----+-------+----------+
|ID |NOMBRE   |TELEFONO      |CORREO                                 |FECHA_INGRESO|EDAD|SALARIO|ID_EMPRESA|
+---+---------+--------------+---------------------------------------+-------------+----+-------+----------+
|1  |Carl     |1-745-633-9145|arcu.Sed.et@ante.co.uk                 |2004-04-23   |32  |20095  |5         |
|2  |Priscilla|155-2498      |Donec.egestas.Aliquam@volutpatnunc.edu |2019-02-17   |34  |9298   |2         |
|3  |Jocelyn  |1-204-956-8594|amet.diam@lobortis.co.uk               |2002-08-01   |27  |10853  |3         |
|4  |Aidan    |1-719-862-9385|euismod.et.commodo@nibhlaciniaorci.edu |2018-11-06   |29  |3387   |10        |
|5  |Leandra  |839-8044      |at@pretiumetrutrum.com                 |2002-10-10   |41  |22102  |1         |
|6  |Bert     |797-4453      |a.felis.ullamcorper@arcu.org           |2017-04-25   |70  |7800   |7         |
|7  |Mark     |1-680-102-6792|Quisque.ac@placerat.ca                 |2006-04-21   |52  |8112   |5         |
|8  |Jonah    |214-2975      |eu.ultrices.sit@vitae.ca               |2017-10-07   |23  |17040  |5         |
|9  |Hanae    |935-2277      |eu@Nunc.ca                             |2003-05-25   |69  |6834   |3         |
|10 |Cadman   |1-866-561-2701|orci.adipiscing.non@semperNam.ca       |2001-05-19   |19  |7996   |7         |
|11 |Melyssa  |596-7736      |vel@vulputateposuerevulputate.net      |2008-10-14   |48  |4913   |8         |
|12 |Tanner   |1-739-776-7897|arcu.Aliquam.ultrices@sociis.com       |2011-05-10   |24  |19943  |8         |
|13 |Trevor   |512-1955      |Nunc.quis.arcu@egestasa.org            |2010-08-06   |34  |9501   |5         |
|14 |Allen    |733-2795      |felis.Donec@necleo.org                 |2005-03-07   |59  |16289  |2         |
|15 |Wanda    |359-6973      |Nam.nulla.magna@In.org                 |2005-08-21   |27  |1539   |5         |
|16 |Alden    |341-8522      |odio@morbitristiquesenectus.ca         |2006-12-05   |26  |3377   |2         |
|17 |Omar     |720-1543      |Phasellus.vitae.mauris@sollicitudin.net|2014-06-24   |60  |6851   |6         |
|18 |Owen     |1-167-335-7541|sociis@erat.com                        |2002-04-09   |34  |4759   |7         |
|19 |Laura    |1-974-623-2057|mollis@ornare.ca                       |2017-03-09   |70  |17403  |4         |
|20 |Emery    |1-672-840-0264|at.nisi@vel.org                        |2004-02-27   |24  |18752  |9         |
+---+---------+--------------+---------------------------------------+-------------+----+-------+----------+
only showing top 20 rows


//Databricks ofrece una función especial que nos permite visualizar de una mejor manera el contenido de un dataframe
display(dfPersona)


//IMPORTANTE: Aunque es cierto que lo imprime de una mejor manera, esta función es propia de Databricks, así que esta función 
//            no podrá ejecutarse en otros entornos de SPARK (p.e., Cloudera, Hortonworks, HDInsight [Azure], EMR [AWS], 
//            Dataproc [GCP]), solo hay que usarla si sabemos que nuestro código se ejecutará siempre en Databricks


// DBTITLE 1,2. Definición de esquema
//Veremos los campos que tiene el dataframe
dfPersona.printSchema()

root
 |-- ID: string (nullable = true)
 |-- NOMBRE: string (nullable = true)
 |-- TELEFONO: string (nullable = true)
 |-- CORREO: string (nullable = true)
 |-- FECHA_INGRESO: string (nullable = true)
 |-- EDAD: string (nullable = true)
 |-- SALARIO: string (nullable = true)
 |-- ID_EMPRESA: string (nullable = true)

//Todos los datos están como "STRING", colocaremos "EDAD" y "SALARIO" a "INTEGER" y "DOUBLE"


//Importamos los objetos "StructType" y el "StructField"
//Estos objetos nos ayudarán a definir la metadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField


//Si importamos varios objetos de un mismo paquete, podemos escribirlo así
import org.apache.spark.sql.types.{StructType, StructField}


//Importamos los tipos de datos que usaremos
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}


//Esta librería tiene otros tipos de datos
//Para importarlos todos usamos la siguiente linea
import org.apache.spark.sql.types._


//Leemos el archivo indicando el esquema
var dfPersona2 = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
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

dfPersona2:org.apache.spark.sql.DataFrame
ID:string
NOMBRE:string
TELEFONO:string
CORREO:string
FECHA_INGRESO:string
EDAD:integer
SALARIO:double
ID_EMPRESA:string
dfPersona2: org.apache.spark.sql.DataFrame = [ID: string, NOMBRE: string ... 6 more fields]


//Mostramos la data
dfPersona2.show()

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


//Mostramos el esquema de la data
dfPersona2.printSchema()

root
 |-- ID: string (nullable = true)
 |-- NOMBRE: string (nullable = true)
 |-- TELEFONO: string (nullable = true)
 |-- CORREO: string (nullable = true)
 |-- FECHA_INGRESO: string (nullable = true)
 |-- EDAD: integer (nullable = true)
 |-- SALARIO: double (nullable = true)
 |-- ID_EMPRESA: string (nullable = true)


// DBTITLE 1,3. Leyendo todos los archivos dentro de un directorio
//Vamos a crear un directorio llamado transacción

%fs mkdirs dbfs:///FileStore/_spark/transaccion


//Copiaremos los tres archivos de transacciones dentro

%fs cp dbfs:///FileStore/_spark/transacciones_2021_01_21.data dbfs:///FileStore/_spark/transaccion
%fs cp dbfs:///FileStore/_spark/transacciones_2021_01_22.data dbfs:///FileStore/_spark/transaccion
%fs cp dbfs:///FileStore/_spark/transacciones_2021_01_23.data dbfs:///FileStore/_spark/transaccion


//Verificamos

%fs ls dbfs:///FileStore/_spark/transaccion


//Vamos a colocar el contenido de todos los archivos de la carpeta en un dataframe
//Solo deberemos indicar la ruta asociada
//Para este ejemplo, el archivo tiene un delimitador de coma ","
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

dfTransaccion:org.apache.spark.sql.DataFrame
ID_PERSONA:string
ID_EMPRESA:string
MONTO:double
FECHA:string
dfTransaccion: org.apache.spark.sql.DataFrame = [ID_PERSONA: string, ID_EMPRESA: string ... 2 more fields]

//Vemos el contenido
dfTransaccion.show()

+----------+----------+------+----------+
|ID_PERSONA|ID_EMPRESA|MONTO |FECHA     |
+----------+----------+------+----------+
|26        |7         |2244.0|2021-01-23|
|24        |6         |1745.0|2021-01-23|
|1         |10        |238.0 |2021-01-23|
|19        |2         |3338.0|2021-01-23|
|95        |8         |2656.0|2021-01-23|
|37        |6         |3811.0|2021-01-23|
|40        |2         |297.0 |2021-01-23|
|27        |3         |2896.0|2021-01-23|
|65        |2         |4097.0|2021-01-23|
|21        |8         |2799.0|2021-01-23|
|71        |8         |3148.0|2021-01-23|
|71        |4         |2712.0|2021-01-23|
|34        |6         |513.0 |2021-01-23|
|71        |5         |1548.0|2021-01-23|
|81        |3         |1343.0|2021-01-23|
|90        |1         |4224.0|2021-01-23|
|87        |8         |1571.0|2021-01-23|
|82        |4         |3411.0|2021-01-23|
|61        |4         |871.0 |2021-01-23|
|48        |6         |783.0 |2021-01-23|
+----------+----------+------+----------+
only showing top 20 rows