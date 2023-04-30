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


// DBTITLE 1,3. Actions para ejecutar pasos de procesamiento
//DEFINIMOS LOS PASOS DE PROCESAMIENTO

//PASO 1: Agrupamos los datos según la edad
var df1 = dfPersona.groupBy(dfPersona.col("EDAD")).agg(
	f.count(dfPersona.col("EDAD")).alias("CANTIDAD"), 
	f.min(dfPersona.col("FECHA_INGRESO")).alias("FECHA_CONTRATO_MAS_RECIENTE"), 
	f.sum(dfPersona.col("SALARIO")).alias("SUMA_SALARIOS"), 
	f.max(dfPersona.col("SALARIO")).alias("SALARIO_MAYOR")
)

df1:org.apache.spark.sql.DataFrame
EDAD:integer
CANTIDAD:long
FECHA_CONTRATO_MAS_RECIENTE:string
SUMA_SALARIOS:double
SALARIO_MAYOR:double

//PASO 2: Filtramos por una EDAD
var df2 = df1.filter(df1.col("EDAD") > 35)

df2:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
EDAD:integer
CANTIDAD:long
FECHA_CONTRATO_MAS_RECIENTE:string
SUMA_SALARIOS:double
SALARIO_MAYOR:double

//PASO 3: Filtramos por SUMA_SALARIOS
var df3 = df2.filter(df2.col("SUMA_SALARIOS") > 20000)

df3:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
EDAD:integer
CANTIDAD:long
FECHA_CONTRATO_MAS_RECIENTE:string
SUMA_SALARIOS:double
SALARIO_MAYOR:double

//PASO 4: Filtramos por SALARIO_MAYOR
var dfResultado = df3.filter(df3.col("SALARIO_MAYOR") > 1000)

dfResultado:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
EDAD:integer
CANTIDAD:long
FECHA_CONTRATO_MAS_RECIENTE:string
SUMA_SALARIOS:double
SALARIO_MAYOR:double


//Al ejecutar la sección de código anterior, solamente hemos definido cómo se crearán los dataframes "df1", "df2", "df3" y "dfResultado"
//Los dataframes aún no se han creado y no ocupan memoria RAM en el clúster
//Para que se cree, deberemos llamar a un action
//Generalmente con los dataframes queremos hacer dos cosas, verlos o almacenarlos
//Estos son "actions" que ejecutan la cadena de procesos


//Al ejecutar "dfResultado.show()", la cadena de procesos asociada se ejecuta y se cargan en RAM todos los dataframes
dfResultado.show()

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


//Si volvemos a llamar al "action", nuevamente se tendrá que recalcular toda la cadena de procesos
//Si la cadena de procesos se demora 1 hora, para que el action "show" muestre los datos, cada vez que lo ejecutemos deberemos esperar 1 hora
dfResultado.show()

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


// DBTITLE 1,4. Procesamiento encadenado
//Definimos los pasos de procesamiento en una única cadena de proceso
var dfResultado2 = dfPersona.groupBy(dfPersona.col("EDAD")).agg(
	f.count(dfPersona.col("EDAD")).alias("CANTIDAD"), 
	f.min(dfPersona.col("FECHA_INGRESO")).alias("FECHA_CONTRATO_MAS_RECIENTE"), 
	f.sum(dfPersona.col("SALARIO")).alias("SUMA_SALARIOS"), 
	f.max(dfPersona.col("SALARIO")).alias("SALARIO_MAYOR")
).alias("P1").
filter(f.col("P1.EDAD") > 35).alias("P2").
filter(f.col("P2.SUMA_SALARIOS") > 20000).alias("P3").
filter(f.col("P3.SALARIO_MAYOR") > 1000)

dfResultado2:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
EDAD:integer
CANTIDAD:long
FECHA_CONTRATO_MAS_RECIENTE:string
SUMA_SALARIOS:double
SALARIO_MAYOR:double

//Al ejecutar la sección de código anterior, solamente hemos definido cómo se creará el dataframe "dfResultado2"
//Cada paso, también crea en memoria RAM un dataframe ("P1", "P2", "P3"), sólo que ya no lo tenemos en una variable
//Eso significa que encadenar procesos utiliza la misma cantidad de memoria RAM que el hacerlo en variables separadas
//Los dataframes aún no se han creado y no ocupan memoria RAM en el clúster
//Para que se cree, deberemos llamar a un action


//Al ejecutar "dfResultado2.show()", la cadena de procesos asociada se ejecuta y se cargan en RAM todos los dataframes
dfResultado2.show()

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

// DBTITLE 1,5. Posible colapso de memoria RAM
//Nuestros procesamiento independientemente de que estén en pasos separados o en una única cadena de procesamiento ocupa la misma cantidad de memoria RAM
//Mientras más pasos tenga el código, más dataframes ocuparán espacio en la memoria RAM
//La memoria RAM es limitada, si tenemos un script con muchos pasos de procesamiento, es probable que en algún momento termine colapsando y nuestro script no se ejecute
//Esto lo solucionaremos en laboratorios posteriores
