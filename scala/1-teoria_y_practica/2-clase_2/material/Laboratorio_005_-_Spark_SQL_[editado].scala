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


// DBTITLE 1,2. Procesamiento con Spark SQL
//La mejor forma de aprovechar SPARK es con algún lenguaje de programación
//Por ejemplo podemos usar Java, Scala, Python o R para empezar a programar
//Pero, si no conocemos un lenguaje de programación, ¿podemos usar SPARK?
//Spark tiene un que nos permite procesar los dataframes con la sintaxis SQL
//A este módulo se le conoce como SPARK SQL
//SPARK SQL es ideal para implementar procesamiento de datos estructurados


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

dfPersona:org.apache.spark.sql.DataFrame
ID:string
NOMBRE:string
TELEFONO:string
CORREO:string
FECHA_INGRESO:string
EDAD:integer
SALARIO:double
ID_EMPRESA:string


//Mostramos los datos
dfPersona.show(truncate=false)

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


//Vamos a visualizar el dataframe como si fuera una tabla de base de datos
//Para eso deberemos registrar al dataframe como una "TempView"
dfPersona.createOrReplaceTempView("dfPersona")


//Visualizamos los dataframes que podemos procesar como tabla
spark.sql("SHOW VIEWS").show()

(1) Spark Jobs
Job 5 View(Stages: 1/1)
Stage 5: 1/1   
+---------+---------+-----------+--------------+
|namespace| viewName|isTemporary|isMaterialized|
+---------+---------+-----------+--------------+
|         |dfpersona|       true|         false|
+---------+---------+-----------+--------------+


//Filtraremos algunos registros según la edad
spark.sql("SELECT T.* FROM dfpersona T WHERE T.EDAD > 25").show(truncate=false)

+---+---------+--------------+---------------------------------------------------------+-------------+----+-------+----------+
|ID |NOMBRE   |TELEFONO      |CORREO                                                   |FECHA_INGRESO|EDAD|SALARIO|ID_EMPRESA|
+---+---------+--------------+---------------------------------------------------------+-------------+----+-------+----------+
|1  |Carl     |1-745-633-9145|arcu.Sed.et@ante.co.uk                                   |2004-04-23   |32  |20095.0|5         |
|2  |Priscilla|155-2498      |Donec.egestas.Aliquam@volutpatnunc.edu                   |2019-02-17   |34  |9298.0 |2         |
|3  |Jocelyn  |1-204-956-8594|amet.diam@lobortis.co.uk                                 |2002-08-01   |27  |10853.0|3         |
|4  |Aidan    |1-719-862-9385|euismod.et.commodo@nibhlaciniaorci.edu                   |2018-11-06   |29  |3387.0 |10        |
|5  |Leandra  |839-8044      |at@pretiumetrutrum.com                                   |2002-10-10   |41  |22102.0|1         |
|6  |Bert     |797-4453      |a.felis.ullamcorper@arcu.org                             |2017-04-25   |70  |7800.0 |7         |
|7  |Mark     |1-680-102-6792|Quisque.ac@placerat.ca                                   |2006-04-21   |52  |8112.0 |5         |
|9  |Hanae    |935-2277      |eu@Nunc.ca                                               |2003-05-25   |69  |6834.0 |3         |
|11 |Melyssa  |596-7736      |vel@vulputateposuerevulputate.net                        |2008-10-14   |48  |4913.0 |8         |
|13 |Trevor   |512-1955      |Nunc.quis.arcu@egestasa.org                              |2010-08-06   |34  |9501.0 |5         |
|14 |Allen    |733-2795      |felis.Donec@necleo.org                                   |2005-03-07   |59  |16289.0|2         |
|15 |Wanda    |359-6973      |Nam.nulla.magna@In.org                                   |2005-08-21   |27  |1539.0 |5         |
|16 |Alden    |341-8522      |odio@morbitristiquesenectus.ca                           |2006-12-05   |26  |3377.0 |2         |
|17 |Omar     |720-1543      |Phasellus.vitae.mauris@sollicitudin.net                  |2014-06-24   |60  |6851.0 |6         |
|18 |Owen     |1-167-335-7541|sociis@erat.com                                          |2002-04-09   |34  |4759.0 |7         |
|19 |Laura    |1-974-623-2057|mollis@ornare.ca                                         |2017-03-09   |70  |17403.0|4         |
|21 |Carissa  |1-300-877-0859|dignissim.pharetra.Nam@Pellentesqueultriciesdignissim.com|2011-10-16   |31  |1952.0 |10        |
|23 |Samson   |1-430-188-6663|urna.justo.faucibus@neceleifendnon.net                   |2011-12-15   |51  |8099.0 |6         |
|25 |Pearl    |1-850-202-3373|vel.convallis@rhoncus.co.uk                              |2018-12-21   |52  |14756.0|6         |
|26 |Brenden  |1-455-726-9413|elit.pede.malesuada@liberomaurisaliquam.org              |2000-03-17   |33  |20549.0|7         |
+---+---------+--------------+---------------------------------------------------------+-------------+----+-------+----------+
only showing top 20 rows


//En ocasiones los SQL pueden ser muy grandes
//Para escribirlos de una mejor manera, utilizaremos la triple comilla doble
//Nos permitira colocar "ENTERS" en las sentencias para ordenar el código
spark.sql("""
SELECT
  T.*
FROM
  dfPersona T
WHERE
  T.EDAD > 25
""").show(truncate=false)

+---+---------+--------------+---------------------------------------------------------+-------------+----+-------+----------+
|ID |NOMBRE   |TELEFONO      |CORREO                                                   |FECHA_INGRESO|EDAD|SALARIO|ID_EMPRESA|
+---+---------+--------------+---------------------------------------------------------+-------------+----+-------+----------+
|1  |Carl     |1-745-633-9145|arcu.Sed.et@ante.co.uk                                   |2004-04-23   |32  |20095.0|5         |
|2  |Priscilla|155-2498      |Donec.egestas.Aliquam@volutpatnunc.edu                   |2019-02-17   |34  |9298.0 |2         |
|3  |Jocelyn  |1-204-956-8594|amet.diam@lobortis.co.uk                                 |2002-08-01   |27  |10853.0|3         |
|4  |Aidan    |1-719-862-9385|euismod.et.commodo@nibhlaciniaorci.edu                   |2018-11-06   |29  |3387.0 |10        |
|5  |Leandra  |839-8044      |at@pretiumetrutrum.com                                   |2002-10-10   |41  |22102.0|1         |
|6  |Bert     |797-4453      |a.felis.ullamcorper@arcu.org                             |2017-04-25   |70  |7800.0 |7         |
|7  |Mark     |1-680-102-6792|Quisque.ac@placerat.ca                                   |2006-04-21   |52  |8112.0 |5         |
|9  |Hanae    |935-2277      |eu@Nunc.ca                                               |2003-05-25   |69  |6834.0 |3         |
|11 |Melyssa  |596-7736      |vel@vulputateposuerevulputate.net                        |2008-10-14   |48  |4913.0 |8         |
|13 |Trevor   |512-1955      |Nunc.quis.arcu@egestasa.org                              |2010-08-06   |34  |9501.0 |5         |
|14 |Allen    |733-2795      |felis.Donec@necleo.org                                   |2005-03-07   |59  |16289.0|2         |
|15 |Wanda    |359-6973      |Nam.nulla.magna@In.org                                   |2005-08-21   |27  |1539.0 |5         |
|16 |Alden    |341-8522      |odio@morbitristiquesenectus.ca                           |2006-12-05   |26  |3377.0 |2         |
|17 |Omar     |720-1543      |Phasellus.vitae.mauris@sollicitudin.net                  |2014-06-24   |60  |6851.0 |6         |
|18 |Owen     |1-167-335-7541|sociis@erat.com                                          |2002-04-09   |34  |4759.0 |7         |
|19 |Laura    |1-974-623-2057|mollis@ornare.ca                                         |2017-03-09   |70  |17403.0|4         |
|21 |Carissa  |1-300-877-0859|dignissim.pharetra.Nam@Pellentesqueultriciesdignissim.com|2011-10-16   |31  |1952.0 |10        |
|23 |Samson   |1-430-188-6663|urna.justo.faucibus@neceleifendnon.net                   |2011-12-15   |51  |8099.0 |6         |
|25 |Pearl    |1-850-202-3373|vel.convallis@rhoncus.co.uk                              |2018-12-21   |52  |14756.0|6         |
|26 |Brenden  |1-455-726-9413|elit.pede.malesuada@liberomaurisaliquam.org              |2000-03-17   |33  |20549.0|7         |
+---+---------+--------------+---------------------------------------------------------+-------------+----+-------+----------+
only showing top 20 rows


//También, podemos almacenar el resultado del procesamiento SQL en una variable dataframe
//NOTAR QUE AQUÍ NO AGREGAMOS EL ".show()" AL FINAL
var df1 = spark.sql("""
SELECT
  T.*
FROM
  dfPersona T
WHERE
  T.EDAD > 25
""")

//Vemos el resultado
df1.show(truncate=false)

+---+---------+--------------+---------------------------------------------------------+-------------+----+-------+----------+
|ID |NOMBRE   |TELEFONO      |CORREO                                                   |FECHA_INGRESO|EDAD|SALARIO|ID_EMPRESA|
+---+---------+--------------+---------------------------------------------------------+-------------+----+-------+----------+
|1  |Carl     |1-745-633-9145|arcu.Sed.et@ante.co.uk                                   |2004-04-23   |32  |20095.0|5         |
|2  |Priscilla|155-2498      |Donec.egestas.Aliquam@volutpatnunc.edu                   |2019-02-17   |34  |9298.0 |2         |
|3  |Jocelyn  |1-204-956-8594|amet.diam@lobortis.co.uk                                 |2002-08-01   |27  |10853.0|3         |
|4  |Aidan    |1-719-862-9385|euismod.et.commodo@nibhlaciniaorci.edu                   |2018-11-06   |29  |3387.0 |10        |
|5  |Leandra  |839-8044      |at@pretiumetrutrum.com                                   |2002-10-10   |41  |22102.0|1         |
|6  |Bert     |797-4453      |a.felis.ullamcorper@arcu.org                             |2017-04-25   |70  |7800.0 |7         |
|7  |Mark     |1-680-102-6792|Quisque.ac@placerat.ca                                   |2006-04-21   |52  |8112.0 |5         |
|9  |Hanae    |935-2277      |eu@Nunc.ca                                               |2003-05-25   |69  |6834.0 |3         |
|11 |Melyssa  |596-7736      |vel@vulputateposuerevulputate.net                        |2008-10-14   |48  |4913.0 |8         |
|13 |Trevor   |512-1955      |Nunc.quis.arcu@egestasa.org                              |2010-08-06   |34  |9501.0 |5         |
|14 |Allen    |733-2795      |felis.Donec@necleo.org                                   |2005-03-07   |59  |16289.0|2         |
|15 |Wanda    |359-6973      |Nam.nulla.magna@In.org                                   |2005-08-21   |27  |1539.0 |5         |
|16 |Alden    |341-8522      |odio@morbitristiquesenectus.ca                           |2006-12-05   |26  |3377.0 |2         |
|17 |Omar     |720-1543      |Phasellus.vitae.mauris@sollicitudin.net                  |2014-06-24   |60  |6851.0 |6         |
|18 |Owen     |1-167-335-7541|sociis@erat.com                                          |2002-04-09   |34  |4759.0 |7         |
|19 |Laura    |1-974-623-2057|mollis@ornare.ca                                         |2017-03-09   |70  |17403.0|4         |
|21 |Carissa  |1-300-877-0859|dignissim.pharetra.Nam@Pellentesqueultriciesdignissim.com|2011-10-16   |31  |1952.0 |10        |
|23 |Samson   |1-430-188-6663|urna.justo.faucibus@neceleifendnon.net                   |2011-12-15   |51  |8099.0 |6         |
|25 |Pearl    |1-850-202-3373|vel.convallis@rhoncus.co.uk                              |2018-12-21   |52  |14756.0|6         |
|26 |Brenden  |1-455-726-9413|elit.pede.malesuada@liberomaurisaliquam.org              |2000-03-17   |33  |20549.0|7         |
+---+---------+--------------+---------------------------------------------------------+-------------+----+-------+----------+
only showing top 20 rows


//En este otro ejemplo, creamos otro dataframe donde:
//Seleccionamos el campos ID, NOMBRE, CORREO, EDAD, SALARIO
//Las personas tengan más de 25 años Y
//Las personas tengan un salario mayor a 5000 dólares
var df2 = spark.sql("""
SELECT
  T.ID,
  T.NOMBRE,
  T.CORREO,
  T.EDAD,
  T.SALARIO
FROM
  dfPersona T
WHERE
  T.EDAD > 25 AND
  T.SALARIO > 5000
""")

//Vemos el resultado
df2.show(truncate=false)

+---+---------+-------------------------------------------+----+-------+
|ID |NOMBRE   |CORREO                                     |EDAD|SALARIO|
+---+---------+-------------------------------------------+----+-------+
|1  |Carl     |arcu.Sed.et@ante.co.uk                     |32  |20095.0|
|2  |Priscilla|Donec.egestas.Aliquam@volutpatnunc.edu     |34  |9298.0 |
|3  |Jocelyn  |amet.diam@lobortis.co.uk                   |27  |10853.0|
|5  |Leandra  |at@pretiumetrutrum.com                     |41  |22102.0|
|6  |Bert     |a.felis.ullamcorper@arcu.org               |70  |7800.0 |
|7  |Mark     |Quisque.ac@placerat.ca                     |52  |8112.0 |
|9  |Hanae    |eu@Nunc.ca                                 |69  |6834.0 |
|13 |Trevor   |Nunc.quis.arcu@egestasa.org                |34  |9501.0 |
|14 |Allen    |felis.Donec@necleo.org                     |59  |16289.0|
|17 |Omar     |Phasellus.vitae.mauris@sollicitudin.net    |60  |6851.0 |
|19 |Laura    |mollis@ornare.ca                           |70  |17403.0|
|23 |Samson   |urna.justo.faucibus@neceleifendnon.net     |51  |8099.0 |
|25 |Pearl    |vel.convallis@rhoncus.co.uk                |52  |14756.0|
|26 |Brenden  |elit.pede.malesuada@liberomaurisaliquam.org|33  |20549.0|
|27 |Alexander|semper.auctor.Mauris@mollislectuspede.edu  |55  |13813.0|
|28 |Stephen  |arcu.Aliquam.ultrices@acnullaIn.co.uk      |53  |9469.0 |
|29 |Jana     |sed.dolor.Fusce@Sedet.co.uk                |39  |6483.0 |
|30 |Clayton  |est.Nunc@dictumeuplacerat.com              |52  |9505.0 |
|31 |Rylee    |Sed.nunc@turpis.edu                        |47  |21591.0|
|32 |Gisela   |Praesent.luctus@dui.co.uk                  |67  |6497.0 |
+---+---------+-------------------------------------------+----+-------+
only showing top 20 rows


// DBTITLE 1,3. Parametrizando el código SQL
//Crearemos dos variables para filtrar por edad y salario
var PARAM_EDAD = 25
var PARAM_SALARIO = 5000

// COMMAND ----------

//Crearemos la misma consulta anterior, pero parametrizada
//Debemos anteponer el caracter f antes de la triple comilla doble
//Para usar un parametro en el codigo debemos de escribir $NOMBRE_PARAMETRO%s
var df3 = spark.sql(f"""
SELECT
  T.ID,
  T.NOMBRE,
  T.CORREO,
  T.EDAD,
  T.SALARIO
FROM
  dfPersona T
WHERE
  T.EDAD > $PARAM_EDAD%s AND
  T.SALARIO > $PARAM_SALARIO%s
""")

//Vemos el resultado
df3.show(truncate=false)

+---+---------+-------------------------------------------+----+-------+
|ID |NOMBRE   |CORREO                                     |EDAD|SALARIO|
+---+---------+-------------------------------------------+----+-------+
|1  |Carl     |arcu.Sed.et@ante.co.uk                     |32  |20095.0|
|2  |Priscilla|Donec.egestas.Aliquam@volutpatnunc.edu     |34  |9298.0 |
|3  |Jocelyn  |amet.diam@lobortis.co.uk                   |27  |10853.0|
|5  |Leandra  |at@pretiumetrutrum.com                     |41  |22102.0|
|6  |Bert     |a.felis.ullamcorper@arcu.org               |70  |7800.0 |
|7  |Mark     |Quisque.ac@placerat.ca                     |52  |8112.0 |
|9  |Hanae    |eu@Nunc.ca                                 |69  |6834.0 |
|13 |Trevor   |Nunc.quis.arcu@egestasa.org                |34  |9501.0 |
|14 |Allen    |felis.Donec@necleo.org                     |59  |16289.0|
|17 |Omar     |Phasellus.vitae.mauris@sollicitudin.net    |60  |6851.0 |
|19 |Laura    |mollis@ornare.ca                           |70  |17403.0|
|23 |Samson   |urna.justo.faucibus@neceleifendnon.net     |51  |8099.0 |
|25 |Pearl    |vel.convallis@rhoncus.co.uk                |52  |14756.0|
|26 |Brenden  |elit.pede.malesuada@liberomaurisaliquam.org|33  |20549.0|
|27 |Alexander|semper.auctor.Mauris@mollislectuspede.edu  |55  |13813.0|
|28 |Stephen  |arcu.Aliquam.ultrices@acnullaIn.co.uk      |53  |9469.0 |
|29 |Jana     |sed.dolor.Fusce@Sedet.co.uk                |39  |6483.0 |
|30 |Clayton  |est.Nunc@dictumeuplacerat.com              |52  |9505.0 |
|31 |Rylee    |Sed.nunc@turpis.edu                        |47  |21591.0|
|32 |Gisela   |Praesent.luctus@dui.co.uk                  |67  |6497.0 |
+---+---------+-------------------------------------------+----+-------+
only showing top 20 rows


// DBTITLE 1,4. Escribiendo la resultante en disco duro
//Crearemos una carpeta "output" dentro de "FileStore"

%fs mkdirs dbfs:///FileStore/_spark/output


//Dentro de este directorio escribiremos la resultante del dataframe "df3"
//Es importante notar que para almacenar el dataframe, se creará un directorio y dentro los archivos del dataframe
//Esto se hace así porque un dataframe puede tener billones de registros, para hacer la escritura en paralelo, se escriben 
//dentro del directorio varios archivos de manera paralela
//Podemos elegir dos modos de escritura:
//
// overwrite: si el directorio ya existe y tiene archivos, borra todos los archivos y luego escribe los archivos del dataframe
// append: si el directorio ya existe y tiene archivos, agrega los archivos del dataframe dentro del directorio
df3.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", "|").save("dbfs:///FileStore/_spark/output/df3")


//Para verificar la escritura en disco duro, vamos a listar el contenido de "output"
//Notaremos que se ha creado la carpeta "df3"

%fs ls dbfs:///FileStore/_spark/output


//Ahora listaremos la carpeta "df3"

%fs ls dbfs:///FileStore/_spark/output/df3


//Encontraremos los siguientes archivos:
//
// _SUCCESS indica que el proceso de escritura finalizo correctamente, si finalizara con errores colocaría un archivo _ERROR
// _committed_... indica la hora en que el proceso ingresó al clúster
// _started_... indica la hora en que el proceso comenzó a ejecutarse
// part-00000-... son los archivos que tienen el contenido del dataframe


// No necesariamente un dataframe se convierte en un archivo, un dataframe puede ser 10, 20 o 30 archivos de datos, eso esta 
// en función de cuantas particiones tiene el dataframe, esta es una técnica de optimización. En ocasiones, un dataframe no
// puede tener MUCHAS PARTICIONES porque sino el proceso en muy lento y tampoco puede tener POCAS PARTICIONES porque sino 
// colapsa la memoria RAM. Entonces, ¿cual es el numero exacto de particiones? depende de cada contexto, en un dataframe quizas
// 10 particiones y en otro dataframe quzias 20. Eso también es una tpecnica de tunning. 


//Para verificar, leemos el directorio del dataframe en una variable
var df3Leido = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        Array(
            StructField("ID", StringType, true),
            StructField("NOMBRE", StringType, true),
            StructField("CORREO", StringType, true),
            StructField("EDAD", IntegerType, true),
            StructField("SALARIO", DoubleType, true)
        )
    )
).load("dbfs:///FileStore/_spark/output/df3")

//Mostramos los datos
df3Leido.show(truncate=false)

+---+---------+-------------------------------------------+----+-------+
|ID |NOMBRE   |CORREO                                     |EDAD|SALARIO|
+---+---------+-------------------------------------------+----+-------+
|1  |Carl     |arcu.Sed.et@ante.co.uk                     |32  |20095.0|
|2  |Priscilla|Donec.egestas.Aliquam@volutpatnunc.edu     |34  |9298.0 |
|3  |Jocelyn  |amet.diam@lobortis.co.uk                   |27  |10853.0|
|5  |Leandra  |at@pretiumetrutrum.com                     |41  |22102.0|
|6  |Bert     |a.felis.ullamcorper@arcu.org               |70  |7800.0 |
|7  |Mark     |Quisque.ac@placerat.ca                     |52  |8112.0 |
|9  |Hanae    |eu@Nunc.ca                                 |69  |6834.0 |
|13 |Trevor   |Nunc.quis.arcu@egestasa.org                |34  |9501.0 |
|14 |Allen    |felis.Donec@necleo.org                     |59  |16289.0|
|17 |Omar     |Phasellus.vitae.mauris@sollicitudin.net    |60  |6851.0 |
|19 |Laura    |mollis@ornare.ca                           |70  |17403.0|
|23 |Samson   |urna.justo.faucibus@neceleifendnon.net     |51  |8099.0 |
|25 |Pearl    |vel.convallis@rhoncus.co.uk                |52  |14756.0|
|26 |Brenden  |elit.pede.malesuada@liberomaurisaliquam.org|33  |20549.0|
|27 |Alexander|semper.auctor.Mauris@mollislectuspede.edu  |55  |13813.0|
|28 |Stephen  |arcu.Aliquam.ultrices@acnullaIn.co.uk      |53  |9469.0 |
|29 |Jana     |sed.dolor.Fusce@Sedet.co.uk                |39  |6483.0 |
|30 |Clayton  |est.Nunc@dictumeuplacerat.com              |52  |9505.0 |
|31 |Rylee    |Sed.nunc@turpis.edu                        |47  |21591.0|
|32 |Gisela   |Praesent.luctus@dui.co.uk                  |67  |6497.0 |
+---+---------+-------------------------------------------+----+-------+
only showing top 20 rows