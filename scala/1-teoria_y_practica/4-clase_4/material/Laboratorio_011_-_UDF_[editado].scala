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


// DBTITLE 1,3. UDFs
//Hemos visto que SPARK tiene implementado un juego de funciones clásicas que podemos usar
//Si queremos usar nuestras funciones, debemos de implementarlas dentro de los objetos UDF
//Las funciones personalizadas las implementamos con el lenguaje de programación (SCALA)
//El UDF es el objeto que paraleliza el uso de la función


//Importamos las librerías para implementar UDFs
import org.apache.spark.sql.functions.udf


// DBTITLE 1,4. Implementación de función
//Funcion para calcular el salario anual
def calcularSalarioAnual(salarioMensual : Double) : Double = {
  //Calculo del salario anual
  var salarioAnual : Double = salarioMensual*12

  //Devolvemos el valor
  return salarioAnual
}


// DBTITLE 1,5. Definición del UDF
//Creamos la función personalizada
var udfCalcularSalarioAnual = udf((salarioMensual : Double) => calcularSalarioAnual(salarioMensual))


//Registramos el UDF
spark.udf.register("udfCalcularSalarioAnual", udfCalcularSalarioAnual)


// DBTITLE 1,6. Uso del UDF
//Aplicamos el UDF
var df1 = dfPersona.select(
  dfPersona.col("NOMBRE").alias("NOMBRE"),
  dfPersona.col("SALARIO").alias("SALARIO_MENSUAL"),
  udfCalcularSalarioAnual(dfPersona.col("SALARIO")).alias("SALARIO_ANUAL")
)

//Mostramos los datos
df1.show(truncate=false)

df1:org.apache.spark.sql.DataFrame
NOMBRE:string
SALARIO_MENSUAL:double
SALARIO_ANUAL:double

+---------+---------------+-------------+
|NOMBRE   |SALARIO_MENSUAL|SALARIO_ANUAL|
+---------+---------------+-------------+
|Carl     |20095.0        |241140.0     |
|Priscilla|9298.0         |111576.0     |
|Jocelyn  |10853.0        |130236.0     |
|Aidan    |3387.0         |40644.0      |
|Leandra  |22102.0        |265224.0     |
|Bert     |7800.0         |93600.0      |
|Mark     |8112.0         |97344.0      |
|Jonah    |17040.0        |204480.0     |
|Hanae    |6834.0         |82008.0      |
|Cadman   |7996.0         |95952.0      |
|Melyssa  |4913.0         |58956.0      |
|Tanner   |19943.0        |239316.0     |
|Trevor   |9501.0         |114012.0     |
|Allen    |16289.0        |195468.0     |
|Wanda    |1539.0         |18468.0      |
|Alden    |3377.0         |40524.0      |
|Omar     |6851.0         |82212.0      |
|Owen     |4759.0         |57108.0      |
|Laura    |17403.0        |208836.0     |
|Emery    |18752.0        |225024.0     |
+---------+---------------+-------------+
only showing top 20 rows


// DBTITLE 1,7. Otra forma de aplicar el UDF [Potencial Anti-Patrón]
//Aplicamos el UDF
var df2 = dfPersona.withColumn("SALARIO_ANUAL", udfCalcularSalarioAnual(dfPersona.col("SALARIO")))

//Mostramos los datos
df2.show(truncate=false)

df2:org.apache.spark.sql.DataFrame
ID:string
NOMBRE:string
TELEFONO:string
CORREO:string
FECHA_INGRESO:string
EDAD:integer
SALARIO:double
ID_EMPRESA:string
SALARIO_ANUAL:double

+---+---------+--------------+---------------------------------------+-------------+----+-------+----------+-------------+
|ID |NOMBRE   |TELEFONO      |CORREO                                 |FECHA_INGRESO|EDAD|SALARIO|ID_EMPRESA|SALARIO_ANUAL|
+---+---------+--------------+---------------------------------------+-------------+----+-------+----------+-------------+
|1  |Carl     |1-745-633-9145|arcu.Sed.et@ante.co.uk                 |2004-04-23   |32  |20095.0|5         |241140.0     |
|2  |Priscilla|155-2498      |Donec.egestas.Aliquam@volutpatnunc.edu |2019-02-17   |34  |9298.0 |2         |111576.0     |
|3  |Jocelyn  |1-204-956-8594|amet.diam@lobortis.co.uk               |2002-08-01   |27  |10853.0|3         |130236.0     |
|4  |Aidan    |1-719-862-9385|euismod.et.commodo@nibhlaciniaorci.edu |2018-11-06   |29  |3387.0 |10        |40644.0      |
|5  |Leandra  |839-8044      |at@pretiumetrutrum.com                 |2002-10-10   |41  |22102.0|1         |265224.0     |
|6  |Bert     |797-4453      |a.felis.ullamcorper@arcu.org           |2017-04-25   |70  |7800.0 |7         |93600.0      |
|7  |Mark     |1-680-102-6792|Quisque.ac@placerat.ca                 |2006-04-21   |52  |8112.0 |5         |97344.0      |
|8  |Jonah    |214-2975      |eu.ultrices.sit@vitae.ca               |2017-10-07   |23  |17040.0|5         |204480.0     |
|9  |Hanae    |935-2277      |eu@Nunc.ca                             |2003-05-25   |69  |6834.0 |3         |82008.0      |
|10 |Cadman   |1-866-561-2701|orci.adipiscing.non@semperNam.ca       |2001-05-19   |19  |7996.0 |7         |95952.0      |
|11 |Melyssa  |596-7736      |vel@vulputateposuerevulputate.net      |2008-10-14   |48  |4913.0 |8         |58956.0      |
|12 |Tanner   |1-739-776-7897|arcu.Aliquam.ultrices@sociis.com       |2011-05-10   |24  |19943.0|8         |239316.0     |
|13 |Trevor   |512-1955      |Nunc.quis.arcu@egestasa.org            |2010-08-06   |34  |9501.0 |5         |114012.0     |
|14 |Allen    |733-2795      |felis.Donec@necleo.org                 |2005-03-07   |59  |16289.0|2         |195468.0     |
|15 |Wanda    |359-6973      |Nam.nulla.magna@In.org                 |2005-08-21   |27  |1539.0 |5         |18468.0      |
|16 |Alden    |341-8522      |odio@morbitristiquesenectus.ca         |2006-12-05   |26  |3377.0 |2         |40524.0      |
|17 |Omar     |720-1543      |Phasellus.vitae.mauris@sollicitudin.net|2014-06-24   |60  |6851.0 |6         |82212.0      |
|18 |Owen     |1-167-335-7541|sociis@erat.com                        |2002-04-09   |34  |4759.0 |7         |57108.0      |
|19 |Laura    |1-974-623-2057|mollis@ornare.ca                       |2017-03-09   |70  |17403.0|4         |208836.0     |
|20 |Emery    |1-672-840-0264|at.nisi@vel.org                        |2004-02-27   |24  |18752.0|9         |225024.0     |
+---+---------+--------------+---------------------------------------+-------------+----+-------+----------+-------------+
only showing top 20 rows