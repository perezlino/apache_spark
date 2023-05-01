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

//Importamos las librerías para implementar UDFs
import org.apache.spark.sql.functions.udf



// DBTITLE 1,2. Lectura de datos
//Leemos los datos
var dfJson = spark.read.format("json").load("dbfs:///FileStore/_spark/transacciones.json")

{
  "PERSONA":{
    "ID_PERSONA":"26",
    "NOMBRE_PERSONA":"Brenden",
    "EDAD":33,
    "SALARIO":20549.0,
    "CONTACTO":[
      {"PREFIJO":"59","TELEFONO":"9811935"},
      {"PREFIJO":"53","TELEFONO":"9423163"}
      ]
  },
  "EMPRESA":{
    "ID_EMPRESA":"5",
    "NOMBRE_EMPRESA":"Amazon"
  },
  "TRANSACCION":{
    "MONTO":2628.0,
    "FECHA":"2021-01-23"
  }
}

//Mostramos los datos
//Vemos que lo está truncando
dfJson.show()

dfJson:org.apache.spark.sql.DataFrame
EMPRESA:struct
  ID_EMPRESA:string
  NOMBRE_EMPRESA:string
PERSONA:struct
  CONTACTO:array
    element:struct
      PREFIJO:string
      TELEFONO:string
  EDAD:long
  ID_PERSONA:string
  NOMBRE_PERSONA:string
  SALARIO:double
TRANSACCION:struct
  FECHA:string
  MONTO:double

+--------------+--------------------------------------------------------------------------------------------------------+--------------------+
|EMPRESA       |PERSONA                                                                                                 |TRANSACCION         |
+--------------+--------------------------------------------------------------------------------------------------------+--------------------+
|{5, Amazon}   |{[{59, 9811935}, {53, 9423163}], 33, 26, Brenden, 20549.0}                                              |{2021-01-23, 2628.0}|
|{9, IBM}      |{[{50, 9912937}, {54, 9046676}, {55, 9874284}, {58, 9746053}, {53, 9058704}], 31, 21, Carissa, 1952.0}  |{2021-01-23, 4261.0}|
|{7, Samsung}  |{[{53, 9769557}, {59, 9754523}, {52, 9063371}, {55, 9301624}, {56, 9770100}], 42, 73, Fiona, 9960.0}    |{2021-01-23, 1429.0}|
|{5, Amazon}   |{[{51, 9733329}, {57, 9619332}, {51, 9087416}, {50, 9486747}], 59, 14, Allen, 16289.0}                  |{2021-01-23, 3385.0}|
|{4, Toyota}   |{[{52, 9091334}, {59, 9831571}], 59, 80, Ebony, 3600.0}                                                 |{2021-01-23, 3514.0}|
|{9, IBM}      |{[{59, 9708669}, {52, 9751344}], 22, 53, Zachery, 23820.0}                                              |{2021-01-23, 823.0} |
|{2, Microsoft}|{null, 47, 31, Rylee, 21591.0}                                                                          |{2021-01-23, 3724.0}|
|{10, Sony}    |{[{51, 9443174}], 64, 55, Jennifer, 19013.0}                                                            |{2021-01-23, 3429.0}|
|{4, Toyota}   |{[{54, 9375039}, {58, 9397273}], 22, 45, Kylynn, 7040.0}                                                |{2021-01-23, 4267.0}|
|{9, IBM}      |{[{59, 9227653}, {56, 9409477}, {52, 9710151}], 22, 22, Kibo, 7449.0}                                   |{2021-01-23, 796.0} |
|{4, Toyota}   |{[{53, 9758464}, {54, 9188183}, {53, 9347047}, {57, 9681777}], 28, 64, Graiden, 22037.0}                |{2021-01-23, 317.0} |
|{9, IBM}      |{[{51, 9058211}, {58, 9955938}, {59, 9341683}, {51, 9805215}, {56, 9475194}], 42, 90, Sigourney, 9145.0}|{2021-01-23, 938.0} |
|{8, HP}       |{[{57, 9251990}, {52, 9332747}, {54, 9419616}, {51, 9912326}], 26, 59, Quemby, 12092.0}                 |{2021-01-23, 887.0} |
|{5, Amazon}   |{[{51, 9993481}], 60, 17, Omar, 6851.0}                                                                 |{2021-01-23, 2067.0}|
|{7, Samsung}  |{null, 28, 64, Graiden, 22037.0}                                                                        |{2021-01-23, 714.0} |
|{5, Amazon}   |{[{51, 9827831}, {57, 9378537}, {58, 9392860}, {52, 9428324}], 33, 61, null, 15070.0}                   |{2021-01-23, 2967.0}|
|{1, Walmart}  |{[{53, 9976594}, {53, 9714105}, {55, 9501594}, {51, 9954189}], 33, 84, Keith, 13348.0}                  |{2021-01-23, 769.0} |
|{3, Apple}    |{[{58, 9590391}, {50, 9740832}], 54, 35, Aurora, 4588.0}                                                |{2021-01-23, 3546.0}|
|{2, Microsoft}|{[{50, 9118416}, {54, 9584852}, {53, 9861048}, {57, 9951844}, {52, 9989666}], 52, 25, Pearl, 14756.0}   |{2021-01-23, 642.0} |
|{3, Apple}    |{[{54, 9427144}], 51, 79, Philip, 3919.0}                                                               |{2021-01-23, 275.0} |
+--------------+--------------------------------------------------------------------------------------------------------+--------------------+
only showing top 20 rows


//Mostramos los 20 primeros registros sin truncar el output en consola
dfJson.show(20, false)


//Pintamos el esquema
dfJson.printSchema()

root
 |-- EMPRESA: struct (nullable = true)
 |    |-- ID_EMPRESA: string (nullable = true)
 |    |-- NOMBRE_EMPRESA: string (nullable = true)
 |-- PERSONA: struct (nullable = true)
 |    |-- CONTACTO: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- PREFIJO: string (nullable = true)
 |    |    |    |-- TELEFONO: string (nullable = true)
 |    |-- EDAD: long (nullable = true)
 |    |-- ID_PERSONA: string (nullable = true)
 |    |-- NOMBRE_PERSONA: string (nullable = true)
 |    |-- SALARIO: double (nullable = true)
 |-- TRANSACCION: struct (nullable = true)
 |    |-- FECHA: string (nullable = true)
 |    |-- MONTO: double (nullable = true)


// DBTITLE 1,3. Navegando entre campos y sub-campos
//Seleccionar algunos campos
var df1 = dfJson.select("EMPRESA", "PERSONA")

//Mostramos la data
df1.show(5, false)

df1:org.apache.spark.sql.DataFrame
EMPRESA:struct
  ID_EMPRESA:string
  NOMBRE_EMPRESA:string
PERSONA:struct
  CONTACTO:array
    element:struct
      PREFIJO:string
      TELEFONO:string
  EDAD:long
  ID_PERSONA:string
  NOMBRE_PERSONA:string
  SALARIO:double

+------------+------------------------------------------------------------------------------------------------------+
|EMPRESA     |PERSONA                                                                                               |
+------------+------------------------------------------------------------------------------------------------------+
|{5, Amazon} |{[{59, 9811935}, {53, 9423163}], 33, 26, Brenden, 20549.0}                                            |
|{9, IBM}    |{[{50, 9912937}, {54, 9046676}, {55, 9874284}, {58, 9746053}, {53, 9058704}], 31, 21, Carissa, 1952.0}|
|{7, Samsung}|{[{53, 9769557}, {59, 9754523}, {52, 9063371}, {55, 9301624}, {56, 9770100}], 42, 73, Fiona, 9960.0}  |
|{5, Amazon} |{[{51, 9733329}, {57, 9619332}, {51, 9087416}, {50, 9486747}], 59, 14, Allen, 16289.0}                |
|{4, Toyota} |{[{52, 9091334}, {59, 9831571}], 59, 80, Ebony, 3600.0}                                               |
+------------+------------------------------------------------------------------------------------------------------+
only showing top 5 rows


//Seleccionar los subcampos
var df2 = dfJson.select("EMPRESA.ID_EMPRESA", "EMPRESA.NOMBRE_EMPRESA", "PERSONA.ID_PERSONA", "PERSONA.NOMBRE_PERSONA", "PERSONA.CONTACTO")

//Mostramos la data
df2.show(5, false)

df2:org.apache.spark.sql.DataFrame
ID_EMPRESA:string
NOMBRE_EMPRESA:string
ID_PERSONA:string
NOMBRE_PERSONA:string
CONTACTO:array
  element:struct
    PREFIJO:string
    TELEFONO:string

+----------+--------------+----------+--------------+---------------------------------------------------------------------------+
|ID_EMPRESA|NOMBRE_EMPRESA|ID_PERSONA|NOMBRE_PERSONA|CONTACTO                                                                   |
+----------+--------------+----------+--------------+---------------------------------------------------------------------------+
|5         |Amazon        |26        |Brenden       |[{59, 9811935}, {53, 9423163}]                                             |
|9         |IBM           |21        |Carissa       |[{50, 9912937}, {54, 9046676}, {55, 9874284}, {58, 9746053}, {53, 9058704}]|
|7         |Samsung       |73        |Fiona         |[{53, 9769557}, {59, 9754523}, {52, 9063371}, {55, 9301624}, {56, 9770100}]|
|5         |Amazon        |14        |Allen         |[{51, 9733329}, {57, 9619332}, {51, 9087416}, {50, 9486747}]               |
|4         |Toyota        |80        |Ebony         |[{52, 9091334}, {59, 9831571}]                                             |
+----------+--------------+----------+--------------+---------------------------------------------------------------------------+
only showing top 5 rows


//Navegamos por los sub-campos de PERSONA
var df3 = dfJson.select(
  dfJson.col("PERSONA.ID_PERSONA"), 
  dfJson.col("PERSONA.NOMBRE_PERSONA"), 
  dfJson.col("PERSONA.CONTACTO")
)

//Mostramos la data
df3.show(5, false)

df3:org.apache.spark.sql.DataFrame
ID_PERSONA:string
NOMBRE_PERSONA:string
CONTACTO:array
  element:struct
    PREFIJO:string
    TELEFONO:string

+----------+--------------+---------------------------------------------------------------------------+
|ID_PERSONA|NOMBRE_PERSONA|CONTACTO                                                                   |
+----------+--------------+---------------------------------------------------------------------------+
|26        |Brenden       |[{59, 9811935}, {53, 9423163}]                                             |
|21        |Carissa       |[{50, 9912937}, {54, 9046676}, {55, 9874284}, {58, 9746053}, {53, 9058704}]|
|73        |Fiona         |[{53, 9769557}, {59, 9754523}, {52, 9063371}, {55, 9301624}, {56, 9770100}]|
|14        |Allen         |[{51, 9733329}, {57, 9619332}, {51, 9087416}, {50, 9486747}]               |
|80        |Ebony         |[{52, 9091334}, {59, 9831571}]                                             |
+----------+--------------+---------------------------------------------------------------------------+
only showing top 5 rows


//Nos enfocaremos en el sub-campo CONTACTO
var df4 = dfJson.select(dfJson.col("PERSONA.CONTACTO"))

//Mostramos la data
df4.show(5, false)

df4:org.apache.spark.sql.DataFrame
CONTACTO:array
  element:struct
    PREFIJO:string
    TELEFONO:string

+---------------------------------------------------------------------------+
|CONTACTO                                                                   |
+---------------------------------------------------------------------------+
|[{59, 9811935}, {53, 9423163}]                                             |
|[{50, 9912937}, {54, 9046676}, {55, 9874284}, {58, 9746053}, {53, 9058704}]|
|[{53, 9769557}, {59, 9754523}, {52, 9063371}, {55, 9301624}, {56, 9770100}]|
|[{51, 9733329}, {57, 9619332}, {51, 9087416}, {50, 9486747}]               |
|[{52, 9091334}, {59, 9831571}]                                             |
+---------------------------------------------------------------------------+
only showing top 5 rows


// DBTITLE 1,4. Navegando en campos arrays
//Obtenemos el primer elemento
var df5 = dfJson.select(dfJson.col("PERSONA.CONTACTO").getItem(0))

//Mostramos la data
df5.show(5, false)

df5:org.apache.spark.sql.DataFrame
PERSONA.CONTACTO AS CONTACTO[0]:struct
  PREFIJO:string
  TELEFONO:string
+-------------------------------+
|PERSONA.CONTACTO AS CONTACTO[0]|
+-------------------------------+
|{59, 9811935}                  |
|{50, 9912937}                  |
|{53, 9769557}                  |
|{51, 9733329}                  |
|{52, 9091334}                  |
+-------------------------------+
only showing top 5 rows


//Colocamos un alias
var df6 = dfJson.select(dfJson.col("PERSONA.CONTACTO").getItem(0).alias("CONTACTO_1"))

//Mostramos la data
df6.show(5, false)

df6:org.apache.spark.sql.DataFrame
CONTACTO_1:struct
  PREFIJO:string
  TELEFONO:string
+-------------+
|CONTACTO_1   |
+-------------+
|{59, 9811935}|
|{50, 9912937}|
|{53, 9769557}|
|{51, 9733329}|
|{52, 9091334}|
+-------------+
only showing top 5 rows


//Vemos el esquema
df6.printSchema()

root
 |-- CONTACTO_1: struct (nullable = true)
 |    |-- PREFIJO: string (nullable = true)
 |    |-- TELEFONO: string (nullable = true)


//Obtenemos los sub-campos del primer elemento
//Podemos navegar de dos formas
//Usaremos la primera "getItem"
var df7 = dfJson.select(
    dfJson.col("PERSONA.NOMBRE_PERSONA"),
    dfJson.col("PERSONA.CONTACTO").getItem(0).getItem("PREFIJO"),
    dfJson.col("PERSONA.CONTACTO").getItem(0)("TELEFONO")
)

//Mostramos la data
df7.show(5, false)

df7:org.apache.spark.sql.DataFrame
NOMBRE_PERSONA:string
PERSONA.CONTACTO AS CONTACTO[0].PREFIJO:string
PERSONA.CONTACTO AS CONTACTO[0].TELEFONO:string

+--------------+---------------------------------------+----------------------------------------+
|NOMBRE_PERSONA|PERSONA.CONTACTO AS CONTACTO[0].PREFIJO|PERSONA.CONTACTO AS CONTACTO[0].TELEFONO|
+--------------+---------------------------------------+----------------------------------------+
|Brenden       |59                                     |9811935                                 |
|Carissa       |50                                     |9912937                                 |
|Fiona         |53                                     |9769557                                 |
|Allen         |51                                     |9733329                                 |
|Ebony         |52                                     |9091334                                 |
+--------------+---------------------------------------+----------------------------------------+
only showing top 5 rows


//Colocamos un alias
var df8 = dfJson.select(
    dfJson.col("PERSONA.NOMBRE_PERSONA"),
    dfJson.col("PERSONA.CONTACTO").getItem(0).getItem("PREFIJO").alias("PREFIJO_1"),
    dfJson.col("PERSONA.CONTACTO").getItem(0).getItem("TELEFONO").alias("TELEFONO_1")
)

//Mostramos la data
df8.show(5, false)

df8:org.apache.spark.sql.DataFrame
NOMBRE_PERSONA:string
PREFIJO_1:string
TELEFONO_1:string
+--------------+---------+----------+
|NOMBRE_PERSONA|PREFIJO_1|TELEFONO_1|
+--------------+---------+----------+
|Brenden       |59       |9811935   |
|Carissa       |50       |9912937   |
|Fiona         |53       |9769557   |
|Allen         |51       |9733329   |
|Ebony         |52       |9091334   |
+--------------+---------+----------+
only showing top 5 rows


//Colocamos un alias
var df9 = dfJson.select(
    dfJson.col("PERSONA.NOMBRE_PERSONA"),
    dfJson.col("PERSONA.CONTACTO").getItem(0).getItem("PREFIJO").alias("PREFIJO_1"),
    dfJson.col("PERSONA.CONTACTO").getItem(0).getItem("TELEFONO").alias("TELEFONO_1"),
    dfJson.col("PERSONA.CONTACTO").getItem(1).getItem("PREFIJO").alias("PREFIJO_2"),
    dfJson.col("PERSONA.CONTACTO").getItem(1).getItem("TELEFONO").alias("TELEFONO_2"),
    dfJson.col("PERSONA.CONTACTO").getItem(2).getItem("PREFIJO").alias("PREFIJO_3"),
    dfJson.col("PERSONA.CONTACTO").getItem(2).getItem("TELEFONO").alias("TELEFONO_3"),
    dfJson.col("PERSONA.CONTACTO").getItem(3).getItem("PREFIJO").alias("PREFIJO_4"),
    dfJson.col("PERSONA.CONTACTO").getItem(3).getItem("TELEFONO").alias("TELEFONO_4")
)

//Mostramos la data
df9.show(5, false)

df9:org.apache.spark.sql.DataFrame
NOMBRE_PERSONA:string
PREFIJO_1:string
TELEFONO_1:string
PREFIJO_2:string
TELEFONO_2:string
PREFIJO_3:string
TELEFONO_3:string
PREFIJO_4:string
TELEFONO_4:string
+--------------+---------+----------+---------+----------+---------+----------+---------+----------+
|NOMBRE_PERSONA|PREFIJO_1|TELEFONO_1|PREFIJO_2|TELEFONO_2|PREFIJO_3|TELEFONO_3|PREFIJO_4|TELEFONO_4|
+--------------+---------+----------+---------+----------+---------+----------+---------+----------+
|Brenden       |59       |9811935   |53       |9423163   |null     |null      |null     |null      |
|Carissa       |50       |9912937   |54       |9046676   |55       |9874284   |58       |9746053   |
|Fiona         |53       |9769557   |59       |9754523   |52       |9063371   |55       |9301624   |
|Allen         |51       |9733329   |57       |9619332   |51       |9087416   |50       |9486747   |
|Ebony         |52       |9091334   |59       |9831571   |null     |null      |null     |null      |
+--------------+---------+----------+---------+----------+---------+----------+---------+----------+
only showing top 5 rows


// DBTITLE 1,5. Aplanando campos arrays con la función "explode"
//Definimos un identificador y el campo array que aplanaremos
var df10= dfJson.select(
    dfJson.col("PERSONA.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"), 
    f.explode(dfJson.col("PERSONA.CONTACTO")).alias("DATOS_CONTACTO")
)

//Mostramos la data
df10.show(20, false)

df10:org.apache.spark.sql.DataFrame
NOMBRE_PERSONA:string
DATOS_CONTACTO:struct
  PREFIJO:string
  TELEFONO:string

  +--------------+--------------+
|NOMBRE_PERSONA|DATOS_CONTACTO|
+--------------+--------------+
|Brenden       |{59, 9811935} |
|Brenden       |{53, 9423163} |
|Carissa       |{50, 9912937} |
|Carissa       |{54, 9046676} |
|Carissa       |{55, 9874284} |
|Carissa       |{58, 9746053} |
|Carissa       |{53, 9058704} |
|Fiona         |{53, 9769557} |
|Fiona         |{59, 9754523} |
|Fiona         |{52, 9063371} |
|Fiona         |{55, 9301624} |
|Fiona         |{56, 9770100} |
|Allen         |{51, 9733329} |
|Allen         |{57, 9619332} |
|Allen         |{51, 9087416} |
|Allen         |{50, 9486747} |
|Ebony         |{52, 9091334} |
|Ebony         |{59, 9831571} |
|Zachery       |{59, 9708669} |
|Zachery       |{52, 9751344} |
+--------------+--------------+
only showing top 20 rows


//Vemos el esquema
df10.printSchema()

root
 |-- NOMBRE_PERSONA: string (nullable = true)
 |-- DATOS_CONTACTO: struct (nullable = true)
 |    |-- PREFIJO: string (nullable = true)
 |    |-- TELEFONO: string (nullable = true)


//Estructuramos
var df11 = df10.select(
  df10.col("NOMBRE_PERSONA"),
  df10.col("DATOS_CONTACTO.PREFIJO"),
  df10.col("DATOS_CONTACTO.TELEFONO")
)

//Mostramos los datos
df11.show()

df11:org.apache.spark.sql.DataFrame
NOMBRE_PERSONA:string
PREFIJO:string
TELEFONO:string

+--------------+-------+--------+
|NOMBRE_PERSONA|PREFIJO|TELEFONO|
+--------------+-------+--------+
|       Brenden|     59| 9811935|
|       Brenden|     53| 9423163|
|       Carissa|     50| 9912937|
|       Carissa|     54| 9046676|
|       Carissa|     55| 9874284|
|       Carissa|     58| 9746053|
|       Carissa|     53| 9058704|
|         Fiona|     53| 9769557|
|         Fiona|     59| 9754523|
|         Fiona|     52| 9063371|
|         Fiona|     55| 9301624|
|         Fiona|     56| 9770100|
|         Allen|     51| 9733329|
|         Allen|     57| 9619332|
|         Allen|     51| 9087416|
|         Allen|     50| 9486747|
|         Ebony|     52| 9091334|
|         Ebony|     59| 9831571|
|       Zachery|     59| 9708669|
|       Zachery|     52| 9751344|
+--------------+-------+--------+
only showing top 20 rows


// DBTITLE 1,6. Realizando operaciones
//Para realizar operaciones, podemos aplicar las transformations clásicas o UDFs
//Sólo tenemos que navegar entre los campos y sub-campos donde las aplicaremos


//Por ejemplo, filtremos a las personas que tengan un prefijo telefonico de "51"
var dfPersona51 = df11.filter(df11.col("PREFIJO") === "51")

//Mostramos los datos
dfPersona51.show()

dfPersona51:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
NOMBRE_PERSONA:string
PREFIJO:string
TELEFONO:string

+--------------+-------+--------+
|NOMBRE_PERSONA|PREFIJO|TELEFONO|
+--------------+-------+--------+
|         Allen|     51| 9733329|
|         Allen|     51| 9087416|
|      Jennifer|     51| 9443174|
|     Sigourney|     51| 9058211|
|     Sigourney|     51| 9805215|
|        Quemby|     51| 9912326|
|          Omar|     51| 9993481|
|          null|     51| 9827831|
|         Keith|     51| 9954189|
|          Ross|     51| 9158266|
|          Ross|     51| 9255690|
|          Lila|     51| 9025379|
|       Illiana|     51| 9449297|
|          Igor|     51| 9972184|
|          Suki|     51| 9169144|
|          Suki|     51| 9145149|
|        Philip|     51| 9762104|
|      Tallulah|     51| 9303393|
|        Kylynn|     51| 9487893|
|          null|     51| 9251336|
+--------------+-------+--------+
only showing top 20 rows