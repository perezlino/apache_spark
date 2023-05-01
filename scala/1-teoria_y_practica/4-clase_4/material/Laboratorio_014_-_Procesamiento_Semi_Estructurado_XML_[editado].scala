// Databricks notebook source
//COPYRIGHT: ALFONSO PEREZ [perezlino@gmail.com]
//AUTHOR: ALFONSO PEREZ [perezlino@gmail.com]


// DBTITLE 1,1. Instalación de librería para procesamiento XML
//Para el caso de la librería de XMLs, la librería es:
//
//groupId: com.databricks
//artifactId: spark-xml_2.12
//version: 0.14.0
//
//com.databricks:spark-xml_2.12:0.14.0


// DBTITLE 1,2. Lectura de datos
//Leemos los datos
var dfXml = spark.read.format("xml").option("rootTag", "root").option("rowTag", "element").load("dbfs:///FileStore/_spark/transacciones.xml")


//Vemos el esquema
dfXml.printSchema()

root
 |-- EMPRESA: struct (nullable = true)
 |    |-- ID_EMPRESA: long (nullable = true)
 |    |-- NOMBRE_EMPRESA: string (nullable = true)
 |-- PERSONA: struct (nullable = true)
 |    |-- CONTACTO: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- PREFIJO: long (nullable = true)
 |    |    |    |-- TELEFONO: long (nullable = true)
 |    |-- EDAD: long (nullable = true)
 |    |-- ID_PERSONA: long (nullable = true)
 |    |-- NOMBRE_PERSONA: string (nullable = true)
 |    |-- SALARIO: double (nullable = true)
 |-- TRANSACCION: struct (nullable = true)
 |    |-- FECHA: string (nullable = true)
 |    |-- MONTO: double (nullable = true)



//Mostramos los datos
dfXml.show(20, false)

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