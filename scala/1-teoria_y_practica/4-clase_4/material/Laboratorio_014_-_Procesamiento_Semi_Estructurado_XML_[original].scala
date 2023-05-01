// Databricks notebook source
//COPYRIGHT: BIG DATA ACADEMY [info@bigdataacademy.org]
//AUTHOR: ALONSO MELGAREJO [alonsoraulmgs@gmail.com]

// COMMAND ----------

// DBTITLE 1,1. Instalación de librería para procesamiento XML
//Para el caso de la librería de XMLs, la librería es:
//
//groupId: com.databricks
//artifactId: spark-xml_2.12
//version: 0.14.0
//
//com.databricks:spark-xml_2.12:0.14.0

// COMMAND ----------

// DBTITLE 1,2. Lectura de datos
//Leemos los datos
var dfXml = spark.read.format("xml").option("rootTag", "root").option("rowTag", "element").load("dbfs:///FileStore/_spark/transacciones.xml")

// COMMAND ----------

//Vemos el esquema
dfXml.printSchema()

// COMMAND ----------

//Mostramos los datos
dfXml.show(20, false)
