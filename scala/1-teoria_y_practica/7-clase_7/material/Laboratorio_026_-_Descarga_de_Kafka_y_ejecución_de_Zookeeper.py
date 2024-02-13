# Databricks notebook source
# DBTITLE 1,1. Instalación de librerías de Kafka
#Librería para leer en streaming los datos desde KAFKA hacia SPARK
#
#groupId = org.apache.spark
#artifactId = spark-streaming-kafka-0-10_2.12
#version = 3.2.2
#
#org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.2

# COMMAND ----------

#Librería para procesamiento entre SPARK y KAFKA
#
#groupId = org.apache.spark
#artifactId = spark-sql-kafka-0-10_2.12
#version = 3.2.2
#
#org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2

# COMMAND ----------

# DBTITLE 1,2. Descarga de Kafka
# MAGIC %sh wget https://dlcdn.apache.org/kafka/3.2.3/kafka_2.12-3.2.3.tgz

# COMMAND ----------

# DBTITLE 1,3. Descompresión
# MAGIC %sh tar -xvf kafka_2.12-3.2.3.tgz

# COMMAND ----------

# DBTITLE 1,4. Iniciar Zookeeper
# MAGIC %sh ./kafka_2.12-3.2.3/bin/zookeeper-server-start.sh ./kafka_2.12-3.2.3/config/zookeeper.properties
