# Databricks notebook source
# DBTITLE 1,1. Creación de tópico
# MAGIC %sh ./kafka_2.12-3.2.3/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic compras --partitions 1 --replication-factor 1

# COMMAND ----------

# DBTITLE 1,2. Listar tópicos
# MAGIC %sh ./kafka_2.12-3.2.3/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# COMMAND ----------

# DBTITLE 1,3. Creación de Consumer de consola
# MAGIC %sh ./kafka_2.12-3.2.3/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic compras
