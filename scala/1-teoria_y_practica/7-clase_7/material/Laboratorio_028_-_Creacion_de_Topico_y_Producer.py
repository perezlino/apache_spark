# Databricks notebook source
# DBTITLE 1,1. Creación de tópico
# MAGIC %sh ./kafka_2.12-3.2.3/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic transaccion --partitions 1 --replication-factor 1

# COMMAND ----------

# DBTITLE 1,2. Listar tópicos
# MAGIC %sh ./kafka_2.12-3.2.3/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# COMMAND ----------

# DBTITLE 1,3. Envío de mensaje con un Producer de consola
# MAGIC %sh echo "Hola mundo desde CONSOLA" | ./kafka_2.12-3.2.3/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic transaccion
