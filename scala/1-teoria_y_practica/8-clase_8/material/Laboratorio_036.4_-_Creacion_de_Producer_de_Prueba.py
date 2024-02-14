# Databricks notebook source
# DBTITLE 1,1. Creación de Consumer de consola
# MAGIC %sh echo "Hola mundo desde CONSOLA" | ./kafka_2.12-3.2.3/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic compras
