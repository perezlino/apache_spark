// Databricks notebook source


// DBTITLE 1,1. Librerías
//Utilitario para enviar datos a Kafka
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

//Los datos enviados a Kafka deben binarizarse con un serializador
import org.apache.kafka.common.serialization.StringSerializer

//Permite configurar propiedades de objetos
import java.util.Properties

//Funciones utilitarias
import org.apache.spark.sql.{functions => f}



// DBTITLE 1,2. Función utilitaria
//Función utilitaria para enviar un registro al tópico
def writeDataToTopic(clusterKafka : String, topico : String, data : String) : Unit = {
  //Creamos una variable de propiedades
  var configuracion: Properties = new Properties()

  //Configuramos la dirección del clúster Kafka
  configuracion.put("bootstrap.servers", clusterKafka)

  //Indicamos el tipo de dato para la clave
  configuracion.put("key.serializer", classOf[StringSerializer].getName)

  //Indicamos el tipo de dato para el valor
  configuracion.put("value.serializer", classOf[StringSerializer].getName)

  //Instanciamos el producer indicando el tipo de dato de la clave, el valor y la configuración
  var producer = new KafkaProducer[String, String](configuracion)
  
  //Enviamos el registro a Kafka, indicando el tipo de dato de la clave, el valor, el tópico en donde se escribe y el mensaje
  producer.send(new ProducerRecord[String, String](topico, data))
  producer.flush()
}



// DBTITLE 1,3. Lectura de N registros al azar de archivo JSON
//Leemos el archivo JSON
var dfJson = spark.read.format("json").option("multiLine", "false").load("dbfs:///FileStore/_spark/transacciones.json")

//Convertirmos el dataframe de Spark en un Array de JSON
var arrayJson = dfJson.toJSON.collect().toList

//Librería para seleccionar elementos aleatorios
import scala.util.Random

//Obtenemos 10 registros al azar
var arrayJsonAleatorio = Random.shuffle(arrayJson).take(10)



//Vemos el contenido
println(arrayJsonAleatorio)



//Vemos un registro
println(arrayJsonAleatorio(0))



//Cambiar el valor por la IP asignada
var clusterKafka = "localhost:9092"



//Lo escribimos en el tópico
writeDataToTopic(clusterKafka, "transaccion", arrayJsonAleatorio(0))



// DBTITLE 1,4. Función utilitaria para ingesta emulada
//Leemos el archivo JSON
var dfJson = spark.read.format("json").option("multiLine", "false").load("dbfs:///FileStore/_spark/transacciones.json")

//Convertirmos el dataframe de Spark en un Array de JSON
var arrayJson = dfJson.toJSON.collect().toList

//Creamos una función utilitaria
def readDataFromSource() : List[String] = {
  //Obtenemos 10 registros al azar
  var arrayJsonAleatorio = Random.shuffle(arrayJson).take(10)
  
  return arrayJsonAleatorio
}



//Bucle iterativo
for(i <- 1 to 10000){
  //Imprimimos el número de iteración
  println("Iteración: "+i)
  
  //Leemos la fuente de datos
  var dataFromSource = readDataFromSource()
  
  //Escribimos los datos en el topico
  for(transaccion <- dataFromSource){
    writeDataToTopic(clusterKafka, "transaccion", transaccion)
  }
  
  //Dormimos el bucle por un segundo
  Thread.sleep(1000)
}
