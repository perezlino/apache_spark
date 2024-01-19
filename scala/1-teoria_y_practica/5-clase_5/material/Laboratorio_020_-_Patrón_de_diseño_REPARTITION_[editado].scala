// Databricks notebook source


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

//Reserva de recursos computacionales
import org.apache.spark.sql.SparkSession

//Importamos la librería que define el tipo de dato de un Dataframe
import org.apache.spark.sql.DataFrame


// DBTITLE 1,2. Lectura
//Leemos el archivo indicando el esquema
var dfTransaccion = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        Array(
          StructField("ID_PERSONA", StringType, true),
          StructField("ID_EMPRESA", StringType, true),
          StructField("MONTO", DoubleType, true),
          StructField("FECHA", StringType, true)
        )
    )
).load("dbfs:///FileStore/_spark/DATA_TRANSACCION.txt")

//Mostramos los datos
dfTransaccion.show()

+----------+----------+------+----------+
|ID_PERSONA|ID_EMPRESA| MONTO|     FECHA|
+----------+----------+------+----------+
|        18|         3|1383.0|2018-01-21|
|        30|         6|2331.0|2018-01-21|
|        47|         2|2280.0|2018-01-21|
|        28|         1| 730.0|2018-01-21|
|        91|         4|3081.0|2018-01-21|
|        74|         8|2409.0|2018-01-21|
|        41|         2|3754.0|2018-01-22|
|        42|         9|4079.0|2018-01-22|
|        24|         6|4475.0|2018-01-22|
|        67|         9| 561.0|2018-01-22|
|         9|         4|3765.0|2018-01-22|
|        97|         3|3669.0|2018-01-22|
|        91|         5|3497.0|2018-01-22|
|        61|         3| 735.0|2018-01-23|
|        15|         5| 367.0|2018-01-23|
|        20|         9|2039.0|2018-01-23|
|        11|         4| 719.0|2018-01-23|
|        36|         2|2659.0|2018-01-23|
|        12|         4| 467.0|2018-01-23|
|        38|         9|2411.0|2018-01-23|
+----------+----------+------+----------+
only showing top 20 rows


// DBTITLE 1,3. Cálculo de particiones en un dataframe
//Averiguar el número de particiones de un dataframe
var particionesActuales = dfTransaccion.rdd.getNumPartitions
println(particionesActuales)

2
particionesActuales: Int = 2


//Cada partición debe tener 100 mil registros
//En total el dataframe tiene 235040, necesitaremos 3 particiones
var cantidadDeRegistros = dfTransaccion.count()
println(cantidadDeRegistros)

235040
cantidadDeRegistros: Long = 235040


var numeroDeParticiones = (cantidadDeRegistros / 100000.0).ceil.toInt
println(numeroDeParticiones)

3
numeroDeParticiones: Int = 3


// DBTITLE 1,4. Reparticionamiento
//Llamamos a la función de reparticionamiento
dfTransaccion = dfTransaccion.repartition(3)

var particionesActuales = dfTransaccion.rdd.getNumPartitions
println(particionesActuales)

3
particionesActuales: Int = 3


//Adicionalmente, si el nuevo número de particiones es menor al número actual de particiones tenemos una función optimizada para reducir particiones
dfTransaccion = dfTransaccion.coalesce(3)

var particionesActuales = dfTransaccion.rdd.getNumPartitions
println(particionesActuales)

3
particionesActuales: Int = 3


// DBTITLE 1,5. Función utilitaria de reparticionamiento
//Definimos el número de registros por partición
var REGISTROS_POR_PARTICION = 100000

//Función de reparticionamiento
def reparticionar(df : DataFrame) : DataFrame = {
  var dfReparticionado : DataFrame = null
  
  //Obtenemos el número de particiones actuales
  var numeroDeParticionesActuales = df.rdd.getNumPartitions
  
  //Obtenemos la cantidad de registros del dataframe
  var cantidadDeRegistros = df.count()
  
  //Obtenemos el nuevo número de particiones
  var nuevoNumeroDeParticiones = (cantidadDeRegistros / (REGISTROS_POR_PARTICION *1.0)).ceil.toInt
  
  //Reparticionamos
  print("Reparticionando a "+nuevoNumeroDeParticiones+ " particiones...")
  if(nuevoNumeroDeParticiones > numeroDeParticionesActuales){
    dfReparticionado = df.repartition(nuevoNumeroDeParticiones)
  }else{
    dfReparticionado = df.coalesce(nuevoNumeroDeParticiones)
  }
  println(", reparticionado!")
  
  return dfReparticionado
}


//Ejemplo de uso

//Leemos el archivo indicando el esquema
var dfTransaccion = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        Array(
          StructField("ID_PERSONA", StringType, true),
          StructField("ID_EMPRESA", StringType, true),
          StructField("MONTO", DoubleType, true),
          StructField("FECHA", StringType, true)
        )
    )
).load("dbfs:///FileStore/_spark/DATA_TRANSACCION.txt")

//Reparticionamos
dfTransaccion = reparticionar(dfTransaccion)

//Mostramos los datos
dfTransaccion.show()
