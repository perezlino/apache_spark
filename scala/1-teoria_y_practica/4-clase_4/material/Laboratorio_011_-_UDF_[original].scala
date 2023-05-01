// Databricks notebook source
//COPYRIGHT: BIG DATA ACADEMY [info@bigdataacademy.org]
//AUTHOR: ALONSO MELGAREJO [alonsoraulmgs@gmail.com]

// COMMAND ----------

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

// COMMAND ----------

// DBTITLE 1,2. Lectura de datos
//Leemos los datos
var dfJson = spark.read.format("json").load("dbfs:///FileStore/_spark/transacciones.json")

//Mostramos los datos
//Vemos que lo está truncando
dfJson.show()

// COMMAND ----------

//Mostramos los 20 primeros registros sin truncar el output en consola
dfJson.show(20, false)

// COMMAND ----------

//Pintamos el esquema
dfJson.printSchema()

// COMMAND ----------

// DBTITLE 1,3. Navegando entre campos y sub-campos
//Seleccionar algunos campos
var df1 = dfJson.select("EMPRESA", "PERSONA")

//Mostramos la data
df1.show(5, false)

// COMMAND ----------

//Seleccionar los subcampos
var df2 = dfJson.select("EMPRESA.ID_EMPRESA", "EMPRESA.NOMBRE_EMPRESA", "PERSONA.ID_PERSONA", "PERSONA.NOMBRE_PERSONA", "PERSONA.CONTACTO")

//Mostramos la data
df2.show(5, false)

// COMMAND ----------

//Navegamos por los sub-campos de PERSONA
var df3 = dfJson.select(
  dfJson.col("PERSONA.ID_PERSONA"), 
  dfJson.col("PERSONA.NOMBRE_PERSONA"), 
  dfJson.col("PERSONA.CONTACTO")
)

//Mostramos la data
df3.show(5, false)

// COMMAND ----------

//Nos enfocaremos en el sub-campo CONTACTO
var df4 = dfJson.select(dfJson.col("PERSONA.CONTACTO"))

//Mostramos la data
df4.show(5, false)

// COMMAND ----------

// DBTITLE 1,4. Navegando en campos arrays
//Obtenemos el primer elemento
var df5 = dfJson.select(dfJson.col("PERSONA.CONTACTO").getItem(0))

//Mostramos la data
df5.show(5, false)

// COMMAND ----------

//Colocamos un alias
var df6 = dfJson.select(dfJson.col("PERSONA.CONTACTO").getItem(0).alias("CONTACTO_1"))

//Mostramos la data
df6.show(5, false)

// COMMAND ----------

//Vemos el esquema
df6.printSchema()

// COMMAND ----------

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

// COMMAND ----------

//Colocamos un alias
var df8 = dfJson.select(
    dfJson.col("PERSONA.NOMBRE_PERSONA"),
    dfJson.col("PERSONA.CONTACTO").getItem(0).getItem("PREFIJO").alias("PREFIJO_1"),
    dfJson.col("PERSONA.CONTACTO").getItem(0).getItem("TELEFONO").alias("TELEFONO_1")
)

//Mostramos la data
df8.show(5, false)

// COMMAND ----------

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

// COMMAND ----------

// DBTITLE 1,5. Aplanando campos arrays con la función "explode"
//Definimos un identificador y el campo array que aplanaremos
var df10= dfJson.select(
    dfJson.col("PERSONA.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"), 
    f.explode(dfJson.col("PERSONA.CONTACTO")).alias("DATOS_CONTACTO")
)

//Mostramos la data
df10.show(20, false)

// COMMAND ----------

//Vemos el esquema
df10.printSchema()

// COMMAND ----------

//Estructuramos
var df11 = df10.select(
  df10.col("NOMBRE_PERSONA"),
  df10.col("DATOS_CONTACTO.PREFIJO"),
  df10.col("DATOS_CONTACTO.TELEFONO")
)

//Mostramos los datos
df11.show()

// COMMAND ----------

// DBTITLE 1,6. Realizando operaciones
//Para realizar operaciones, podemos aplicar las transformations clásicas o UDFs
//Sólo tenemos que navegar entre los campos y sub-campos donde las aplicaremos

// COMMAND ----------

//Por ejemplo, filtremos a las personas que tengan un prefijo telefonico de "51"
var dfPersona51 = df11.filter(df11.col("PREFIJO") === "51")

//Mostramos los datos
dfPersona51.show()
