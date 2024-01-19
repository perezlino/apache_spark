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

// COMMAND ----------

// DBTITLE 1,2. Lectura
//Leemos el archivo de persona
var dfPersona = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        Array(
            StructField("ID", StringType, true),
            StructField("NOMBRE", StringType, true),
            StructField("TELEFONO", StringType, true),
            StructField("CORREO", StringType, true),
            StructField("FECHA_INGRESO", StringType, true),
            StructField("EDAD", IntegerType, true),
            StructField("SALARIO", DoubleType, true),
            StructField("ID_EMPRESA", StringType, true)
        )
    )
).load("dbfs:///FileStore/_spark/persona.data")

//Mostramos los datos
dfPersona.show()

// COMMAND ----------

// DBTITLE 1,3. Anti-patrón show
//Cuando estamos desarrollando, el show nos ayudará a ver si el proceso está siendo implementado de manera correcta
//Pero recordemos que el SHOW al ser un action re-ejecuta toda la cadena de procesos asociada al dataframe

//PASO 1: Agrupamos los datos según la edad
var df1 = dfPersona.groupBy(dfPersona.col("EDAD")).agg(
	f.count(dfPersona.col("EDAD")).alias("CANTIDAD"), 
	f.min(dfPersona.col("FECHA_INGRESO")).alias("FECHA_CONTRATO_MAS_RECIENTE"), 
	f.sum(dfPersona.col("SALARIO")).alias("SUMA_SALARIOS"), 
	f.max(dfPersona.col("SALARIO")).alias("SALARIO_MAYOR")
)

//Mostramos los datos
//TIEMPO DE PROCESAMIENTO: 1 HORA
df1.show()

// COMMAND ----------

//PASO 2: Filtramos por una EDAD
var df2 = df1.filter(df1.col("EDAD") > 35)

//Mostramos los datos
//Se vuelve a crear df1
//TIEMPO DE PROCESAMIENTO: 1HORA (df2) + 1HORA (df1) = 2 HORAS
df2.show()

// COMMAND ----------

//PASO 3: Filtramos por SUMA_SALARIOS
//Se vuelve a crear df1 y df2
var df3 = df2.filter(df2.col("SUMA_SALARIOS") > 20000)

//Mostramos los datos
//TIEMPO DE PROCESAMIENTO: 1HORA (df3) + 1HORA (df2) + 1HORA (df1) = 3 HORAS
df3.show()

// COMMAND ----------

//PASO 4: Filtramos por SALARIO_MAYOR
//Se vuelve a crear df1, df2 y df3
var dfResultado = df3.filter(df3.col("SALARIO_MAYOR") > 1000)

//Mostramos los datos
//TIEMPO DE PROCESAMIENTO: 1HORA (df4) + 1HORA (df3) + 1HORA (df2) + 1HORA (df1) = 4 HORAS
dfResultado.show()

// COMMAND ----------

//Si volvemos a ejecutar todo desde el principio el proceso demorará: 
// PASO 1 (1 HORA) + PASO 2 (2 HORAS) + PASO 3 (3 HORAS) + PASO 4 (4 HORAS) = 10 HORAS

// COMMAND ----------

// DBTITLE 1,4. Función "show" personalizada
//Una manera de evitar posibles errores es crear una función show personalizada

//Definimos una varible de control
var PARAM_SHOW_HABILITADO = false

//Importamos la librería que define el tipo de dato de un Dataframe
import org.apache.spark.sql.DataFrame

//Definimos la función
def show(df : DataFrame) = {
  if(PARAM_SHOW_HABILITADO == true){
    df.show()
  }
}

// COMMAND ----------

//Cuando implementemos cada paso del proeceso necesitaremos el show
//Pero cada vez que tengamos un output correcto, deberemos comentar el "show"
//De esta manera cuando el proceso esté listo, no ejecutará los show y no estará recreando toda la cadena de procesos

//PASO 1: Agrupamos los datos según la edad
var df1 = dfPersona.groupBy(dfPersona.col("EDAD")).agg(
	f.count(dfPersona.col("EDAD")).alias("CANTIDAD"), 
	f.min(dfPersona.col("FECHA_INGRESO")).alias("FECHA_CONTRATO_MAS_RECIENTE"), 
	f.sum(dfPersona.col("SALARIO")).alias("SUMA_SALARIOS"), 
	f.max(dfPersona.col("SALARIO")).alias("SALARIO_MAYOR")
)

//Mostramos los datos
show(df1)

//PASO 2: Filtramos por una EDAD
var df2 = df1.filter(df1.col("EDAD") > 35)

//Mostramos los datos
show(df2)

//PASO 3: Filtramos por SUMA_SALARIOS
var df3 = df2.filter(df2.col("SUMA_SALARIOS") > 20000)

//Mostramos los datos
show(df3)

//PASO 4: Filtramos por SALARIO_MAYOR
var dfResultado = df3.filter(df3.col("SALARIO_MAYOR") > 1000)

//Mostramos los datos
show(dfResultado)

// COMMAND ----------

// DBTITLE 1,5. Escritura
//El tunning no se aplica a la escritura

//Escribimos el dataframe de la resultante final en disco duro
dfResultado.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", "|").save("dbfs:///FileStore/_spark/output/dfResultado")
