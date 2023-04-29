// Databricks notebook source
//COPYRIGHT: ALFONSO PEREZ [perezlino@gmail.com]
//AUTHOR: ALFONSO PEREZ [perezlino@gmail.com]

// COMMAND ----------

// DBTITLE 1,1. Librerías
//Objetos para definir la metadata
import org.apache.spark.sql.types.{StructType, StructField}

//Importamos los tipos de datos que usaremos
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}

//Podemos importar todos los utilitarios con la siguiente sentencia
import org.apache.spark.sql.types._

// COMMAND ----------

// DBTITLE 1,2. Lectura de dataframes
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

//Vamos a colocar el contenido de todos los archivos de la carpeta en un dataframe
//Solo deberemos indicar la ruta asociada
//Para este ejemplo, el archivo tiene un delimitador de coma ","
var dfTransaccion = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(
    StructType(
        Array(
            StructField("ID_PERSONA", StringType, true),
            StructField("ID_EMPRESA", StringType, true),
            StructField("MONTO", DoubleType, true),
            StructField("FECHA", StringType, true)
        )
    )
).load("dbfs:///FileStore/_spark/transaccion")

//Vemos el contenido
dfTransaccion.show()

// COMMAND ----------

// DBTITLE 1,3. Registro de dataframes como TempViews
//Registramos los dataframes como TempViews
dfPersona.createOrReplaceTempView("dfPersona")
dfTransaccion.createOrReplaceTempView("dfTransaccion")

// COMMAND ----------

//Verificamos que las TempViews se hayan creado
spark.sql("SHOW VIEWS").show()

// COMMAND ----------

// DBTITLE 1,4. Procesamos con SQL
//PASO 1: Agregaremos los nombres y edades de las personas que realizaron transacciones
var df1 = spark.sql("""
SELECT
  T.ID_PERSONA,
  P.NOMBRE NOMBRE_PERSONA,
  P.EDAD EDAD_PERSONA,
  T.ID_EMPRESA,
  T.MONTO,
  T.FECHA
FROM
  dftransaccion T
    JOIN dfpersona P ON T.ID_PERSONA = P.ID
""")

//Mostramos los datos
df1.show()

//Guardamos el dataframe como TempView
df1.createOrReplaceTempView("df1")

// COMMAND ----------

//PASO 2: Filtramos por edad

//Crearemos una variable para filtrar por edad
var PARAM_EDAD = 25

//Procesamos
var df2 = spark.sql(f"""
SELECT
  T.ID_PERSONA,
  T.NOMBRE_PERSONA,
  T.EDAD_PERSONA,
  T.ID_EMPRESA,
  T.MONTO,
  T.FECHA
FROM
  df1 T
WHERE
  T.EDAD_PERSONA > $PARAM_EDAD%s
""")

//Vemos el resultado
df2.show()

//Guardamos el dataframe como TempView
df2.createOrReplaceTempView("df2")

// COMMAND ----------

//PASO 3: Nos quedamos con las transacciones hechas por personas cuya inicial es la letra A

//Crearemos una variable para filtrar por la inicial
var PARAM_INICIAL = "A"

//Procesamos
var df3 = spark.sql(f"""
SELECT
  T.ID_PERSONA,
  T.NOMBRE_PERSONA,
  T.EDAD_PERSONA,
  T.ID_EMPRESA,
  T.MONTO,
  T.FECHA
FROM
  df2 T
WHERE
  SUBSTRING(T.NOMBRE_PERSONA, 0, 1) = '$PARAM_INICIAL%s'
""")

//Vemos el resultado
df3.show()

//Guardamos el dataframe como TempView
df3.createOrReplaceTempView("df3")

// COMMAND ----------

// DBTITLE 1,5. Escritura de resultante final
//Escribimos el dataframe de la resultante final en disco duro
df3.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", "|").save("dbfs:///FileStore/_spark/output/df3")

// COMMAND ----------

//Verificamos la ruta para ver si el archivo se escribió

// COMMAND ----------

// MAGIC %fs ls dbfs:///FileStore/output/df3

// COMMAND ----------

// DBTITLE 1,6. Verificamos
//Para verificar, leemos el directorio del dataframe en una variable
//Como solo quiero verificar que esta escrito, puedo omitir la definición del esquema
var df3Leido = spark.read.format("csv").option("header", "true").option("delimiter", "|").load("dbfs:///FileStore/_spark/output/df3")

//Mostramos los datos
df3Leido.show()
