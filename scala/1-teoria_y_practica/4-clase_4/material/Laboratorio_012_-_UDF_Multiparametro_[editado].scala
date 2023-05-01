// Databricks notebook source
//COPYRIGHT: ALFONSO PEREZ [perezlino@gmail.com]
//AUTHOR: ALFONSO PEREZ [perezlino@gmail.com]


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


// DBTITLE 1,2. Lectura de datos
//Leemos el archivo de cliente
var dfCliente = spark.read.format("csv").option("header", "true").option("separator", ",").schema(
    StructType(
        Array(
            StructField("ID_CLIENTE", StringType, true),
            StructField("SEXO", StringType, true),
            StructField("EDAD", IntegerType, true),
            StructField("ESTATURA", DoubleType, true),
            StructField("ESTADO_CIVIL", StringType, true),
            StructField("TELEFONO", StringType, true)
        )
    )
).load("dbfs:///FileStore/_spark/CLIENTE.csv")

dfCliente.printSchema()

root
 |-- ID_CLIENTE: string (nullable = true)
 |-- SEXO: string (nullable = true)
 |-- EDAD: integer (nullable = true)
 |-- ESTATURA: double (nullable = true)
 |-- ESTADO_CIVIL: string (nullable = true)
 |-- TELEFONO: string (nullable = true)


//Mostramos los datos
dfCliente.show()

+----------+---------+----+--------+------------+--------+
|ID_CLIENTE|     SEXO|EDAD|ESTATURA|ESTADO_CIVIL|TELEFONO|
+----------+---------+----+--------+------------+--------+
|         1|MASCULINO|  36|    1.57|      CASADO| 1403321|
|         2|MASCULINO|  58|    1.81|      CASADO| 4030041|
|         3|MASCULINO|  75|    1.59|      CASADO| 9285459|
|         4|MASCULINO|  70|    1.58|      CASADO| 3193731|
|         5|MASCULINO|  49|     1.7|      CASADO| 7172962|
|         6|MASCULINO|  45|    1.78|     SOLTERO| 2387729|
|         7| FEMENINO|  69|    1.59|     SOLTERO| 3388494|
|         8|MASCULINO|  65|    1.81|     SOLTERO| 1423886|
|         9| FEMENINO|  64|    1.75|     SOLTERO| 7068124|
|        10|MASCULINO|  36|    1.81|      CASADO| 8488374|
|        11|MASCULINO|  31|    1.61|     SOLTERO| 2790891|
|        12|MASCULINO|  40|    1.61|      CASADO| 3123359|
|        13| FEMENINO|  35|    1.61|      CASADO| 6416244|
|        14| FEMENINO|  53|     1.7|     SOLTERO| 7078692|
|        15| FEMENINO|  41|    1.55|     SOLTERO| 3538001|
|        16| FEMENINO|  25|    1.57|     SOLTERO| 3687570|
|        17|MASCULINO|  76|    1.54|     SOLTERO| 5855749|
|        18| FEMENINO|  29|     1.8|     SOLTERO| 3701447|
|        19|MASCULINO|  26|    1.78|     SOLTERO| 3157694|
|        20|MASCULINO|  77|    1.75|      CASADO| 8184776|
+----------+---------+----+--------+------------+--------+
only showing top 20 rows


// DBTITLE 1,3. Implementación de función
//Funcion para calcular el salario anual
def clasificarCliente(sexo : String, edad : Integer) : String = {
  //Aquí colocaremos la respuesta
  var respuesta : String = ""
  
  if(sexo == "MASCULINO"){
    if(edad < 30){
      respuesta = "REGULAR"
    }else if(edad >= 30 && edad < 50){
      respuesta = "TOP"
    }else{
      respuesta = "PREMIUM"
    }
  }else{
    if(edad < 25){
      respuesta = "REGULAR"
    }else if(edad >= 25 && edad < 40){
      respuesta = "TOP"
    }else{
      respuesta = "PREMIUM"
    }
  }
  
  //Devolvemos el valor
  return respuesta
}


// DBTITLE 1,4. Definición del UDF
//Creamos la función personalizada
var udfClasificarCliente = udf(
  (
    sexo : String, 
    edad : Integer
  ) => clasificarCliente(
    sexo, 
    edad
  )
)


//Registramos el UDF
spark.udf.register("udfClasificarCliente", udfClasificarCliente)


// DBTITLE 1,5. Uso del UDF
//Aplicamos la función
var df1 = dfCliente.select(
  dfCliente.col("ID_CLIENTE"),
  dfCliente.col("TELEFONO").alias("CONTACTO_CLIENTE"),
  udfClasificarCliente(
    dfCliente.col("SEXO"), 
    dfCliente.col("EDAD")
  ).alias("CATEGORIA_CLIENTE")
)


//Vemos los datos
df1.show()

+----------+----------------+-----------------+
|ID_CLIENTE|CONTACTO_CLIENTE|CATEGORIA_CLIENTE|
+----------+----------------+-----------------+
|         1|         1403321|              TOP|
|         2|         4030041|          PREMIUM|
|         3|         9285459|          PREMIUM|
|         4|         3193731|          PREMIUM|
|         5|         7172962|              TOP|
|         6|         2387729|              TOP|
|         7|         3388494|          PREMIUM|
|         8|         1423886|          PREMIUM|
|         9|         7068124|          PREMIUM|
|        10|         8488374|              TOP|
|        11|         2790891|              TOP|
|        12|         3123359|              TOP|
|        13|         6416244|              TOP|
|        14|         7078692|          PREMIUM|
|        15|         3538001|          PREMIUM|
|        16|         3687570|              TOP|
|        17|         5855749|          PREMIUM|
|        18|         3701447|              TOP|
|        19|         3157694|          REGULAR|
|        20|         8184776|          PREMIUM|
+----------+----------------+-----------------+
only showing top 20 rows


// DBTITLE 1,6. Uso del UDF con withcolumn
//Aplicamos el UDF
var df2 = dfCliente.withColumn("CATEGORIA_CLIENTE", udfClasificarCliente(
    dfCliente.col("SEXO"), 
    dfCliente.col("EDAD")
  )
)

//Mostramos los datos
df2.show()

df2:org.apache.spark.sql.DataFrame
ID_CLIENTE:string
SEXO:string
EDAD:integer
ESTATURA:double
ESTADO_CIVIL:string
TELEFONO:string
CATEGORIA_CLIENTE:string

+----------+---------+----+--------+------------+--------+-----------------+
|ID_CLIENTE|     SEXO|EDAD|ESTATURA|ESTADO_CIVIL|TELEFONO|CATEGORIA_CLIENTE|
+----------+---------+----+--------+------------+--------+-----------------+
|         1|MASCULINO|  36|    1.57|      CASADO| 1403321|              TOP|
|         2|MASCULINO|  58|    1.81|      CASADO| 4030041|          PREMIUM|
|         3|MASCULINO|  75|    1.59|      CASADO| 9285459|          PREMIUM|
|         4|MASCULINO|  70|    1.58|      CASADO| 3193731|          PREMIUM|
|         5|MASCULINO|  49|     1.7|      CASADO| 7172962|              TOP|
|         6|MASCULINO|  45|    1.78|     SOLTERO| 2387729|              TOP|
|         7| FEMENINO|  69|    1.59|     SOLTERO| 3388494|          PREMIUM|
|         8|MASCULINO|  65|    1.81|     SOLTERO| 1423886|          PREMIUM|
|         9| FEMENINO|  64|    1.75|     SOLTERO| 7068124|          PREMIUM|
|        10|MASCULINO|  36|    1.81|      CASADO| 8488374|              TOP|
|        11|MASCULINO|  31|    1.61|     SOLTERO| 2790891|              TOP|
|        12|MASCULINO|  40|    1.61|      CASADO| 3123359|              TOP|
|        13| FEMENINO|  35|    1.61|      CASADO| 6416244|              TOP|
|        14| FEMENINO|  53|     1.7|     SOLTERO| 7078692|          PREMIUM|
|        15| FEMENINO|  41|    1.55|     SOLTERO| 3538001|          PREMIUM|
|        16| FEMENINO|  25|    1.57|     SOLTERO| 3687570|              TOP|
|        17|MASCULINO|  76|    1.54|     SOLTERO| 5855749|          PREMIUM|
|        18| FEMENINO|  29|     1.8|     SOLTERO| 3701447|              TOP|
|        19|MASCULINO|  26|    1.78|     SOLTERO| 3157694|          REGULAR|
|        20|MASCULINO|  77|    1.75|      CASADO| 8184776|          PREMIUM|
+----------+---------+----+--------+------------+--------+-----------------+
only showing top 20 rows


//¿Negocio realmente necesita todas esas columnas?, recordemos que desperdiciamos uso de memoria RAM


// DBTITLE 1,7. Uso del UDF con SQL
//Luego de definir el UDF, lo hemos registrado de esta manera
spark.udf.register("udfClasificarCliente", udfClasificarCliente)


//Al hacer esto, podemos usarlo dentro de SPARK SQL

//Vamos a registrar al "dfCliente" como una vista temporal
dfCliente.createOrReplaceTempView("dfCliente")


//Procesémoslo con SPARK SQL haciendo uso de nuestro UDF
var df3 = spark.sql("""
SELECT
  ID_CLIENTE,
  TELEFONO CONTACTO_CLIENTE,
  udfClasificarCliente(SEXO, EDAD) CATEGORIA_CLIENTE
FROM
  dfCliente
""")

//Mostramos los datos
df3.show()

df3:org.apache.spark.sql.DataFrame
ID_CLIENTE:string
CONTACTO_CLIENTE:string
CATEGORIA_CLIENTE:string

+----------+----------------+-----------------+
|ID_CLIENTE|CONTACTO_CLIENTE|CATEGORIA_CLIENTE|
+----------+----------------+-----------------+
|         1|         1403321|              TOP|
|         2|         4030041|          PREMIUM|
|         3|         9285459|          PREMIUM|
|         4|         3193731|          PREMIUM|
|         5|         7172962|              TOP|
|         6|         2387729|              TOP|
|         7|         3388494|          PREMIUM|
|         8|         1423886|          PREMIUM|
|         9|         7068124|          PREMIUM|
|        10|         8488374|              TOP|
|        11|         2790891|              TOP|
|        12|         3123359|              TOP|
|        13|         6416244|              TOP|
|        14|         7078692|          PREMIUM|
|        15|         3538001|          PREMIUM|
|        16|         3687570|              TOP|
|        17|         5855749|          PREMIUM|
|        18|         3701447|              TOP|
|        19|         3157694|          REGULAR|
|        20|         8184776|          PREMIUM|
+----------+----------------+-----------------+
only showing top 20 rows