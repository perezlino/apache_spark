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
dfPersona.show(truncate=false)

dfPersona:org.apache.spark.sql.DataFrame
ID:string
NOMBRE:string
TELEFONO:string
CORREO:string
FECHA_INGRESO:string
EDAD:integer
SALARIO:double
ID_EMPRESA:string

+---+---------+--------------+---------------------------------------+-------------+----+-------+----------+
|ID |NOMBRE   |TELEFONO      |CORREO                                 |FECHA_INGRESO|EDAD|SALARIO|ID_EMPRESA|
+---+---------+--------------+---------------------------------------+-------------+----+-------+----------+
|1  |Carl     |1-745-633-9145|arcu.Sed.et@ante.co.uk                 |2004-04-23   |32  |20095.0|5         |
|2  |Priscilla|155-2498      |Donec.egestas.Aliquam@volutpatnunc.edu |2019-02-17   |34  |9298.0 |2         |
|3  |Jocelyn  |1-204-956-8594|amet.diam@lobortis.co.uk               |2002-08-01   |27  |10853.0|3         |
|4  |Aidan    |1-719-862-9385|euismod.et.commodo@nibhlaciniaorci.edu |2018-11-06   |29  |3387.0 |10        |
|5  |Leandra  |839-8044      |at@pretiumetrutrum.com                 |2002-10-10   |41  |22102.0|1         |
|6  |Bert     |797-4453      |a.felis.ullamcorper@arcu.org           |2017-04-25   |70  |7800.0 |7         |
|7  |Mark     |1-680-102-6792|Quisque.ac@placerat.ca                 |2006-04-21   |52  |8112.0 |5         |
|8  |Jonah    |214-2975      |eu.ultrices.sit@vitae.ca               |2017-10-07   |23  |17040.0|5         |
|9  |Hanae    |935-2277      |eu@Nunc.ca                             |2003-05-25   |69  |6834.0 |3         |
|10 |Cadman   |1-866-561-2701|orci.adipiscing.non@semperNam.ca       |2001-05-19   |19  |7996.0 |7         |
|11 |Melyssa  |596-7736      |vel@vulputateposuerevulputate.net      |2008-10-14   |48  |4913.0 |8         |
|12 |Tanner   |1-739-776-7897|arcu.Aliquam.ultrices@sociis.com       |2011-05-10   |24  |19943.0|8         |
|13 |Trevor   |512-1955      |Nunc.quis.arcu@egestasa.org            |2010-08-06   |34  |9501.0 |5         |
|14 |Allen    |733-2795      |felis.Donec@necleo.org                 |2005-03-07   |59  |16289.0|2         |
|15 |Wanda    |359-6973      |Nam.nulla.magna@In.org                 |2005-08-21   |27  |1539.0 |5         |
|16 |Alden    |341-8522      |odio@morbitristiquesenectus.ca         |2006-12-05   |26  |3377.0 |2         |
|17 |Omar     |720-1543      |Phasellus.vitae.mauris@sollicitudin.net|2014-06-24   |60  |6851.0 |6         |
|18 |Owen     |1-167-335-7541|sociis@erat.com                        |2002-04-09   |34  |4759.0 |7         |
|19 |Laura    |1-974-623-2057|mollis@ornare.ca                       |2017-03-09   |70  |17403.0|4         |
|20 |Emery    |1-672-840-0264|at.nisi@vel.org                        |2004-02-27   |24  |18752.0|9         |
+---+---------+--------------+---------------------------------------+-------------+----+-------+----------+
only showing top 20 rows


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
dfTransaccion.show(truncate=false)

+----------+----------+------+----------+
|ID_PERSONA|ID_EMPRESA|MONTO |FECHA     |
+----------+----------+------+----------+
|26        |7         |2244.0|2021-01-23|
|24        |6         |1745.0|2021-01-23|
|1         |10        |238.0 |2021-01-23|
|19        |2         |3338.0|2021-01-23|
|95        |8         |2656.0|2021-01-23|
|37        |6         |3811.0|2021-01-23|
|40        |2         |297.0 |2021-01-23|
|27        |3         |2896.0|2021-01-23|
|65        |2         |4097.0|2021-01-23|
|21        |8         |2799.0|2021-01-23|
|71        |8         |3148.0|2021-01-23|
|71        |4         |2712.0|2021-01-23|
|34        |6         |513.0 |2021-01-23|
|71        |5         |1548.0|2021-01-23|
|81        |3         |1343.0|2021-01-23|
|90        |1         |4224.0|2021-01-23|
|87        |8         |1571.0|2021-01-23|
|82        |4         |3411.0|2021-01-23|
|61        |4         |871.0 |2021-01-23|
|48        |6         |783.0 |2021-01-23|
+----------+----------+------+----------+
only showing top 20 rows


// DBTITLE 1,3. Registro de dataframes como TempViews
//Registramos los dataframes como TempViews
dfPersona.createOrReplaceTempView("dfPersona")
dfTransaccion.createOrReplaceTempView("dfTransaccion")


//Verificamos que las TempViews se hayan creado
spark.sql("SHOW VIEWS").show(truncate=false)

+---------+-------------+-----------+--------------+
|namespace|viewName     |isTemporary|isMaterialized|
+---------+-------------+-----------+--------------+
|         |dfpersona    |true       |false         |
|         |dftransaccion|true       |false         |
+---------+-------------+-----------+--------------+


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
df1.show(truncate=false)

df1:org.apache.spark.sql.DataFrame
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:integer
ID_EMPRESA:string
MONTO:double
FECHA:string

+----------+--------------+------------+----------+------+----------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|ID_EMPRESA|MONTO |FECHA     |
+----------+--------------+------------+----------+------+----------+
|26        |Brenden       |33          |7         |2244.0|2021-01-23|
|24        |Amaya         |24          |6         |1745.0|2021-01-23|
|1         |Carl          |32          |10        |238.0 |2021-01-23|
|19        |Laura         |70          |2         |3338.0|2021-01-23|
|95        |Jayme         |58          |8         |2656.0|2021-01-23|
|37        |Inga          |41          |6         |3811.0|2021-01-23|
|40        |Ross          |67          |2         |297.0 |2021-01-23|
|27        |Alexander     |55          |3         |2896.0|2021-01-23|
|65        |Nehru         |34          |2         |4097.0|2021-01-23|
|21        |Carissa       |31          |8         |2799.0|2021-01-23|
|71        |Doris         |23          |8         |3148.0|2021-01-23|
|71        |Doris         |23          |4         |2712.0|2021-01-23|
|34        |Lila          |48          |6         |513.0 |2021-01-23|
|71        |Doris         |23          |5         |1548.0|2021-01-23|
|81        |Joy           |19          |3         |1343.0|2021-01-23|
|90        |Sigourney     |42          |1         |4224.0|2021-01-23|
|87        |Karly         |25          |8         |1571.0|2021-01-23|
|82        |Cally         |67          |4         |3411.0|2021-01-23|
|61        |Abel          |33          |4         |871.0 |2021-01-23|
|48        |Illiana       |18          |6         |783.0 |2021-01-23|
+----------+--------------+------------+----------+------+----------+
only showing top 20 rows


//Guardamos el dataframe como TempView
df1.createOrReplaceTempView("df1")


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
df2.show(truncate=false)

df2:org.apache.spark.sql.DataFrame
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:integer
ID_EMPRESA:string
MONTO:double
FECHA:string

+----------+--------------+------------+----------+------+----------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|ID_EMPRESA|MONTO |FECHA     |
+----------+--------------+------------+----------+------+----------+
|26        |Brenden       |33          |7         |2244.0|2021-01-23|
|1         |Carl          |32          |10        |238.0 |2021-01-23|
|19        |Laura         |70          |2         |3338.0|2021-01-23|
|95        |Jayme         |58          |8         |2656.0|2021-01-23|
|37        |Inga          |41          |6         |3811.0|2021-01-23|
|40        |Ross          |67          |2         |297.0 |2021-01-23|
|27        |Alexander     |55          |3         |2896.0|2021-01-23|
|65        |Nehru         |34          |2         |4097.0|2021-01-23|
|21        |Carissa       |31          |8         |2799.0|2021-01-23|
|34        |Lila          |48          |6         |513.0 |2021-01-23|
|90        |Sigourney     |42          |1         |4224.0|2021-01-23|
|82        |Cally         |67          |4         |3411.0|2021-01-23|
|61        |Abel          |33          |4         |871.0 |2021-01-23|
|72        |Tallulah      |46          |1         |1580.0|2021-01-23|
|26        |Brenden       |33          |5         |2628.0|2021-01-23|
|27        |Alexander     |55          |8         |1436.0|2021-01-23|
|15        |Wanda         |27          |5         |391.0 |2021-01-23|
|90        |Sigourney     |42          |2         |3483.0|2021-01-23|
|40        |Ross          |67          |4         |3342.0|2021-01-23|
|3         |Jocelyn       |27          |5         |722.0 |2021-01-23|
+----------+--------------+------------+----------+------+----------+
only showing top 20 rows

PARAM_EDAD: Int = 25


//Guardamos el dataframe como TempView
df2.createOrReplaceTempView("df2")


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
df3.show(truncate=false)

df3:org.apache.spark.sql.DataFrame
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:integer
ID_EMPRESA:string
MONTO:double
FECHA:string

+----------+--------------+------------+----------+------+----------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|ID_EMPRESA|MONTO |FECHA     |
+----------+--------------+------------+----------+------+----------+
|27        |Alexander     |55          |3         |2896.0|2021-01-23|
|61        |Abel          |33          |4         |871.0 |2021-01-23|
|27        |Alexander     |55          |8         |1436.0|2021-01-23|
|96        |Amos          |42          |4         |2887.0|2021-01-23|
|61        |Abel          |33          |9         |3141.0|2021-01-23|
|44        |Azalia        |27          |3         |1408.0|2021-01-23|
|14        |Allen         |59          |5         |3385.0|2021-01-23|
|4         |Aidan         |29          |6         |3216.0|2021-01-23|
|16        |Alden         |26          |5         |1887.0|2021-01-23|
|14        |Allen         |59          |6         |4368.0|2021-01-23|
|66        |Adrian        |46          |5         |4470.0|2021-01-23|
|4         |Aidan         |29          |6         |1857.0|2021-01-23|
|62        |Amelia        |35          |8         |1239.0|2021-01-23|
|66        |Adrian        |46          |3         |3023.0|2021-01-23|
|14        |Allen         |59          |8         |2880.0|2021-01-23|
|14        |Allen         |59          |1         |1137.0|2021-01-23|
|61        |Abel          |33          |5         |2967.0|2021-01-23|
|4         |Aidan         |29          |2         |4004.0|2021-01-23|
|35        |Aurora        |54          |3         |3546.0|2021-01-23|
|66        |Adrian        |46          |10        |718.0 |2021-01-23|
+----------+--------------+------------+----------+------+----------+
only showing top 20 rows


//Guardamos el dataframe como TempView
df3.createOrReplaceTempView("df3")


// DBTITLE 1,5. Escritura de resultante final
//Escribimos el dataframe de la resultante final en disco duro
df3.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", "|").save("dbfs:///FileStore/_spark/output/df3")


//Verificamos la ruta para ver si el archivo se escribió

%fs ls dbfs:///FileStore/_spark/output/df3

// _SUCCESS
// _committed_1598814422794687398
// _committed_2069103580117983878
// _committed_vacuum1081680840145144049
// _started_2069103580117983878
// part-00000-tid-2069103580117983878-f78ea023-225d-44b1-81ee-20ecbcd83a34-27-1-c000.csv
// part-00001-tid-2069103580117983878-f78ea023-225d-44b1-81ee-20ecbcd83a34-28-1-c000.csv
// part-00002-tid-2069103580117983878-f78ea023-225d-44b1-81ee-20ecbcd83a34-29-1-c000.csv


// DBTITLE 1,6. Verificamos
//Para verificar, leemos el directorio del dataframe en una variable
//Como solo quiero verificar que esta escrito, puedo omitir la definición del esquema
var df3Leido = spark.read.format("csv").option("header", "true").option("delimiter", "|").load("dbfs:///FileStore/_spark/output/df3")

//Mostramos los datos
df3Leido.show(truncate=false)

df3Leido:org.apache.spark.sql.DataFrame
ID_PERSONA:string
NOMBRE_PERSONA:string
EDAD_PERSONA:string
ID_EMPRESA:string
MONTO:string
FECHA:string

+----------+--------------+------------+----------+------+----------+
|ID_PERSONA|NOMBRE_PERSONA|EDAD_PERSONA|ID_EMPRESA|MONTO |FECHA     |
+----------+--------------+------------+----------+------+----------+
|27        |Alexander     |55          |3         |2896.0|2021-01-23|
|61        |Abel          |33          |4         |871.0 |2021-01-23|
|27        |Alexander     |55          |8         |1436.0|2021-01-23|
|96        |Amos          |42          |4         |2887.0|2021-01-23|
|61        |Abel          |33          |9         |3141.0|2021-01-23|
|44        |Azalia        |27          |3         |1408.0|2021-01-23|
|14        |Allen         |59          |5         |3385.0|2021-01-23|
|4         |Aidan         |29          |6         |3216.0|2021-01-23|
|16        |Alden         |26          |5         |1887.0|2021-01-23|
|14        |Allen         |59          |6         |4368.0|2021-01-23|
|66        |Adrian        |46          |5         |4470.0|2021-01-23|
|4         |Aidan         |29          |6         |1857.0|2021-01-23|
|62        |Amelia        |35          |8         |1239.0|2021-01-23|
|66        |Adrian        |46          |3         |3023.0|2021-01-23|
|14        |Allen         |59          |8         |2880.0|2021-01-23|
|14        |Allen         |59          |1         |1137.0|2021-01-23|
|61        |Abel          |33          |5         |2967.0|2021-01-23|
|4         |Aidan         |29          |2         |4004.0|2021-01-23|
|35        |Aurora        |54          |3         |3546.0|2021-01-23|
|66        |Adrian        |46          |10        |718.0 |2021-01-23|
+----------+--------------+------------+----------+------+----------+
only showing top 20 rows