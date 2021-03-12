package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBascis extends App{
    // DataFrames are a Distributed(through multiple nodes) collection of rows conforming to a schema
    // Each nodes receives the schema of the DF and a few rows
    // They are runtime and lazy. Spark will only load the DataFrame if an action is perfomed on it
    // Partition = splits the data into files and distributes between nodes in the cluster
    // This impacts the processing parallelism
    // They are Immutable, create other DFs via transformations

    //Transformation
        // Narrow = One input partition contributes to at most one output partition(e.g. map)
        // Wide = Input partitions (one or more) crate manu output partitions(e.g. sort)

    // Shuffle = Data exchange between cluster nodes (e.g. sort)
        // Occurs in wide transformations

    // Transformations VS Actions
        // Transformations describe how new DFs are obtained (e.g. map)
        // Actions actually start executing Spark code (e.g. count,show)



    // Creating a Spark session
    val spark = SparkSession.builder()
      .appName("DataFrames Basics")
      .config("spark.master","local")
      .getOrCreate()

    // Reading a DF
    val firstDF = spark.read
      .format("json")
      .option("inferSchema","true")
      .load("src/main/resources/data/cars.json")

    // Showing a DF
    firstDF.show()
    firstDF.printSchema()
    firstDF.take(10).foreach(println)           //Printa cada linha com um array heterogêneo

    // Spark types are case objects use to describe schemas
    val longtype = LongType

    // Schema
//    val carSchema = StructType(
//        Array(
//            StructField("Name",StringType, nullable = true)                                 // StructField descibres a column
//        )
//    )

    val carsSchema = StructType(Array(
        StructField("Name", StringType),
        StructField("Miles_per_Gallon", DoubleType),
        StructField("Cylinders", LongType),
        StructField("Displacement", DoubleType),
        StructField("Horsepower", LongType),
        StructField("Weight_in_lbs", LongType),
        StructField("Acceleration", DoubleType),
        StructField("Year", StringType),
        StructField("Origin", StringType)
    ))

    // Obtain schema from existing DataFrame
    val carsDFSchema = firstDF.schema

    // read a DF with your schema
    val carsDF = spark.read
      .format("json")
      .schema(carsSchema)
      .load("src/main/resources/data/cars.json")

    carsDF.printSchema()

    // Create rows by hand
    // You can put anything in the row apply method
    val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

    //create DF from tuples
    val cars = Seq(
        ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
        ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
        ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
        ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
        ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
        ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
        ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
        ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
        ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
        ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
    )

    val manualCarsDF = spark.createDataFrame(cars)    // schema auto-inferred

    // Schemas are only applied to dataframes, not rows

    // Create DFs with implicits
    import spark.implicits._                    // To importando da nossa sparkSession
    val manualCarsDFWithimplicits = cars.toDF("Name","MPG","Cylinders","Displacement","HP","Weight","Acceleration","Year","Country")

    manualCarsDF.printSchema()
    manualCarsDFWithimplicits.printSchema()

    // Exercises

    val smartPhones = Seq(
        ("Apple","12 Max",5.6,30),
        ("Samsung","S13",6.4,34),
        ("ASUS","Zenfone XYZ",6.0,30)
    )

    val smartPhonesDF = smartPhones.toDF("Make","Model","Screen Dimension","Camera Megapixels")
    smartPhonesDF.show()
    smartPhonesDF.printSchema()

    val movies = spark.read
      .format("json")
      .option("inferSchema","true")
      .load("src/main/resources/data/movies.json")

    movies.show()
    movies.printSchema()
    println("Número de linhas = " + movies.count())                  //Número de linhas
    println("Número de colunas = " + movies.columns.length)





}
