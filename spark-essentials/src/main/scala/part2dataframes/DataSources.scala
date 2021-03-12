package part2dataframes

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {
    val spark = SparkSession.builder()
      .appName("Data Sources and Formats")
      .config("spark.master","local")
      .getOrCreate()

//    val carsSchema = StructType(Array(
//        StructField("Name", StringType),
//        StructField("Miles_per_Gallon", DoubleType),
//        StructField("Cylinders", LongType),
//        StructField("Displacement", DoubleType),
//        StructField("Horsepower", LongType),
//        StructField("Weight_in_lbs", LongType),
//        StructField("Acceleration", DoubleType),
//        StructField("Year", DateType),
//        StructField("Origin", StringType)
//    ))
//
//    /*
//        Reading a DF:
//        - format
//        - schema or inferSchema = true
//        - zero or more options
//     */
//
//
//    // Mode decides what spark should do in case
//    // it encounters a mal formed record, for example, if it's not conformed with the schema
//    val carsDF = spark.read
//      .format("json")
//      .schema(carsSchema)               // Enforce a schema (recomended)
//      .option("mode","failFast")        // failFast throws an exception, dropMalformed will drop faulty rows, permissive(default)
//      .option("path","src/main/resources/data/cars.json")
//      .load()
//
//    carsDF.printSchema()
//
//    // Alternative reading with options map
//    val carsDFsWithOptionMap = spark.read
//      .format("json")
//      .options(Map (
//          "mode" -> "failFast",
//          "path" -> "src/main/resources/data/cars.json",
//          "inferSchema" -> "true"
//      ))
//      .load()
//
//    /*
//        Writing DFs
//        - format
//        - save mode = overwerite, append, ignore, errorIfExists
//        - path
//        - zero or more options
//     */
//
//    carsDF.write
//      .format("json")
//      .mode(SaveMode.Overwrite)
//      .save("src/main/resources/data/cars_dupe.json")
//
//    //JSON Flags
//    spark.read
//      .format("json")
//      .schema(carsSchema)
//      .option("dateFormat", "YYYY-MM-dd")       // Couple with schema. if Spark fails parsing, it will put null
//      .option("allowSingleQuotes","true")
//      .option("compression", "uncompressed")        // Uncompressed is the default, also have bzip2, gzip, lz4, snappy, deflate
//      .json("src/main/resources/data/cars.json")
//
//    // CSV Flags
//    val stocksSchema = StructType(Array(
//        StructField("symbol",StringType),       //Same names as the column names on the CSV file
//        StructField("date",DateType),
//        StructField("price",DoubleType)
//    ))
//
//    spark.read
//      .schema(stocksSchema)
//      .option("dateFormat","MMM dd YYYY")
//      .option("header","true")
//      .option("sep",",")
//      .option("nullValue","")
//      .csv("src/main/resources/data/stocks.csv")
//
//    // Parquet - Is an open source compressed binary data storage format optimized for fast reading of columns
//    // is the default storage format for DataFrames
//
//    carsDF.write
//      .mode(SaveMode.Overwrite)
//      .parquet("src/main/resources/data/cars.parquet")
//      // ou .save("src/main/resources/data/cars.parquet")
//
//    // Text files
//    spark.read.text("src/main/resources/data/sampleTextFile.txt").show()
//
//    // Reading from a remote DB
//    // To usando um postgres num container
//    val employeesDF = spark.read
//      .format("jdbc")
//      .options(Map(
//          "driver" -> "org.postgresql.Driver",
//          "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
//          "user" -> "docker",
//          "password" -> "docker",
//          "dbtable" -> "public.employees"
//      ))
//      .load()
//
//    employeesDF.show()

    // Exercícios

    val moviesDF = spark.read
      .option("nullValue","null")
      .json("src/main/resources/data/movies.json")

    moviesDF.show()
    moviesDF.printSchema()

    moviesDF.write
      .mode(SaveMode.Overwrite)
      .option("header","true")
      .option("sep","\t")
      .csv("src/main/resources/data/tab_movies.csv")

    moviesDF.write
      .mode(SaveMode.Overwrite)
      .parquet("src/main/resources/data/parque_movies.parquet")

    moviesDF.write
      .format("jdbc")
      .mode(SaveMode.Overwrite)
      .options( Map (
          "driver" -> "org.postgresql.Driver",
          "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
          "user" -> "docker",
          "password" -> "docker",
          "dbtable" -> "public.movies"
      ))
      .save()

    val moviesParquet = spark.read.parquet("src/main/resources/data/parque_movies.parquet")
    moviesParquet.show()

}
