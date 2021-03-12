package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ComplexTypes extends App{
    val spark = SparkSession.builder()
      .appName("Common Spark Types")
      .config("spark.master","local")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val movies = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")

    // Dates
    val moviesWithReleaseDates = movies.select(col("Title"),to_date(col("Release_Date"),"dd-MMM-yy").as("Actual_Release"))
      .withColumn("Today",current_date())
      .withColumn("Right_Now",current_timestamp())
      .withColumn("Movie Age",datediff(col("Today"),col("Actual_Release"))/365)
    //tem tbm date_add e date_sub que adicionam e subtraem dias de dates

    moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull).show()


    // Exercícios

    /*
       1. How to deal with multiple date formats?
           - parse the DF multiple times with the patterns that you identify, then union the small DFs
           - ou ignora os errados
    */

    val stocks = spark.read
      .option("inferSchema","true")
      .option("header","true")
      .option("sep",",")
      .csv("src/main/resources/data/stocks.csv")

    stocks.show()

    val stocksDate = stocks.select(col("symbol"), col("price"),col("date"))
      .withColumn("Actual_Date",to_date(col("date"),"MMM d yyyy"))
      .withColumn("Today",current_date())
      .withColumn("Dif_Today",datediff(col("Today"),col("Actual_date"))/365)

    stocksDate.show()

    // Structures  = Groups of columns aggregated in to one column

    // 1 - With col operators
    movies.select(col("Title"), struct(col("US_Gross"),col("Worldwide_Gross")).as("Profit"))
      .select(col("Title"),col("Profit"),col("Profit").getField("US_Gross").as("US_Profit"))
      .show()

    // 2 - With expression strings
    movies.selectExpr("Title","(US_Gross, Worldwide_Gross) as Profit")
      .selectExpr("Title","Profit.US_Gross").show()


    // Arrays

    // " |," => separa por espaço ou vírgula
    val moviesWithWords = movies.select(col("Title"),split(col("Title"), " |,").as("Title_Words"))
    // Title_Words is an Array of strings

    moviesWithWords.select(
        col("Title"),
        expr("Title_Words[0]"),          //First word of each line
        size(col("Title_Words")),
        array_contains(col("Title_Words"),"Love")
    ).show()










}
