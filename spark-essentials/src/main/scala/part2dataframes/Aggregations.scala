package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Aggregations extends App{
    val spark = SparkSession.builder().appName("Aggregations and Grouping").config("spark.master","local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val moviesDF = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")

    // SÃO WIDE TRANSFORMATIONS

//    // Counting
//    val genresCountDF = moviesDF.select(count(col("Major_Genre")))   // all the values not null
//    genresCountDF.show()
//    moviesDF.selectExpr("count(Major_Genre)").show()                    // all the values not null
//
//    // Counting all
//    moviesDF.select(count("*")).show()                            // all the rows including nulls
//    println(moviesDF.count())                                              // all the rows including nulls
//
//    // Counting disctinct
//    moviesDF.select(countDistinct(col("Major_Genre"))).show()
//
//    // Approximate functions to quick data analysis in large DataFrames
//    moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()
//
//    // Statistics
//    moviesDF.select(min(col("IMDB_Rating"))).show()
//    moviesDF.selectExpr("min(IMDB_Rating)").show()
//
//    moviesDF.select(mean("IMDB_Rating")).show()         //stddev
//    moviesDF.selectExpr("mean(IMDB_Rating)").show()
//
//    moviesDF.select(sum("US_Gross")).show()
//    moviesDF.selectExpr("sum(US_Gross)").show()
//
//    // Grouping

//    val grupadoByGenreCount = moviesDF.groupBy("Major_Genre")        //includes null
//      .count()
//      .sort(desc("count"))
//
//    grupadoByGenreCount.show()
//
//    val grupadoByGenreMean = moviesDF.groupBy("Major_Genre")
//      .mean("IMDB_Rating")
//      .sort(desc("avg(IMDB_Rating)"))
//
//    grupadoByGenreMean.show()
//
//    val aggregationsByGenreDF = moviesDF.groupBy("Major_Genre")
//      .agg(
//          count("*").as("N_Movies"),
//          mean("IMDB_Rating").as("Mean_Rating")
//      )
//      .orderBy(desc("Mean_Rating"))
//
//    aggregationsByGenreDF.show()

    // Exercícios

    moviesDF.selectExpr("sum(US_Gross + Worldwide_Gross + US_DVD_Sales) as Total_Gross").show()


    moviesDF.select(countDistinct("Director")).show()

    moviesDF.selectExpr(
        "mean(US_Gross) as Mean_Gross",
        "stddev(US_Gross) as Std_Gross"
    ).show()

    moviesDF.groupBy("Director")
      .agg(
          mean("IMDB_Rating").as("Mean_IMDB"),
          mean("US_Gross").as("Mean_US_Gross")
      ).orderBy(col("Mean_US_Gross").desc_nulls_last).show()

    moviesDF.groupBy("Director")
      .agg(
          mean("IMDB_Rating").as("Mean_IMDB"),
          mean("US_Gross").as("Mean_US_Gross"),
          count("*").as("N_Movies")
      ).orderBy(desc_nulls_last("Mean_IMDB")).show()


}
