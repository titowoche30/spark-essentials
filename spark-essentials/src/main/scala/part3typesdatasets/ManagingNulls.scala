package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App{
    val spark = SparkSession.builder()
      .appName("Common Spark Types")
      .config("spark.master","local")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val movies = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")

    // select the first non-null value
    movies.select(
        col("Title"),
        coalesce(col("Rotten_Tomatoes_Rating"),col("IMDB_Rating") * 10),        //takes the first non-null value
        col("Rotten_Tomatoes_Rating"),
        col("IMDB_Rating")
    ).show()

    // checking for nulls
    movies.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

    // nulls when ordering
    movies.orderBy(col("IMDB_Rating").desc_nulls_last)

    // removing nulls
    movies.select("Title","IMDB_Rating").na.drop()       // remove linhas que contém null

    // replace nulls
    movies.na.fill(0,List("IMDB_Rating","Rotten_Tomatoes_Rating"))    //valor e quais clunas

    movies.na.fill(Map(
        "Rotten_Tomatoes_Rating" -> 10,
        "IMDB_Rating" -> 0,
        "Director" -> "Unknown"
    ))

    // complex operations
    movies.selectExpr(
        "Title",
        "IMDB_Rating",
        "Rotten_Tomatoes_Rating",
        "ifnull(Rotten_Tomatoes_Rating,IMDB_Rating * 10) as ifnull",       // same as coalesce
        "nvl(Rotten_Tomatoes_Rating,IMDB_Rating * 10) as nvl",              // same as coalesce
        "nullif(Rotten_Tomatoes_Rating,IMDB_Rating * 10) as nullif",        // returns null if the two values are EQUAL, else first value
        "nvl2(Rotten_Tomatoes_Rating,IMDB_Rating * 10,0.0) as nvl2"         // if (first != null) second else third
    ).show()

}
