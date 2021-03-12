package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App{
    val spark = SparkSession.builder().appName("Common Spark Types").config("spark.master","local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

//    val movies = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")
//
//    // adding a plain value to a DF
//    // coluna chamada plain value cheia de 47
//    movies.select(col("Title"),lit(47).as("plain_value")).show()
//
//    // booleans
//    val dramaFilter = col("Major_Genre") === "Drama"
//    val goodRatingFilter = col("IMDB_Rating") > 7.0
//    val preferredFilter = dramaFilter and goodRatingFilter
//
//    movies.select("Title").where(dramaFilter)
//    // filter funciona tbm
//
//    val moviesGood = movies.select(col("Title"),preferredFilter.as("good_movie"))
//    moviesGood.filter("good_movie").show()
//    //moviesGood.filter(col("good_movie") === "true")
//
//    //negation
//    moviesGood.where(not(col("good_movie"))).show()
//
//    // numbers
//    val moviesAvgRatings = movies.select(col("Title"),
//        (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2).show()
//
//    // correlation
//   println(movies.stat.corr("Rotten_Tomatoes_Rating","IMDB_Rating"))   // Action
//
//    // strings
    val cars = spark.read.option("inferSchema","true").json("src/main/resources/data/cars.json")
//
//    // capitalize the first letter of each word in the respective column
//    // tem tbm lower e upper
//    cars.select(initcap(col("Name"))).show()
//
//    // contains
//    cars.select("*").where(col("Name").contains("volkswagen"))
//
//    // regex
//    val regexString = "volkswagen|vw"
//    val vwDF = cars.select(
//        col("Name"),
//        regexp_extract(col("Name"),regexString,0).as("regex_extract")
//    )
//      .where(col("regex_extract") =!= "")
//
//    vwDF.select(
//        col("Name"),
//        regexp_replace(col("Name"),regexString,"People's Car").as("regex_replace")
//    ).show()

    def getCarNames: List[String] = List("mercedes","plymouth","ford","fiat")

    val stringa = getCarNames.map(_.toLowerCase()).mkString("|")
    println(stringa)

    cars.select(
        col("Name"),
        regexp_extract(col("Name"),stringa,0).as("Regexada")            //É um contains
    ).where(col("Regexada") =!= "")
      .show()

}
