package part6practical

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TestDeployApp extends App{
   // def main(args: Array[String]): Unit = {
        /*
        * movies.json as args(0)
        * path of future goodcomedies.json as args(1)
        *
        * goodcomedies = genre == comedy and imdb > 6.5
         */

//        if (args.length != 2) {
//            println("Need input path and output path")
//            System.exit(1)
//        }

        val appName = "Test Deploy App"
        val spark = SparkSession.builder()
          .appName("Test Deploy App")
          .config("spark.master","local[3]")
          .config("spark.metrics.namespace",appName)
          .config("spark.executor.processTreeMetrics.enabled","true")
          .config("spark.ui.prometheus.enabled","true")
          .config("spark.executor.processTreeMetrics.enabled","true")
          .getOrCreate()

        val moviesDF = spark.read
          .option("inferSchema","true")
          .json("src/main/resources/data/movies.json")

    //  .json(args(0))

        val goodComediesDF = moviesDF.select(
            col("Title"),
            col("IMDB_Rating").as("Rating"),
            col("Release_Date").as("Release")
        )
          .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6.5 )
          .orderBy(col("Rating").desc_nulls_last)

        goodComediesDF.show

        goodComediesDF.write
          .mode(SaveMode.Overwrite)
          .format("json")
          .save("src/main/resources/data/OUT")

    //  .save(args(1))

   // }

}
