package part7bigdata

import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object TaxiEconomicImpact extends App{

    /* Args
        1 - Data Source
        2 - taxi zones data source
        3 - output data destination
     */

//        if (args.length != 3) {
//            println("Quero 3 argumentos")
//            System.exit(1)
//        }

        val spark = SparkSession.builder()
          .config("spark.master", "local")
          .appName("Taxi Big Data Application")
          .getOrCreate()

        import spark.implicits._

        //val bigTaxiDF =
        val taxiDF = spark.read.load(args(0))
        val taxiZonesDF = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(args(1))

        val groupAttemptsDF = taxiDF
          .select(round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinId")
              , col("PULocationID"), col("total_amount"))
          .where(col("passenger_count") < 3)
          .groupBy(col("fiveMinId"), col("PULocationID"))
          .agg(count("*").as("total_trips"), sum(col("total_amount")).as("total_amount"))
          .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
          .drop("fiveMinId")
          .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
          .orderBy(col("total_trips").desc)
          .drop("LocationID", "service_zone")

        val percentGroupAttempt = 0.05
        val percentAcceptGrouping = 0.3
        val discount = 5
        val extraCost = 2
        val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

        val groupingEstimateEconomicImpactDF = groupAttemptsDF
          .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
          .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
          .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
          .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))

        groupingEstimateEconomicImpactDF.show(100)

        val totalProfitDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
        totalProfitDF.show()
//        totalProfitDF.write
//          .mode(SaveMode.Overwrite)
//          .option("header","true")
//          .csv(args(2))
}
