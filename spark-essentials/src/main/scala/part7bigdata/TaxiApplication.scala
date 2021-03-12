package part7bigdata

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object TaxiApplication extends App{
    val spark = SparkSession.builder()
      .appName("Spark SQL Practice")
      .config("spark.master","local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    //val bigTaxiDF =
    val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
//    taxiDF.printSchema()
//    taxiDF.show()
   // println(taxiDF.count())

    val taxiZonesDF = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("src/main/resources/data/taxi_zones.csv")

  //  taxiZonesDF.printSchema()

    /**
      * Questions:
      *
      * 1. Which zones have the most pickups/dropoffs overall?
      * 2. What are the peak hours for taxi?
      * 3. How are the trips distributed by length? Why are people taking the cab?
      * 4. What are the peak hours for long/short trips?
      * 5. What are the top 3 pickup/dropoff zones for long/short trips?
      * 6. How are people paying for the ride, on long/short trips?
      * 7. How is the payment type evolving with time?
      * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
      *
      */

    val pickupsByTaxiZoneDF = taxiDF.groupBy(col("PULocationID"))
      .agg(count("*").as("totalTrips"))
      .join(taxiZonesDF,col("PULocationID") === col("LocationID"))
      .drop("LocationID","service_zone")
      .orderBy(col("totalTrips").desc)

 //   pickupsByTaxiZoneDF.show()

    // 1b - group by borough

    val pickupsByBorough = pickupsByTaxiZoneDF.groupBy(col("Borough"))
      .agg(sum(col("totalTrips")).as("totalTrips"))
      .orderBy(col("totalTrips").desc)

 //   pickupsByBorough.show()

    // Tem muito mais registros de Manhattan do que dos outros
    // Proposal 1: Aumentar um pouco o preço em Manhattan e diminuir em outros lugares

    // 2

    val pickupsByHourDF = taxiDF
      .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
      .groupBy("hour_of_day")
      .agg(count("*").as("totalTrips"))
      .orderBy(col("totalTrips").desc)

 //   pickupsByHourDF.show()

    // Proposal 2: Aumentar os preços nos horários de pico

    // 3
    val tripDistanceDF = taxiDF.select(col("trip_distance").as("distance"))
    val longDistanceThreshold = 30
    val tripDistanceStatsDF = tripDistanceDF.select(
        count("*").as("count"),
        lit(longDistanceThreshold).as("threshold"),
        mean("distance").as("mean"),
        stddev("distance").as("stddev"),
        min("distance").as("min"),
        max("distance").as("max")
    )

 //   tripDistanceStatsDF.show()

    val tripsWithLengthDF = taxiDF
      .withColumn("isLong",col("trip_distance") >= longDistanceThreshold)
    val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count()
  //  tripsByLengthDF.show()

    // 4
    val pickupsByHourByLengthDF = tripsWithLengthDF
      .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
      .groupBy("hour_of_day","isLong")
      .agg(count("*").as("totalTrips"))
      .orderBy(col("totalTrips").desc)
 //   pickupsByHourByLengthDF.show(48)

    // 5

    def pickupDropoffPopularity(predicate: Column) = tripsWithLengthDF
      .where(predicate)
      .groupBy("PULocationID","DOLocationID").agg(count("*").as("totalTrips"))
      .join(taxiZonesDF,col("PULocationID") === col("LocationID"))
      .withColumnRenamed("Zone","Pickup_Zone")
      .drop("LocationID","Borough","service_zone")
      .join(taxiZonesDF,col("DOLocationID") === col("LocationID"))
      .withColumnRenamed("Zone","Dropoff Zone")
      .drop("LocationID","Borough","service_zone")
      .drop("PULocationID","DOLocationID")
      .orderBy(col("totalTrips").desc)

 //   pickupDropoffPopularity(col("isLong")).show()
 //   pickupDropoffPopularity(not(col("isLong"))).show()

    // Por mais que existam MUITO menos long trips, ela são bem diferentes entre si
    // Shorts são mais entre áreas ricas
    // Longs são mais entre aeroportos
    // Proposal 3:  - sugestão de transporte público entre aeroportos pra NYC town hall
    //              - sugestão pra compania de taxi: segmentos de mercado separados e serviços personalizados para cada
    //              - sugestão pra compania de taxi: fazer uma parceria com bares e restaurantes das áreas ricas pra buscar a galera

    // 6

    // ratecode é o tipo de pagamento,1 é cartão e 2 é dinheiro.
    // proposal: Nunca ficar sem máquina de cartão
    val ratecodeDistribution = taxiDF
      .groupBy(col("RatecodeID")).agg(count("*").as("totalTrips"))
      .orderBy(col("totalTrips").desc)

  //  ratecodeDistribution.show()

    // 7
    val ratecodeEvolution = taxiDF
      .groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_day"),col("RatecodeID"))
      .agg(count("*").as("totalTrips"))
      .orderBy(col("pickup_day"))

  //  ratecodeEvolution.show()

    // 8
    // grouping as corridas que começam na mesma pickup_location em buckets de 5 minutos de intervalo
    val groupAttemptsDF = taxiDF
      .select(round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinId")
      ,col("PULocationID"), col("total_amount"))
      .where(col("passenger_count") < 3)
      .groupBy(col("fiveMinId"),col("PULocationID"))
      .agg(count("*").as("total_trips"),sum(col("total_amount")).as("total_amount"))
      .withColumn("approximate_datetime",from_unixtime(col("fiveMinId") * 300))
      .drop("fiveMinId")
      .join(taxiZonesDF,col("PULocationID") === col("LocationID"))
      .orderBy(col("total_trips").desc)
      .drop("LocationID","service_zone")

  //  groupAttemptsDF.show()

    // Muitas corridas numa mesma zone em um intervalo de tempo muito pequeno
    // Com isso, incentivar as pessoas a fazerem corridas compartilhadas a partir de um desconto

    // Assumindo que:
    // 5% das corridas são grupáveis
    // 30% das pessoas aceitam ser agrupadas
    // disconto de 5 pila pra quem pega uma agrupada
    // 2 conto extra pra quem pega uma individual
    // se 2 corridas são grupadas, o custo é diminuído em 60% em relação à uma corrida média

    val percentGroupAttempt = 0.05
    val percentAcceptGrouping = 0.3
    val discount = 5
    val extraCost = 2
    val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

    val groupingEstimateEconomicImpactDF = groupAttemptsDF
      .withColumn("groupedRides",col("total_trips") * percentGroupAttempt)
      .withColumn("acceptedGroupedRidesEconomicImpact",col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount) )
      .withColumn("rejectedGroupedRidesEconomicImpact",col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
      .withColumn("totalImpact",col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))

    groupingEstimateEconomicImpactDF.show()

    val totalProfitDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
    totalProfitDF.show()








}
