package part3typesdatasets

import java.sql.Date

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object Datasets extends App{
    val spark = SparkSession.builder()
      .appName("Common Spark Types")
      .config("spark.master","local")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val numbers = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("src/main/resources/data/numbers.csv")

    numbers.show(10)

    // Dataset = typed DataFrame where each row is a JVM object, distributed collection of JVM objects
    // slower tem DataFrames
    // Temos acesso às funções de DataFrame tbm.
    // DataFrame são na vdd DataSet[Row]

    // Convert DataFrame to DataSet
    implicit val intEncoder = Encoders.scalaInt                 // Transforma a linha num DF em um Int
    val numbersDS:Dataset[Int] = numbers.as[Int]

    numbersDS.filter(_ < 100)                                   // Agora consigo fazer isso

    // Dataset of a complex type
    // 1 - Define your case class
    case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],      // Pq pode ter nulo
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],                     // Pq pode ter nulo
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year:Date,
                  Origin:String
                  )

    // 2 - read the DF from the file
    def readDF(filename: String) = spark.read
      .option("inferSchema","True")
      .json(s"src/main/resources/data/$filename")

    // 3 - Define an encoder (importing the implicits)
    import spark.implicits._
    val carsD = readDF("cars.json")
    val carsDF = carsD.select(col("*"),to_date(col("Year"),"yyyy-MM-dd").as("Year")).drop(carsD.col("Year"))
    carsDF.printSchema()

    // 4 - Convert the DF to DS
    val carsDS = carsDF.as[Car]

    // DS collection functions
    // map, flatMap, fold, reduce, for comprehensions...
    carsDS.map(car => car.Name.toUpperCase).show()

    //Exercícios

    val numCars = carsDS.count()
    println(numCars)

    val powerfullCars = carsDS.filter(car => car.Horsepower.getOrElse(0L) > 140).count()                             // 0 se a option tiver None
    println(powerfullCars)

    val averageHP = carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / numCars
    println(averageHP)

    // Joins

    case class Guitar(
                     id : Long,
                     make: String,
                     model: String,
                     guitarType: String
                     )

    case class GuitarPlayer(
                           id: Long,
                           name: String,
                           guitars: Seq[Long],
                           band: Long
                           )

    case class Band(
                   id: Long,
                   name: String,
                   hometown: String,
                   year: Long
                   )
    val guitars = readDF("guitars.json").as[Guitar]
    val guitarPlayer = readDF("guitarPlayers.json").as[GuitarPlayer]
    val bands = readDF("bands.json").as[Band]

    // Se usar só join vai perder a informação de tipo, ou seja, vai virar um DF
    val guitarPlayerBands: Dataset[(GuitarPlayer, Band)]
    = guitarPlayer.joinWith(bands,guitarPlayer.col("band") === bands.col("id"))
    guitarPlayerBands.show()

    //Exercício

    val guitarP = guitarPlayer
      .joinWith(guitars
          ,array_contains(guitarPlayer.col("guitars"),guitars.col(("id")))
          ,"outer")

    guitarP.show()

    // fim exercício

    //grouping
    val carsGroupedByOrigin = carsDS.groupByKey(_.Origin).count()           // Origin é a key na qual iremos agrupar
    carsGroupedByOrigin.show()
    val carsGroupedByOrigin_ = carsDS.groupBy("Origin").count()
    carsGroupedByOrigin_.show()                                             // O que muda são os tipos de dados

    // Joins are wide transformations, e.g., need to shuffle data




}
