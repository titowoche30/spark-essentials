package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,column,expr,asc,desc}

object ColumnsAndExpressions extends App{
    val spark = SparkSession.builder()
      .appName("DF Columns and Expressions")
      .config("spark.master","local")
      .getOrCreate()

//    val carsDF = spark.read
//      .option("inferSchema","true")
//      .json("src/main/resources/data/cars.json")
//
//    carsDF.show()
//
//    // Columns
//
//    val firstColumn = carsDF.col("Name")
//    // Is an column object that have no data inside, we can obtain the data by selecting in it
//
//    // Selecting (projection)
//    // We are projecting the DF into a DF that has fewer columns(data)
//    val carNamesDF = carsDF.select(firstColumn)       //A new DF with just the "Name" column
//    carNamesDF.show()
//
//    // Various select methods
//    import spark.implicits._
//
//    // Select funciona com Column Objects
//    carsDF.select(
//        carsDF.col("Name"),
//        col("Acceleration"),     // Por enquanto é a mesma coisa que o de cima
//        column("Weight_in_lbs"),
//        'Year,                           // Scala symbol, auto-converted to column
//        $"Horsepower",                   // Fancier interpolated string, returns a column object
//        expr("Origin")              // Expression
//    )
//
//    // select with plain column names
//    carsDF.select("Name","Year")
//
//    // Expressions'
//    val simplestExpression = carsDF.col("Weight_in_lbs")
//    val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2
//
//    val carsWithWeightsDF = carsDF.select (
//        col("Name"),
//        col("Weight_in_lbs"),
//        weightInKgExpression.as("Weight_in_kg"),
//        expr("Weight_in_lbs / 2.2").as("Wight_in_kg_2")
//    )
//
//    //selectExpr - mais usado
//    val carsWithSelectExprWeightsDF = carsDF.selectExpr(
//        "Name",
//        "Weight_in_lbs",
//        "Weight_in_lbs / 2.2"
//    )
//
//    // DF Processing
//
//    // adding new column to existing dataframe
//    val carsWithKGDF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
//                                        //colName, expression
//    // renaming a colums
//    val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs","Weight_in_pounds")
//    // remove a column
//    carsWithColumnRenamed.drop("Cylinders","Displacement")
//
//    // Filtering
//    // =!= is the NOT EQUAL operator for column object
//    // === is the EQUAL operator for column object
//    val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
//    val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
//    // Filtering with expression strings
//    val americanCarsDF = carsDF.filter("Origin = 'USA'")
//    // chain filters
//    val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA")
//      .filter(col("Horsepower") > 150)
//
//    //and é um infix operator
//    val americanPowerfulCarsDF2 = carsDF.filter( col("Origin") === "USA" and
//      col("Horsepower") > 150)
//
//    val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")
//
//    // unioning = adding more rows
//    val moreCarsDF = spark.read.option("inferSchema","true").json("src/main/resources/data/more_cars.json")
//    val allCarsDF = carsDF.union(moreCarsDF)      //works if the DFs have the same Schema
//
//    //distinct
//    val allCountriesDF = carsDF.select("Origin").distinct()
//    allCountriesDF.show()

    //Exercícios

    val moviesDF = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")
    moviesDF.show()

    val nameDateDF = moviesDF.select("Title","Release_Date")
    nameDateDF.show()



    val notNullDF = moviesDF.select("Title","US_Gross","Worldwide_Gross","US_DVD_Sales","IMDB_Rating","Major_Genre").na.fill(0)

    val profitsDF = notNullDF.selectExpr(
        "Title",
        "US_Gross",
        "Worldwide_Gross",
        "US_DVD_Sales",
        "US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Profits "
    )

//ou
//    val profitsDF = notNullDF.withColumn("Total_Profits"
//        ,col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales"))

    val namesProfitsDF = profitsDF.select("Title","Total_Profits")
    profitsDF.show()

    println("n de nulos em DVD_SALES = " + moviesDF.selectExpr("US_DVD_SALES = 'null' ").count())



    notNullDF.filter("Major_Genre = 'Comedy' and  IMDB_Rating > 6 ")
      .select("Title","Major_Genre","IMDB_Rating").sort(desc("IMDB_Rating")).show()

}
