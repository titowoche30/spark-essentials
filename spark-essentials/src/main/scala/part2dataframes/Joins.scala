package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc,max}


object Joins extends App{
    val spark = SparkSession.builder().appName("Joins").config("spark.master","local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

//    val guitarsDF = spark.read
//      .option("inferSchema","true")
//      .json("src/main/resources/data/guitars.json")
//
//    val guitaristsDF = spark.read
//      .option("inferSchema","true")
//      .json("src/main/resources/data/guitarPlayers.json")
//
//    val bandsDF = spark.read
//      .option("inferSchema","true")
//      .json("src/main/resources/data/bands.json")

//    // JOIN
//    val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
//    val guitaristsBandsDF = guitaristsDF.join(bandsDF,joinCondition,"inner")
//
//    guitaristsBandsDF.show()
//
//    // outer joins
//    // left outer => left_outer
//    // right outer => right_outer
//    // full outer => outer         == tudo do join + tudo da esq. e dir.
//
//    // semi-joins
//    // left = inner join mas só fica com as colunas da tabela da esquerda
//    // Everything in the left DF for which there is a row in the right DF satisfying the condition
//    guitaristsDF.join(bandsDF,joinCondition,"left_semi").show()
//
//    // anti-joins
//    // left = Linhas onde não tem linha no DF da direita que satisfaça o join, ou seja, as linhas que faltaram
//    // ser joinadas
//    // Everything in the left DF for which there is NO row in the right DF satisfying the condition
//    guitaristsDF.join(bandsDF,joinCondition,"left_anti").show()
//
//    // Join resulta numa tabela com o nome das colunas das duas tabelas, pode acontecer de ter duas colunas
//    // chamadas id.
//    //Pra resolver isso podemos:
//
//    //1 - Mudar o nome da coluna joinada
//    guitaristsDF.join(bandsDF.withColumnRenamed("id","band"),"band").show()
//
//    //2 - Drop the duplicate column
//    guitaristsBandsDF.drop(bandsDF.col("id"))
//    //Works because Spark keeps unique identifiers for all the columns it operates on
//
//    //3 - Rename the offending column and keep the data
//    val bandsModDF = bandsDF.withColumnRenamed("id","bandId")
//    guitaristsDF.join(bandsModDF,guitaristsDF.col("band") === bandsModDF.col("bandId"))
//    // but still keeps the duplicate data
//
//    // Complex types
//    guitaristsDF.join(guitarsDF.withColumnRenamed("id","guitarId"),
//        expr("array_contains(guitars, guitarId)")).show()

    //Exercices


    val employees = spark.read
      .format("jdbc")
      .options(Map(
          "driver" -> "org.postgresql.Driver",
          "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
          "user" -> "docker",
          "password" -> "docker",
          "dbtable" -> "public.employees"
      ))
      .load()


    val titles = spark.read
      .format("jdbc")
      .options(Map(
          "driver" -> "org.postgresql.Driver",
          "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
          "user" -> "docker",
          "password" -> "docker",
          "dbtable" -> "public.titles"
      ))
      .load()

    val salaries = spark.read
      .format("jdbc")
      .options(Map(
          "driver" -> "org.postgresql.Driver",
          "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
          "user" -> "docker",
          "password" -> "docker",
          "dbtable" -> "public.salaries"
      ))
      .load()


    val dept_manager = spark.read
      .format("jdbc")
      .options(Map(
          "driver" -> "org.postgresql.Driver",
          "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
          "user" -> "docker",
          "password" -> "docker",
          "dbtable" -> "public.dept_manager"
      ))
      .load()

    // group-byar salaries pelo emp_no pelo max do salary e joinar com eployees

    val grupadoSalaries = salaries.groupBy("emp_no").max("salary")
    val grupadoSalariesJoined = employees
      .join(grupadoSalaries,"emp_no")
      .orderBy(desc("max(salary)"))

    grupadoSalariesJoined.show()

    // joino employees com dept_manager e pelo anti-join

    val cond = employees.col("emp_no") === dept_manager.col("emp_no")
    employees.join(dept_manager,cond,"left_anti").show()

//    // group-byar titles pelo emp_no pelo max do to_date, joinar o emp_no dessa tabela com o employees
//
//    val titlesByDate = titles.groupBy("emp_no").agg(max("to_date").as("to_date"))
//    val latestTitles = titlesByDate.join(titles,Seq("emp_no","to_date"))
//    latestTitles.join(salaries,"emp_no")
//      .orderBy(desc("salary"))
//      .show()



}
