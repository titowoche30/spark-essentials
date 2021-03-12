package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.reflect.io.Directory
import java.io.File

object SparkSQL extends App{
    val spark = SparkSession.builder()
      .appName("Spark SQL Practice")
      .config("spark.master","local")
      .config("spark.sql.warehouse.dir","src/main/resources/warehouse")         //diretório onde criar o warehouse
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

//    val cars = spark.read
//      .option("inferSchema","true")
//      .json("src/main/resources/data/cars.json")
//
//    cars.createOrReplaceTempView("cars")
//
//    val americanCars = spark.sql(
//        """
//          |SELECT Name,Origin FROM cars WHERE Origin = 'USA'
//          |""".stripMargin
//    )
//    americanCars.show()

    // Se não tiver especificado diretório na config,
    // vai criar uma pasta chamada spark-warehouse que vai conter os DBs criados

    def deleteDB(): Boolean = {
        val directory = new Directory(new File("src/main/resources/warehouse/rtjvm.db"))
        directory.deleteRecursively()
    }

//    deleteDB()

    spark.sql("create database IF NOT EXISTS rtjvm")
    spark.sql("use rtjvm")
    // transfer tables from a DB to Spark tables

    def readTable(tableName: String) = spark.read
      .format("jdbc")
      .options(Map(
          "driver" -> "org.postgresql.Driver",
          "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
          "user" -> "docker",
          "password" -> "docker",
          "dbtable" -> s"public.$tableName"
      ))
      .load()



    def transferTables(tableNames: List[String],warehouse: Boolean = false) = tableNames.foreach { tableName =>
        val tableDF = readTable(tableName)
        tableDF.createOrReplaceTempView(tableName)
        if (warehouse) {
            tableDF.write
              .mode(SaveMode.Overwrite)
              .saveAsTable(tableName) // Salva no database que to usando
        }
    }

    transferTables(List("employees","departments","titles","dept_emp","salaries","dept_manager"))

    // read DF from loaded Spark Tables -- Convert a table read in memory by Spark into a DF
    val dbs = spark.sql("show databases")
    dbs.show()
    spark.sql("SHOW TABLES FROM rtjvm").show()

    val employees2 = spark.read.table("employees")
    employees2.show()



    // Exercícios



    def readDF(filename: String) = spark.read
      .option("inferSchema","True")
      .json(s"src/main/resources/data/$filename")

 //   spark.sql("use rtjvm")
//    val movies = readDF("movies.json")
//    movies.write
//      .mode(SaveMode.Overwrite)
//      .saveAsTable("movies")

    spark.sql("SHOW TABLES FROM rtjvm").show()


    val countEmployees = spark.sql(
        """
          |SELECT count(*)
          |FROM employees
          |WHERE hire_date > '1999-01-01' and hire_date < '2000-01-01'
          |""".stripMargin
    )
    countEmployees.show()



    val avgSalaries = spark.sql(
        """
          |SELECT BROUND(AVG(salaries.salary),3) AS AVG_Salaries, departments.dept_name
          |FROM employees
          |INNER JOIN salaries
          |ON employees.emp_no = salaries.emp_no
          |INNER JOIN dept_emp
          |ON employees.emp_no = dept_emp.emp_no
          |INNER JOIN departments
          |ON departments.dept_no = dept_emp.dept_no
          |WHERE employees.hire_date > date('1999-01-01') and employees.hire_date < date('2001-01-01')
          |GROUP BY departments.dept_name
          |ORDER BY bround(avg(CAST(salary AS BIGINT)), 3) DESC
          |""".stripMargin
    )

    avgSalaries.show()

    val maxSalaries = spark.sql(
        """
          |SELECT MAX(salaries.salary) as MAX_Salaries, departments.dept_name
          |FROM employees
          |INNER JOIN salaries
          |ON employees.emp_no = salaries.emp_no
          |INNER JOIN dept_emp
          |ON employees.emp_no = dept_emp.emp_no
          |INNER JOIN departments
          |ON departments.dept_no = dept_emp.dept_no
          |WHERE hire_date > date('1999-01-01') and hire_date < date('2001-01-01')
          |GROUP BY departments.dept_name
          |ORDER BY MAX_Salaries DESC
          |""".stripMargin
    )

    maxSalaries.show()

}
