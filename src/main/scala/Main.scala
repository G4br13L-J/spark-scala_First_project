package de.gabriel.sparkvideocourse

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

object Main {
  def main(args: Array[String]): Unit = {
    println("Initializing Spark Session...")
    val spark = SparkSession.builder() // Create a Spark session
      .appName("spark-video-course") // Set the application name
      .master("local[*]") // Use all available cores
      .config("spark.driver.bindAddress", "127.0.0.1")// Use local mode for testing
      .getOrCreate() // Get or create a Spark session
    println("\n Spark Session initialized successfully.")

    println("\n Reading CSV data...")
    val df: DataFrame = spark.read // Read data from a CSV file
      .option("header", value = true) // Use the first row as header
      .option("inferSchema", value = true) // Automatically infer the schema
      .csv("data/AAPL.csv") // Specify the path to the CSV file

    println("\n DataFrame loaded successfully. Showing data and schema:")
    df.show() // Display the first 20 rows of the DataFrame
    df.printSchema() // Print the schema of the DataFrame

    println("\n Performing DataFrame operations with select(string/s)...")
    df.select("Date", "Open", "Close") // Select specific columns from the DataFrame
      .filter("Open > 150") // Filter rows where the Open price is greater than 150
      .orderBy("Date") // Order the results by the Date column
      .show()

    // or

    val columName = "Open" // or df("Open")

    import spark.implicits._ // Import implicits for easier column manipulation
    $"Date" // Use the $"columnName" syntax to refer to columns

    println("Second way of using a select (with Object Columns):")
    df.select($"Date",df("Close"), col("High")) // Select columns using different syntaxes
      .show()

    val column = df("Open") // Get the "Open" column from the DataFrame
    val newColumn = (column + 2.0).as("OpenPlusTwo") // Create a new column by adding 2.0 to the "Open" column and rename it
    val columnString = column.cast("string") // Cast the "Open" column to a string type

    df.select(column, newColumn, columnString).show()

    val litColumn = lit(2.0) // Create a literal column with the value 2.0. The lit function is used to create a column with a constant value, which can be useful for adding fixed values to DataFrames.
    val newColumnString = concat(columnString, lit("Hello World")).as("OpenStringPlusHelloWorld") // Concatenate the string-cast "Open" column with a literal string "Hello World" and rename it

    df.select(column, newColumn, columnString, newColumnString, litColumn) // Select the original "Open" column, the new column, the string-cast column, the concatenated string column, and the literal column
      .show(truncate = false) // Show the selected columns without truncating the output
    // truncate means to limit the number of characters displayed in the output. Setting it to false means that the full content of each column will be displayed, regardless of its length.

    // DSL stands for Domain Specific Language, which allows you to write queries in a more readable and concise way.

    // Next we're gonna practice with some expressions
    println("\n Performing DataFrame operations with expressions:")
    val timeStampFromExpression = expr("cast(current_timestamp() as string) as timeStampExpression") // Get the current timestamp (returns a Column object)
    val timeStampFromFunctions = current_timestamp().cast("string").as("timeStampFunction") // Cast the current timestamp to a string type

    df.select(timeStampFromExpression, timeStampFromFunctions).show
    df.select(timeStampFromExpression, timeStampFromFunctions).printSchema() // Print the schema of the DataFrame with the timestamp columns, to ensure timestamps are indeed strings

    df.selectExpr("cast(Date as string)", "(Open + 1.0) as OpenPlus1", "current_timestamp() as currentTimeStamp")
      .show()

    println("\n Creating a temporary view and running SQL queries:")
    df.createTempView("df")
    spark.sql("select * from df").show()

    // Rename columns, varargs, withColumn, filter

    println("\n There are several ways to rename columns in a DataFrame:")

    // 1. Using withColumnRenamed (this uses the dataframe name)
    println("\n Renaming specific columns using withColumnRenamed:")
    df.withColumnRenamed("Open", "open")
    df.withColumnRenamed("Close", "close")

    // 2. Using a list with alias (also using col function)
    println("\n Renaming specific columns using a list with alias:")
    val renameColumns = List(
      col("Date").as("date"),
      col("Open").as("open"),
      col("High").as("high"),
      col("Low").as("low"),
      col("Close").as("close"),
      col("Adj Close").as("adj_close"),
      col("Volume").as("volume")
    )
   df.select(renameColumns: _*) // The : _* syntax is used to expand the list into a varargs argument
   .show()

    // 3. Using the same list, but this time with map function
    println("\n Renaming all columns to lowercase using map function:")
    df.select(df.columns.map(c => col(c).as(c.toLowerCase())): _*) // Convert all column names to lowercase
      .show()

    // Next is the use of withColumn to create or replace columns
    println("\n Creating a new column using withColumn:")

    val stockData = df.select(renameColumns: _*)
    .withColumn("diff", col("close") - col("open")) // Create a new column "diff" that calculates the difference between "close" and "open" prices // Now let's filter the data
    .filter(col("close") > col("open") * 1.1) // Filter rows where the "close" price is greater than the "open" price

    stockData.show()

    // Aggregations, grouping, and sorting
    println("\n Performing aggregations and grouping:")

    stockData
      .groupBy(year($"date").as("year")) // Group the data by "date"
      .agg(
        functions.avg($"diff").as("avg_diff"), // Calculate the average difference and rename it to "avg_diff"
        functions.max($"close").as("max_close"), // Calculate the maximum close price and rename it to "max_close"
        functions.min($"open").as("min_open") // Calculate the minimum open price and rename it to "min_open"
      )
      .sort($"max_close".desc)
      .orderBy(desc("year"), asc("avg_diff")) // Order the results by "date", can also use sort()
      .show() // Show the aggregated results

    // Window Functions
    println("\n Performing window functions:")
    // Window functions allow us to perform calculations across a set of rows related to the current row

    val result: DataFrame = highestClosingPricesPerYear(stockData)

    // Partitions, AST, Logical Plan & Optimizations
    println("\n Examining partitions, AST, logical plan, and optimizations:")

    //Spark SQL provides us with a fully declarative & structured API
    //Meaning it defines operators which we can use to tell Spark how the result can be drived from the input (withColumn, group by, count, etc)
    //Spark SQL is also called the structured API as it deals with structured data as Datasets have a schema
    //Therefore, Spark knows what's in the columns of a DataFrame
    //By calling the API, we chain these operators, while Spark assembles an abstract internal representation: The AST (Abstract Syntax Tree)
    //The AST is a tree structure that represents the logical form of the operations we want to perform on the data, it is also called the logical plan

    println("\n Abstract Syntax Tree (AST):")
    df.select("Date", "Open", "Close").filter("Open > 150").orderBy("Date").explain(extended = true) //.queryExecution.optimizedPlan

    // lazy evaluation: we only trigger the computation when we call an action (like show(), collect(), count(), etc)
    // until then, Spark just builds the logical plan
    // This allows Spark to optimize the execution plan before actually running the computations
    // This optimization can lead to significant performance improvements, especially for complex queries
    // It only evaluates the result once we want to see it

    println("\n Demonstrating lazy evaluation:")
    val df2 = df.select("Date", "Open", "Close").filter("Open > 150").orderBy("Date") // No computation is performed here
    df2.show() // Computation is triggered here

  }

  def highestClosingPricesPerYear(stockData: Dataset[Row]): DataFrame = {
    import stockData.sparkSession.implicits._
    val window = Window.partitionBy(year($"date").as("year")).orderBy($"close".desc)
    stockData
      .withColumn("rank", row_number().over(window))
      .filter($"rank" === 1)
      .drop("rank")
      .sort($"close".desc)
      //.explain(extended = true)
  }
  //Sample test
  //def add(x: Int, y: Int): Int = x + y -1
}