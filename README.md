# 📊 Spark Video Course Project

This project demonstrates foundational data processing techniques using **Apache Spark with Scala**. It's based on a hands-on walkthrough designed to explore Spark SQL capabilities including DataFrame operations, transformations, aggregations, and window functions, using real stock market data (`AAPL.csv`).

---

## 📁 Project Structure

- `Main.scala`: Contains the core logic to:
  - Read and transform stock data
  - Apply column operations and expressions
  - Perform aggregations and grouping
  - Use Spark SQL via temporary views
  - Apply window functions to extract insights
  - Examine Spark’s query planning and optimizations

- `FirstTest`: contains one unit test
  - Tests the output of key transformation functions in the project using Spark's `Dataset[Row]`, validating correctness through comparison with expected results.

---

## 🛠️ Technologies Used

- Apache Spark 3.5
- Scala 2.12+
- sbt (Scala Build Tool)
- CSV Input (Apple stock data)
- Spark SQL (Structured API)
- Local Spark Session (for development and testing)

---

## 📌 Features and Concepts Covered

### ✅ Data Ingestion
- Reads CSV file (`AAPL.csv`) with schema inference and headers.

### ✅ Column Operations
- Select specific columns
- Create computed columns using:
  - Arithmetic operations (`col("close") - col("open")`)
  - Type casting and literal columns
  - `concat`, `lit`, `expr` for string manipulation

### ✅ Expressions and DSL
- Demonstrates the use of:
  - Spark SQL expressions (`expr`, `selectExpr`)
  - DSL-style column references (`$"columnName"`)

### ✅ Column Renaming Techniques
- With `withColumnRenamed`
- With aliases (`as`)
- With `map` and `col` for programmatic renaming

### ✅ Data Transformations
- Filtering by computed conditions
- Creating new derived columns
- Applying domain-specific calculations (e.g., `diff = close - open`)

### ✅ Aggregations
- Group by year with metrics:
  - Average diff (`avg_diff`)
  - Max close
  - Min open

### ✅ Window Functions
- Ranking closing prices per year using `row_number()`
- Partitioning by year and sorting within partitions

### ✅ SQL Integration
- Create temporary views
- Run SQL queries using Spark SQL engine

### ✅ Optimization Insight
- View the **logical plan** and **physical plan** using `.explain(extended = true)`
- Demonstrate Spark's **lazy evaluation model**

---

## 🧠 Learning Objectives

By completing this project, I learned about:

- How to manipulate tabular data with Spark  
- How Spark builds and optimizes execution plans  
- How to use the Structured API (DataFrames, SQL, expressions)  
- Core concepts of lazy evaluation, AST, and window functions  

## 🎥 Reference

This project is based on the [Apache Spark Tutorial Series](https://www.youtube.com/playlist?list=PLrnPJCHvNZuDQ-jWPw13-wY2J57Z6epxk) by **Philipp Brunenberg** on YouTube, which covers the complete content implemented here.

