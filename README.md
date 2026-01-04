TASK -1 BIG DATA ANALYSIS

COMPANY: Codtech IT Solutions Private Limited
NAME: Yashwanth Koppera
Intern ID: CTIS1714
DOMAIN: Data Analytics
Duration: 4 WEEEKS
MENTOR: Neela Santhosh Kumar 


DESCRIPTION OF THE TASK-1  BIG DATA ANALYSIS

1.Overview
This repository contains Task 1 of the CODTECH Internship, which focuses on performing Big Data Analysis using PySpark. The aim of this task is to demonstrate scalable data processing on a large real-world dataset and to extract meaningful insights using distributed computing techniques. The implementation is carried out in a PySpark notebook, showcasing data loading, cleaning, transformation, aggregation, and analysis steps.

2.Objective

The primary objective of this task is to:
    Work with a large-scale dataset
    Use PySpark to demonstrate scalability and parallel processing
    Perform data cleaning and preprocessing
    Apply analytical aggregations
    Derive insights from big data
    Deliver a working notebook/script as proof of execution

3.Dataset:
The dataset used in this task is the NYC Yellow Taxi Trip Records (January 2015).
It contains millions of records with attributes such as:

  Vendor ID
  Pickup and drop-off timestamps
  Passenger count
  Trip distance
  Fare amount
  Payment type
  Total amount
This dataset is suitable for big data analysis due to its size and real-world complexity.


4.Tools & Technologies

   Python
   PySpark (Apache Spark)
   Java (JDK 11)
   Google Colab / VS Code
   CSV Dataset

5 Methodology:
  5.1. Environment Setup
       PySpark was installed and a Spark session was created to initialize the distributed processing environment.
  5.2. Data Loading
       The NYC taxi dataset was loaded from a CSV file into a Spark DataFrame using schema inference. The successful loading of data was verified by displaying sample rows.
  5.3. Data Cleaning
       Rows containing null values were removed.
       Numeric columns such as fare_amount and total_amount were cleaned.
       Regular expressions and type casting were used to safely convert string values into numeric formats.
       Invalid or malformed entries were handled to avoid aggregation errors.
  5.4. Data Analysis
       The following analyses were performed:
       Average Fare Calculation using cleaned fare values.
       Revenue Analysis by Payment Type using grouped aggregations on total amount.
       The operations were executed using Spark transformations and actions, demonstrating scalable computation.
  5.5. Insight Generation
       The analysis highlighted key patterns such as:
       Average taxi fare value
       Payment types contributing the highest revenue
       Effectiveness of PySpark in handling millions of records efficiently


6 output:

