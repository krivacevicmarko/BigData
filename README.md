# Data Processing and Analysis of Udemy Course Data

This project involves building a near real-time data processing system using Apache Kafka, Apache Spark, and Databricks, with a focus on analyzing data related to Udemy courses. The goal is to efficiently collect, process, and analyze data streams to derive valuable insights.

Implementation Details:
1) Kafka Producer & Consumer Implementation:
 - A Kafka producer was developed to continuously fetch data from the Udemy API and stream it to a Kafka topic.
 - A Kafka consumer processed incoming messages, storing them in Parquet/CSV format for further analysis.
2) Data Ingestion & Transformation in Databricks:
 - The collected dataset was imported into Databricks and processed using Apache Spark.
 - Data transformation included handling missing values, normalizing numerical attributes, and structuring the dataset for efficient querying and visualization.
3) Spark Streaming for Real-Time Analysis:
 - Spark Streaming was implemented to process incoming course data dynamically.
 - Periodic queries provided insights into trends such as popular course categories, pricing fluctuations, and enrollment trends over time.
4) Data Analysis & Visualization:
Eight detailed analyses were performed on the dataset using Spark SQL and DataFrame operations, including:
 - Statistical analysis of course ratings and pricing.
 - Regression analysis to determine factors influencing course popularity.
 - Trend analysis on course enrollment over time.
 - Sentiment analysis on course reviews (if available in the dataset).
 - Comparison of free vs. paid courses in terms of engagement and completion rates.
The results were visualized using Databricks' built-in visualization tools to enhance interpretability.
5) Business Impact & Insights:
 - The analysis provided valuable insights for course creators, platform administrators, and learners, helping them understand which courses perform best, how pricing affects enrollments, and what topics are trending.

By leveraging Kafka for data ingestion, Spark for processing, and Databricks for analytics, this project demonstrates a robust end-to-end data pipeline for real-time course data analysis.
