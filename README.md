# PySpark Data Engineering Assignment - Growth with Data Skill Course

## 🚀 Overview

This repository contains the solution to a **Data Engineering assignment** from the **"Growth with Data" skill course**. The task primarily focused on applying **PySpark** to process a dataset that is typically stored in a **Hadoop Distributed File System (HDFS)**. However, in this solution, I have used **local storage** instead of HDFS to simplify development and testing.

## 📁 Project Structure


## 🛠️ Technologies Used

- **Python 3.x**
- **Apache Spark (PySpark)**
- **Local File System** (used instead of HDFS for demonstration)
- **Vscode** 

## ✨ Highlights

- Processed data using **PySpark DataFrame APIs**.
- Successfully read input data from **local storage** rather than HDFS.
- Followed **data engineering best practices** for transformation and analysis.
- Demonstrated usage of:
  - Data ingestion with `spark.read`
  - DataFrame transformations (`select`, `filter`, `groupBy`, etc.)
  - Writing output using `df.write`

## 🔄 Adaptation Note

> Although the original assignment specified the use of Hadoop HDFS, I have adapted the solution to use the **local filesystem** for easier testing and reproducibility on standard environments. All file paths are relative and can be modified to use `hdfs://` if required.



## ✅ How to Run

1. Clone the repo:
   ```bash
   git clone https://github.com/your-username/data-engineering-assignment.git
   cd data-engineering-assignment


