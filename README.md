# SAMA-Web-Scraping


This project automates the scraping, transformation, and loading of monthly statistical data from the [Saudi Central Bank (SAMA)](https://www.sama.gov.sa/ar-sa/EconomicReports/Pages/MonthlyStatistics.aspx) into a SQL Server database. The process is fully automated using **SQL Server Integration Services (SSIS)**, scheduled via **SQL Server Management Studio (SSMS)**, and includes logic to avoid redundant data insertion. 

## Features
- **Web Scraping**: Automatically scrape and download the latest Excel files from the SAMA website.
- **ETL Pipeline**: Extract, transform, and load (ETL) the data into a structured SQL Server database for analysis.
- **Error Handling**: Ensures robust processing with logging and recovery mechanisms for failures.
- **Comprehensive Documentation**: Includes detailed documentation for the web scraping and ETL scripts.  

---

## Documentation  

The project includes a **Documentation** folder that contains detailed explanations of the scripts used in the automation process.  

### Files in the Documentation Folder:  

1. **`Scraping_SAMA_Data.md`**  
   - Explains the **`Scraping_SAMA_Data.py`** script.  
   - Details the process of scraping `.xlsx` files from the SAMA website, including how the script handles downloading, archiving, and error management.  

2. **`SAMA_ETL_Documentation.md`**  
   - Documents the **`SAMA_refactor-V2.py`** script.  
   - Provides an overview of the ETL pipeline, including data extraction, transformation, and loading processes, with specific focus on the logic for deduplicating data during loading into the database.  

These documents serve as a comprehensive guide for understanding, maintaining, and extending the functionality of the scripts.  

---
## Process Overview

### 1. **Web Scraping and File Download**
The scraping process involves the following steps:
- Check if the archive directory exists. If not, create it.
- Access the SAMA website and parse the HTML to locate links to the `.xlsx` files.
- Construct the file URL and download it if it does not already exist in the archive directory.
- Log success or handle errors for missing or inaccessible files.

**Flowchart**:

![Scraping Flowchart](Documentation/Scraping_Flowchart.png)

### 2. **ETL Process**
After the files are downloaded, the ETL pipeline processes them:
- **Extract**: Read the downloaded `.xlsx` files.
- **Transform**: Cleanse and structure the data to make it ready for database insertion.
- **Load**: Insert only **new records** into the main database table by checking for existing data using the provided query logic:  

```sql  
-- Insert new records where the combination of 'Yearnum' and 'Qurternum' does not exist  
INSERT INTO {schema_name}.{table_name} ({', '.join(df.columns)})  
SELECT {', '.join(df.columns)}  
FROM {schema_name}.{temp_table_name} AS temp  
WHERE NOT EXISTS (  
    SELECT 1  
    FROM {schema_name}.{table_name} AS main  
    WHERE main.Yearnum = temp.Yearnum  
    AND main.Qurternum = temp.Qurternum  
)
```  
This ensures:  
1. Duplicate records are avoided by checking the `Yearnum` and `Qurternum` columns in the main table.  
2. Only new records from the temporary table are added to the main table.  
3. Improved performance and data consistency.

**Flowchart**:

![ETL Process Flowchart](Documentation/etl_process_flowchart.png)

### 3. **Scheduling with SSMS**
The entire process (scraping and ETL) is scheduled to run periodically using SQL Server Agent in SSMS:
- Create a **SQL Server Agent Job** for the SSIS package.
- Define a monthly schedule to trigger the process.
- Monitor job status and execution history directly within SSMS.
- SAMA Container from SSIS Package which runs web scraping process and if successful then SAMA ETL process:  
![SAMA Container](Documentation/SSIS Package.png)
---
---

## Requirements
- **SQL Server**:
  - SQL Server Integration Services (SSIS)
  - SQL Server Agent (for scheduling)
- **Tools**:
  - Python/.NET (optional, for scripting within SSIS packages)
  - SQL Server Management Studio (SSMS)
- **Dependencies**:
  - `requests`, `BeautifulSoup`, and `pandas` if Python is used for scraping.
  - SQL Server ODBC/ADO.NET connection drivers for database integration.

---

## Error Handling
- **Scraping Errors**: Logs errors if the `.xlsx` file links are not found or downloads fail.
- **ETL Errors**: Captures issues during file reading, transformation, or loading into the database.
- **Recovery**: Automatically retries operations or gracefully skips failed files while logging details for review.

---

