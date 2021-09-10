

<br>

# Data Engineering Interview Assignment | Wave HQ

<br>

### *Arsalan Noorafkan*

**2021-09-10**
<br>
<br>



# **Instructions**
## Specifications

- ETL script is written in Python
    - Python libraries include Pandas, PySpark, Requests, Glob, and SQLAlchemy






<br> 

## Steps
The ETL process comprises of the following steps:
1) Make API calls to extract dimension tables and fact data
2) Store raw dimension and fact data as partitioned CSV files
3) Load partitioned CSV files into SQLite database for staging and analysis
4) Query the specific questions using SQL and show results in console







<br>


## Testing Script
Follow the steps below to test the ETL process using sample JSON data files.

1) Install Python libraries

2) Open a terminal window and cd to the 'pipeline' folder that contains the etl.py and query.py files
    > cd c:/usr/documents/Project/pipeline

3) Run the elt.py script to extract data from API and load into datalake
    > python etl.py

3) Run the query.py script to extract data from datalake and load into SQLite to run queries
    > python query.py


<br>

Please see the "output.txt" file for an example of the console log of the pipeline after a test run.


<br>
<br>

---


<br>

