# Mask the Week
Analysis of Covid-19 data with Apache Spark

## Getting started
1. Download the daily cases report from the *European Centre for Disease Prevention and Control* (ECDC):
    ```
    mkdir data
    curl -o data/data.csv https://opendata.ecdc.europa.eu/covid19/casedistribution/csv/data.csv
    ```
2. Create a directory for the results
   ```
   mkdir results
   ```
3. Run the application with Docker Compose
   ```
   docker compose up --build --scale spark-worker=<num_workers>
   ```