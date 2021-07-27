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

4. Show the average and average increase for each period/country
   ```
   column -t -s, results/avg/*.csv
   ```

5. Show the countries with the highest average increase for each period
   ```
   column -t -s, results/rank/*.csv
   ```

## Monitoring the cluster
After launching the computation with Docker, one can access:
- the master UI at [localhost:8080](http://localhost:8080), to monitor the active jobs.
- the history UI at [localhost:18080](http://localhost:18080), for a post-mortem analysis of the whole job and its tasks.

NOTE: The worker UIs are not exposed, since the ports of the different replicas would clash.