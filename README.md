# Mask the Week
Analysis of COVID-19 data with the Apache Spark SQL API.

We consider a dataset that reports the new daily COVID-19 cases for each country, worldwide.
Our goal is to compute:

- The seven-days *moving average* of new cases, for each day and for each country
- The *percentage increase* of the seven-days average w.r.t. the day before, for each day and for each country
- The *top-10 countries* with the highest percentage increase, for each day

If data is provided with a coarser grain (e.g. weekly), the increment is spread evenly between the week when computing the average.

Read the [project report](/releases/latest/download/mtw_report) for more detailed information.

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

## Parameters
The parameters are given as command line options to the main class `com.fuljo.polimi.middleware.mask_the_week.MaskTheWeek`.

Here is the help text:
```
Usage: mask-the-week [-hV] [-d=DIR] [-F=FMT] [-L=T] [--master=URL] [-S=M]
                     [-W=N] FILE...
Analyze COVID-19 daily cases reports, per country.
      FILE...               List of source CSV files
  -d, --results-dir=DIR     Directory where to save results (default: results)
  -F, --format=FMT          Schema of the CSV file. Allowed values: ECDC, MPI
                              (default: ECDC)
  -h, --help                Show this help message and exit.
  -L, --rank-limit=T        The rank will include the T top countries (default:
                              10)
      --master=URL          URL of the master. Does not override setting
                              --master on spark-submit.
  -S, --slide-duration=M    Slide window M days at a time (default: 1)
  -V, --version             Print version information and exit.
  -W, --window-duration=N   Compute average over N-days window (default: 7)
```

Regarding the two CSV formats:
- **ECDC** is the schema used by the [dataset linked above](https://opendata.ecdc.europa.eu/covid19/casedistribution/csv/data.csv)
- **MPI** is the one produced by [My Population Infection](https://github.com/fuljo/my-population-infection) simulator, also developed by us

## Monitoring the cluster
After launching the computation with Docker, one can access:
- the master UI at [localhost:8080](http://localhost:8080), to monitor the active jobs.
- the history UI at [localhost:18080](http://localhost:18080), for a post-mortem analysis of the whole job and its tasks.

NOTE: The worker UIs are not exposed, since the ports of the different replicas would clash.

## Report
A PDF project report describing the program, its design principles, an execution analysis and performance analysis can be downloaded [here](/releases/latest/download/mtw_report.pdf).
Alternatively it can be locally compiled with XeLaTeX:
```
cd report
latexmk
```

## Credits
This project was created as part of the exam for the 2020 edition of the *Middleware Technologies for Distributed Systems* at Politecnico di Milano.

The Docker images used for the cluster are released and maintained at [fuljo/docker-spark](https://github.com/fuljo/docker-spark). 

## License
This program (excluding external data and libraries) is distributed under the MIT license.