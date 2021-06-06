package com.fuljo.polimi.middleware.mask_the_week;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class MaskTheWeek {
    public static void main(String[] args) {
        // Disable too verbose logging
        Logger.getLogger("org").setLevel(Level.OFF);

        // Check command line parameters
        if (args.length < 2) {
            System.err.println("Usage: MaskTheWeek <in_file> <out_dir>");
            System.exit(1);
        }
        // Get file path
        final String filePath = args[0];
        final String outDir = args[1];

        // Load default configuration values
        SparkConf sc = new SparkConf()
                .setAppName("MaskTheWek")
                // If submitted to the cluster, the master is already set. Otherwise work locally
                .setIfMissing("spark.master", "local[1]");

        // Set up the Spark Session
        final SparkSession spark = SparkSession
                .builder()
                .config(sc)
                .getOrCreate();

        // Create a DataFrame of the COVID data
        Dataset<Row> df = readECDC(spark, filePath);

        // ---------------------------------------------------------------------------------
        // Seven days moving average of new reported cases, for each county and for each day
        // ---------------------------------------------------------------------------------
        // Specify sliding window
        Column w = window(col("date"), "7 days", "1 day");
        // Compute moving average
        df = df
                .groupBy(col("country"), w)
                .avg("cases");

        // ---------------------------------------------------------------------------------
        // Percentage increase of the average, w.r.t. the day before
        // ---------------------------------------------------------------------------------
        // WARNING: Spaghetti code ahead
        df = df
                .withColumn("avg(cases)_lag",
                        lag("avg(cases)", 1)
                                .over(Window.partitionBy("country").orderBy("window.start")))
                // increase[t] := (avg[t] - avg[t-1]) / avg[t-1] * 100
                .withColumn("avg(cases)_increase",
                        (col("avg(cases)").minus(col("avg(cases)_lag")))
                                .divide(col("avg(cases)_lag"))
                                .multiply(100.));

        df.cache();

        // Select columns to export
        Dataset<Row> df_avg = df.select(
                col("window.start").cast(DataTypes.DateType).as("start_date"),
                col("window.end").cast(DataTypes.DateType).as("end_date"),
                col("country"),
                col("avg(cases)"),
                col("avg(cases)_increase")
        );



        // Export to file
        df_avg
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .option("header", true)
                .option("dateFormat", "d/M/y")
                .save(outDir + "/avg");

        // ----------------------------------------------------------------------------------------------------
        // Top 10 countries with the highest percentage increase of the seven days moving average, for each day
        // ----------------------------------------------------------------------------------------------------
        df = df
                .withColumn("rank", rank()
                        .over(Window.partitionBy("window.end").orderBy("avg(cases)_increase")))
                .filter(col("rank").leq(10));
        // Select columns to export
        Dataset<Row> df_chart = df.select(
                col("window.start").cast(DataTypes.DateType).as("start_date"),
                col("window.end").cast(DataTypes.DateType).as("end_date"),
                col("country"),
                col("rank"),
                col("avg(cases)"),
                col("avg(cases)_increase")
        );

        // Export to file
        df_chart
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .option("header", true)
                .option("dateFormat", "d/M/y")
                .save(outDir + "/chart");
    }

    private static Dataset<Row> readECDC(SparkSession spark, String filePath) {
        // Create relevant schema
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("dateRep", DataTypes.DateType, false),
                DataTypes.createStructField("day", DataTypes.IntegerType, false),
                DataTypes.createStructField("month", DataTypes.IntegerType, false),
                DataTypes.createStructField("year", DataTypes.IntegerType, false),
                DataTypes.createStructField("cases", DataTypes.IntegerType, false),
                DataTypes.createStructField("deaths", DataTypes.IntegerType, false),
                DataTypes.createStructField("countriesAndTerritories", DataTypes.StringType, false),
                DataTypes.createStructField("geoId", DataTypes.StringType, true),
                DataTypes.createStructField("countryterritoryCode", DataTypes.StringType, true),
                DataTypes.createStructField("popData2019", DataTypes.IntegerType, true),
                DataTypes.createStructField("continentExp", DataTypes.StringType, true),
                DataTypes.createStructField("Cumulative_number_for_14_days_of_COVID-19_cases_per_100000", DataTypes.IntegerType, true),
        });

        // Read the data from file
        Dataset<Row> df = spark
                .read()
                .format("csv")
                .schema(schema)
                .option("header", true)
                .option("dateFormat", "d/M/y")
                .load(filePath);

        // Select only relevant columns and rename them
        df = df.select(
                col("dateRep").as("date"),
                col("cases"),
                col("deaths"),
                col("countriesAndTerritories").as("country")
        );
        return df;
    }
}
