package com.fuljo.polimi.middleware.mask_the_week;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

/**
 * Loads CSV reports from the European Center for Disease Control (ECDC) into DataFrames.
 * <p>
 * The files are expected to have the following schema:
 * <ul>
 * <li>dateRep: date (DD/MM/YYYY) </li>
 * <li>day: integer </li>
 * <li>month: integer </li>
 * <li>year: integer </li>
 * <li>cases: integer </li>
 * <li>deaths: integer </li>
 * <li>countriesAndTerritories: string </li>
 * <li>geoId: string </li>
 * <li>countryterritoryCode: string </li>
 * <li>popData2019: integer </li>
 * <li>continentExp: string </li>
 * <li>Cumulative_number_for_14_days_of_COVID-19_cases_per_100000: integer </li>
 * </ul>
 * However only the fields {@code dateRep}, {@code cases} and {@code countriesAndTerritories} are actually used.
 */
public class ECDCDataLoader {

    public static StructType getSchema() {
        return DataTypes.createStructType(new StructField[]{
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
                DataTypes.createStructField("Cumulative_number_for_14_days_of_COVID-19_cases_per_100000",
                        DataTypes.IntegerType, true),
        });
    }

    /**
     * Load CSV files into a single DataFrame.
     * <p>
     * The resulting DataFrame will have the following schema:
     * <ul>
     * <li>date: date (DD/MM/YYYY) </li>
     * <li>cases: integer </li>
     * <li>country: string </li>
     * </ul>
     *
     * @param spark     the current Spark session
     * @param filePaths paths of the CSV files
     * @return a DataFrame
     */
    public static Dataset<Row> read(SparkSession spark, String[] filePaths) {
        // Create relevant schema
        StructType schema = getSchema();

        // Read the data from file
        Dataset<Row> df = spark
                .read()
                .format("csv")
                .schema(schema)
                .option("header", true)
                .option("dateFormat", "d/M/y")
                .load(filePaths);


        // Select only relevant columns and rename them
        df = df.select(
                col("dateRep").as("date"),
                col("cases"),
                col("countriesAndTerritories").as("country")
        );
        return df;
    }

    public static Dataset<Row> readStream(SparkSession spark, String filePath) {
        // Create relevant schema
        StructType schema = getSchema();

        // Read the data from file
        Dataset<Row> df = spark
                .readStream()
                .format("csv")
                .schema(schema)
                .option("header", true)
                .option("dateFormat", "d/M/y")
                .load(filePath);


        // Select only relevant columns and rename them
        df = df.select(
                col("dateRep").as("date"),
                col("cases"),
                col("countriesAndTerritories").as("country")
        );
        return df;
    }
}
