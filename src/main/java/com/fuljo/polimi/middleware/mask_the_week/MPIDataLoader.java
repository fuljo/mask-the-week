package com.fuljo.polimi.middleware.mask_the_week;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;

import static org.apache.spark.sql.functions.*;

/**
 * Loads CSV reports from the simulator "My Population Infection" into DataFrames.
 * Learn more about the simulator at its <a href="https://github.com/fuljo/my-population-infection">GitHub repo</a>.
 * <p>
 * The files are expected to have the following schema:
 * <ul>
 * <li>day: integer (zero-based) </li>
 * <li>country: integer </li>
 * <li>susceptible: integer </li>
 * <li>infected: integer </li>
 * <li>immune: integer </li>
 * </ul>
 * However only the fields {@code day}, {@code country} and {@code infected} are actually used.
 */
public class MPIDataLoader {

    public static StructType getSchema() {
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("day", DataTypes.IntegerType, false),
                DataTypes.createStructField("country", DataTypes.IntegerType, false),
                DataTypes.createStructField("susceptible", DataTypes.IntegerType, false),
                DataTypes.createStructField("infected", DataTypes.IntegerType, false),
                DataTypes.createStructField("immune", DataTypes.IntegerType, false),
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
                .load(filePaths);

        // Adapt the data format
        df = postProcess(df);
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
                .load(filePath);

        // Adapt the data format
        df = postProcess(df);
        return df;
    }

    private static Dataset<Row> postProcess(Dataset<Row> df) {
        // We need to convert progressive days to date format,
        // so we select a conventional start date
        Date startDate = Date.valueOf("1970-01-01");

        // Select only relevant columns and rename them
        df = df.select(
                // Add number of days to start date
                date_add(lit(startDate), col("day")).as("date"),
                // Rename
                col("infected").as("cases"),
                // Cast to string
                col("country").cast(DataTypes.StringType)
        );
        return df;
    }
}
