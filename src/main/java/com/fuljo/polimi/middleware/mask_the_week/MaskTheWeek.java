package com.fuljo.polimi.middleware.mask_the_week;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import static org.apache.spark.sql.functions.*;

@Command(name = "mask-the-week", mixinStandardHelpOptions = true, version = "1.0-SNAPSHOT",
        description = "Analyze COVID-19 daily cases reports, per country.")
public class MaskTheWeek implements Runnable {

    /**
     * Injected by PicoCLI
     */
    @Spec
    CommandSpec spec;

    private String[] filePaths;

    private CSVFormat format;

    private Integer windowDuration;

    private Integer slideDuration;

    private Integer rankLimit;

    private String master;

    private String resultsDir;

    /**
     * Creates a new spark session for this application.
     * <p>
     * The configuration is initialized with default values, which may have been taken from the environment or
     * configuration files. Optionally the URL of the master can be overridden.
     *
     * @param master URL of the master, if empty local[1] will be used
     * @return a new spark session
     */
    static SparkSession createSession(String master) {
        // If no master is provided, run locally
        if (master.length() == 0) {
            master = "local[1]";
        }
        // Load default configuration values
        SparkConf sc = new SparkConf()
                .setAppName("MaskTheWek")
                // If submitted to the cluster, the master is already set. Otherwise use the provided one
                .setIfMissing("spark.master", master);
        // Set up the Spark Session
        return SparkSession
                .builder()
                .config(sc)
                .getOrCreate();
    }

    /**
     * Runs the application in "batch" mode.
     * It assumes that the object's attributes have been set by PicoCLI.
     */
    public void run() {
        // Create the session to work on
        SparkSession spark = createSession(master);

        // Load data on a DataFrame, according to the specified format
        Dataset<Row> df;
        switch (format) {
            case ECDC: {
                df = ECDCDataLoader.read(spark, filePaths);
                break;
            }
            case MPI: {
                df = MPIDataLoader.read(spark, filePaths);
                break;
            }
            default: {
                throw new UnsupportedOperationException(String.format("CSV format %s not supported.", format));
            }
        }

        // Compute average and increase
        df = computeAvg(df, windowDuration, slideDuration);
        df = computeAvgIncrease(df);

        // Cache and export to CSV
        df.cache();
        df
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .option("header", true)
                .option("dateFormat", "d/M/y")
                .save(String.format("%s/avg", resultsDir));

        // Compute rank of countries with highest daily increase of the avg weekly cases
        df = computeRank(df, rankLimit);

        // Export rank to CSV
        df
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .option("header", true)
                .option("dateFormat", "d/M/y")
                .save(String.format("%s/rank", resultsDir));

        // End the session
        spark.close();
    }

    /**
     * Computes the moving average cases for each country over a sliding window.
     * <p>
     * Required input schema:
     * <ul>
     * <li>date: date (DD/MM/YYYY) </li>
     * <li>cases: integer </li>
     * <li>country: string </li>
     * </ul>
     * <p>
     * Output schema:
     * <ul>
     * <li>start_date: date (DD/MM/YYYY) </li>
     * <li>end_date: date (DD/MM/YYYY) </li>
     * <li>country: string </li>
     * <li>avg: float (the moving average) </li>
     * </ul>
     *
     * @param df             input dataframe
     * @param windowDuration size of the sliding window in days (> 0) (e.g. 7)
     * @param slideDuration  slide of the sliding window in days (> 0, must divide the window size)
     * @return a dataframe
     */
    static Dataset<Row> computeAvg(Dataset<Row> df, Integer windowDuration, Integer slideDuration) {
        String wd = String.format("%s days", windowDuration);
        String sd = String.format("%s days", slideDuration);
        // ---------------------------------------------------------------------------------
        // Seven days moving average of new reported cases, for each county and for each day
        // ---------------------------------------------------------------------------------
        // Specify sliding window
        Column w = window(col("date"), wd, sd);
        // Compute moving average
        df = df
                .groupBy(col("country"), w)
                .agg(sum("cases").divide(windowDuration / slideDuration).as("avg"));

        // Flatten the window column
        return df.select(
                col("window.start").cast(DataTypes.DateType).as("start_date"),
                col("window.end").cast(DataTypes.DateType).as("end_date"),
                col("country"),
                col("avg")
        );
    }

    /**
     * Computes the increment of the moving average for each country, for each window interval.
     * <p>
     * Required input schema:
     * <ul>
     * <li>start_date: date (DD/MM/YYYY) </li>
     * <li>end_date: date (DD/MM/YYYY) </li>
     * <li>country: string </li>
     * <li>avg: float (the moving average) </li>
     * </ul>
     * <p>
     * Output schema:
     * <ul>
     * <li>start_date: date (DD/MM/YYYY) </li>
     * <li>end_date: date (DD/MM/YYYY) </li>
     * <li>country: string </li>
     * <li>avg: float (the moving average) </li>
     * <li>avg_increase: float (the moving average) </li>
     * </ul>
     *
     * @param df dataframe with average already calculated
     * @return a dataframe
     */
    static Dataset<Row> computeAvgIncrease(Dataset<Row> df) {
        // ---------------------------------------------------------------------------------
        // Percentage increase of the average, w.r.t. the day before
        // ---------------------------------------------------------------------------------
        df = df
                .withColumn("avg_lag",
                        lag("avg", 1)
                                .over(Window.partitionBy("country").orderBy("end_date")))
                // increase[t] := (avg[t] - avg[t-1]) / avg[t-1] * 100
                .withColumn("avg_increase",
                        (col("avg").minus(col("avg_lag")))
                                .divide(col("avg_lag"))
                                .multiply(100.));
        // Remove temporary column
        return df.drop("avg_lag");
    }

    /**
     * Computes a rank of the countries with the highest avg. increase, for each interval.
     * <p>
     * Required input schema:
     * <ul>
     * <li>start_date: date (DD/MM/YYYY) </li>
     * <li>end_date: date (DD/MM/YYYY) </li>
     * <li>country: string </li>
     * <li>avg: float (the moving average) </li>
     * <li>avg_increase: float (the moving average) </li>
     * </ul>
     * <p>
     * Output schema:
     * <ul>
     * <li>start_date: date (DD/MM/YYYY) </li>
     * <li>end_date: date (DD/MM/YYYY) </li>
     * <li>country: string </li>
     * <li>avg: float (the moving average) </li>
     * <li>avg_increase: float (the moving average) </li>
     * <li>rank: integer </li>
     * </ul>
     *
     * @param df    input dataframe
     * @param limit size of the rank, for each interval
     *              {@code limit = 10} will output the first 10 countries
     * @return a dataframe
     */
    static Dataset<Row> computeRank(Dataset<Row> df, Integer limit) {
        // ----------------------------------------------------------------------------------------------------
        // Top 10 countries with the highest percentage increase of the seven days moving average, for each day
        // ----------------------------------------------------------------------------------------------------
        return df
                .withColumn("rank", rank()
                        .over(Window.partitionBy("end_date").orderBy(desc_nulls_last("avg_increase"))))
                .filter(col("rank").leq(limit - 1));
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(MaskTheWeek.class)
                .setCaseInsensitiveEnumValuesAllowed(true)
                .execute(args);
        System.exit(exitCode);
    }

    // ---------------------------------------------------------
    // Options definitions
    // ---------------------------------------------------------
    @Parameters(arity = "1..*", paramLabel = "FILE", description = "List of source CSV files")
    public void setFilePaths(String[] filePaths) {
        this.filePaths = filePaths;
    }

    public String[] getFilePaths() {
        return filePaths;
    }

    @Option(names = {"-F", "--format"}, paramLabel = "FMT", type = {CSVFormat.class}, defaultValue = "ECDC",
            description = "Schema of the CSV file. Allowed values: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})")
    public void setFormat(CSVFormat format) {
        this.format = format;
    }

    public CSVFormat getFormat() {
        return format;
    }

    @Option(names = {"-W", "--window-duration"}, paramLabel = "N", defaultValue = "7",
            description = "Compute average over N-days window (default: ${DEFAULT-VALUE})")
    public void setWindowDuration(Integer windowDuration) {
        if (windowDuration <= 0) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    "Invalid value %s for option '--window-duration': value should be non-negative");
        }
        this.windowDuration = windowDuration;
    }

    public Integer getWindowDuration() {
        return windowDuration;
    }

    @Option(names = {"-S", "--slide-duration"}, paramLabel = "M", defaultValue = "1",
            description = "Slide window M days at a time (default: ${DEFAULT-VALUE})")
    public void setSlideDuration(Integer slideDuration) {
        if (windowDuration <= 0) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    "Invalid value %s for option '--slide-duration': value should be non-negative");
        }
        this.slideDuration = slideDuration;
    }

    public Integer getSlideDuration() {
        return slideDuration;
    }

    @Option(names = {"-L", "--rank-limit"}, paramLabel = "T", defaultValue = "10",
            description = "The rank will include the T top countries (default: ${DEFAULT-VALUE})")
    public void setRankLimit(Integer rankLimit) {
        this.rankLimit = rankLimit;
    }

    public Integer getRankLimit() {
        return rankLimit;
    }

    @Option(names = {"--master"}, paramLabel = "URL", defaultValue = "",
            description = "URL of the master. Does not override setting --master on spark-submit.")
    public void setMaster(String master) {
        this.master = master;
    }

    public String getMaster() {
        return master;
    }

    @Option(names = {"-d", "--results-dir"}, paramLabel = "DIR", defaultValue = "results",
            description = "Directory where to save results (default: ${DEFAULT-VALUE})")
    public void setResultsDir(String resultsDir) {
        this.resultsDir = resultsDir;
    }

    public String getResultsDir() {
        return resultsDir;
    }

}
