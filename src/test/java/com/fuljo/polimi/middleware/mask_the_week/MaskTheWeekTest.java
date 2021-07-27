package com.fuljo.polimi.middleware.mask_the_week;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.*;

class MaskTheWeekTest {

    private static SparkSession spark;

    private static StructType initialSchema() {
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("date", DataTypes.DateType, false),
                DataTypes.createStructField("country", DataTypes.StringType, false),
                DataTypes.createStructField("cases", DataTypes.IntegerType, false),
        });
    }

    private static StructType avgSchema() {
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("start_date", DataTypes.DateType, false),
                DataTypes.createStructField("end_date", DataTypes.DateType, false),
                DataTypes.createStructField("country", DataTypes.StringType, false),
                DataTypes.createStructField("avg", DataTypes.DoubleType, false),
        });
    }

    private static StructType increaseSchema() {
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("start_date", DataTypes.DateType, false),
                DataTypes.createStructField("end_date", DataTypes.DateType, false),
                DataTypes.createStructField("country", DataTypes.StringType, false),
                DataTypes.createStructField("avg", DataTypes.DoubleType, false),
                DataTypes.createStructField("avg_increase", DataTypes.DoubleType, false),
        });
    }

    /**
     * Parses a date in the format yyyy-mm-dd
     *
     * @param date date string, in the format "yyyy-mm-dd"
     * @return a parsed SQL dat
     */
    public static Date parseDate(String date) {
        return Date.valueOf(date);
    }

    @BeforeAll
    static void setUp() {
        // Load default configuration values
        SparkConf sc = new SparkConf()
                .setAppName("MaskTheWek")
                // If submitted to the cluster, the master is already set. Otherwise use the provided one
                .setMaster("local[1]");
        // Set up the Spark Session
        spark = SparkSession
                .builder()
                .config(sc)
                .getOrCreate();
        // Disable too verbose logging
        spark.sparkContext()
                .setLogLevel("ERROR");
    }

    @AfterAll
    static void tearDown() {
        spark.close();
    }

    @Test
    void computeAvg_SingleCountry() {
        // Create dataset with schema: date, country, cases
        List<Row> rows = Arrays.asList(
                RowFactory.create(parseDate("2021-01-01"), "Italy", 10),
                RowFactory.create(parseDate("2021-01-02"), "Italy", 10),
                RowFactory.create(parseDate("2021-01-03"), "Italy", 10),
                RowFactory.create(parseDate("2021-01-04"), "Italy", 10),
                RowFactory.create(parseDate("2021-01-05"), "Italy", 10),
                RowFactory.create(parseDate("2021-01-06"), "Italy", 10),
                RowFactory.create(parseDate("2021-01-07"), "Italy", 10)
        );
        Dataset<Row> df = spark.createDataFrame(rows, initialSchema());

        // Compute the average over the week
        df = MaskTheWeek.computeAvg(df, 7, 1);
        // Collect to a list so we can check the rows
        // The schema is: start_date, end_date, country, avg
        df = df.orderBy("end_date");
        List<Row> results = df.collectAsList();

        // Check results row by row
        LocalDate localDate = LocalDate.parse("2021-01-01");
        String country = "Italy";
        double cumSum = 0.;
        for (Row r : results) {
            // Check start and end dates
            Date start_date = Date.valueOf(localDate.minusDays(7));
            Date end_date = Date.valueOf(localDate);
            assertEquals(start_date, r.get(0));
            assertEquals(end_date, r.get(1));

            // Check country
            assertEquals(country, r.get(2));

            // Check average
            if (localDate.isBefore(LocalDate.parse("2021-01-08"))) {
                cumSum += 10;
            } else {
                cumSum -= 10;
            }
            assertEquals(cumSum / 7, r.get(3));

            // Advance to next row
            localDate = localDate.plusDays(1);
        }
    }

    @Test
    void computeAvg_WeeklyData() {
        // Create dataset with schema: date, country, cases
        List<Row> rows = Arrays.asList(
                RowFactory.create(parseDate("2021-01-01"), "Italy", 7),
                RowFactory.create(parseDate("2021-01-08"), "Italy", 14),
                RowFactory.create(parseDate("2021-01-15"), "Italy", 21)
        );
        Dataset<Row> df = spark.createDataFrame(rows, initialSchema());

        // Compute the average over the week
        df = MaskTheWeek.computeAvg(df, 7, 1);
        // Collect to a list so we can check the rows
        // The schema is: start_date, end_date, country, avg
        df = df.orderBy("end_date");
        List<Row> results = df.collectAsList();

        // Check results row by row
        LocalDate localDate = LocalDate.parse("2021-01-01");
        String country = "Italy";
        for (Row r : results) {
            // Check start and end dates
            Date start_date = Date.valueOf(localDate.minusDays(7));
            Date end_date = Date.valueOf(localDate);
            assertEquals(start_date, r.get(0));
            assertEquals(end_date, r.get(1));

            // Check country
            assertEquals(country, r.get(2));

            // Check that the cases are spread throughout the week
            double avg;
            if (localDate.isBefore(LocalDate.parse("2021-01-08"))) {
                avg = 1;
            } else if (localDate.isBefore(LocalDate.parse("2021-01-15"))) {
                avg = 2;
            } else {
                avg = 3;
            }
            assertEquals(avg, r.get(3));

            // Advance to next row
            localDate = localDate.plusDays(1);
        }
    }

    @Test
    void computeAvg_MultipleCountries() {
        // Create dataset with schema: date, country, cases
        List<Row> rows = Arrays.asList(
                // Italy
                RowFactory.create(parseDate("2021-01-01"), "Italy", 1),
                RowFactory.create(parseDate("2021-01-02"), "Italy", 1),
                RowFactory.create(parseDate("2021-01-03"), "Italy", 1),
                RowFactory.create(parseDate("2021-01-04"), "Italy", 1),
                RowFactory.create(parseDate("2021-01-05"), "Italy", 1),
                RowFactory.create(parseDate("2021-01-06"), "Italy", 1),
                RowFactory.create(parseDate("2021-01-07"), "Italy", 1),
                // Spain
                RowFactory.create(parseDate("2021-01-01"), "Spain", 2),
                RowFactory.create(parseDate("2021-01-02"), "Spain", 2),
                RowFactory.create(parseDate("2021-01-03"), "Spain", 2),
                RowFactory.create(parseDate("2021-01-04"), "Spain", 2),
                RowFactory.create(parseDate("2021-01-05"), "Spain", 2),
                RowFactory.create(parseDate("2021-01-06"), "Spain", 2),
                RowFactory.create(parseDate("2021-01-07"), "Spain", 2)
        );
        Dataset<Row> df = spark.createDataFrame(rows, initialSchema());

        // Compute the average over the week
        df = MaskTheWeek.computeAvg(df, 7, 1);
        // Collect to a list so we can check the rows
        // The schema is: start_date, end_date, country, avg
        df = df.orderBy("end_date");

        // Check results country by country
        for (String country : new String[]{"Italy", "Spain"}) {
            List<Row> results = df
                    .filter(col("country").equalTo(country))
                    .collectAsList();
            // Check results row by row
            LocalDate localDate = LocalDate.parse("2021-01-01");
            double cumSum = 0;
            double dailyCases = country.equals("Italy") ? 1 : 2;
            for (Row r : results) {
                // Check start and end dates
                Date start_date = Date.valueOf(localDate.minusDays(7));
                Date end_date = Date.valueOf(localDate);
                assertEquals(start_date, r.get(0));
                assertEquals(end_date, r.get(1));

                // Check country
                assertEquals(country, r.get(2));

                // Check average
                if (localDate.isBefore(LocalDate.parse("2021-01-08"))) {
                    cumSum += dailyCases;
                } else {
                    cumSum -= dailyCases;
                }
                assertEquals(cumSum / 7, r.get(3));

                // Advance to next row
                localDate = localDate.plusDays(1);
            }
        }
    }

    @Test
    void computeAvgIncrease_SingleCountry() {
        // Create dataset with schema: start_date, end_date, country, avg
        List<Row> rows = Arrays.asList(
                // Italy
                RowFactory.create(parseDate("2021-01-01"), parseDate("2021-01-08"), "Italy", 1.),
                RowFactory.create(parseDate("2021-01-02"), parseDate("2021-01-09"), "Italy", 2.),
                RowFactory.create(parseDate("2021-01-03"), parseDate("2021-01-10"), "Italy", 3.),
                RowFactory.create(parseDate("2021-01-04"), parseDate("2021-01-11"), "Italy", 4.5),
                RowFactory.create(parseDate("2021-01-05"), parseDate("2021-01-12"), "Italy", 9.),
                RowFactory.create(parseDate("2021-01-06"), parseDate("2021-01-13"), "Italy", 6.),
                RowFactory.create(parseDate("2021-01-07"), parseDate("2021-01-14"), "Italy", 3.)
        );
        Dataset<Row> df = spark.createDataFrame(rows, avgSchema());

        // Compute the increment of the average w.r.t. the previous period
        df = MaskTheWeek.computeAvgIncrease(df);
        // Collect to a list so we can check the rows
        // The schema is: start_date, end_date, country, avg, avg_increase
        df = df.orderBy("end_date");
        List<Row> results = df.collectAsList();

        // Check the results
        LocalDate localDate = LocalDate.parse("2021-01-08");
        String country = "Italy";
        double[] avgs = new double[]{1., 2., 3., 4.5, 9., 6., 3.};
        for (int i = 0; i < results.size(); i++) {
            Row r = results.get(i);

            // Check start and end dates
            Date start_date = Date.valueOf(localDate.minusDays(7));
            Date end_date = Date.valueOf(localDate);
            assertEquals(start_date, r.get(0));
            assertEquals(end_date, r.get(1));

            // Check country
            assertEquals(country, r.get(2));

            // Check avg
            assertEquals(avgs[i], r.get(3));

            // Compute and check increment
            if (i > 0) { // Normal case
                double increment = (avgs[i] - avgs[i - 1]) / avgs[i - 1] * 100;
                assertEquals(increment, r.get(4));
            } else { // First row
                assertNull(r.get(4));
            }

            // Advance to next row
            localDate = localDate.plusDays(1);
        }
    }

    @Test
    void computeRank() {
        // Create dataset with schema: start_date, end_date, country, avg, avg_increase
        List<Row> rows = Arrays.asList(
                // Day 1
                RowFactory.create(parseDate("2021-01-01"), parseDate("2021-01-08"), "Italy", 1., null),
                RowFactory.create(parseDate("2021-01-01"), parseDate("2021-01-08"), "Spain", .5, null),
                RowFactory.create(parseDate("2021-01-01"), parseDate("2021-01-08"), "Germany", 1., null),
                RowFactory.create(parseDate("2021-01-01"), parseDate("2021-01-08"), "UK", 3., null),
                // Day 2
                RowFactory.create(parseDate("2021-01-02"), parseDate("2021-01-09"), "Italy", 2., 100.),
                RowFactory.create(parseDate("2021-01-02"), parseDate("2021-01-09"), "Spain", 4., 800.),
                RowFactory.create(parseDate("2021-01-02"), parseDate("2021-01-09"), "Germany", 1.25, 25.),
                RowFactory.create(parseDate("2021-01-02"), parseDate("2021-01-09"), "UK", 9., 150.),
                // Day 3
                RowFactory.create(parseDate("2021-01-03"), parseDate("2021-01-10"), "Italy", 3., 50.),
                RowFactory.create(parseDate("2021-01-03"), parseDate("2021-01-10"), "Spain", 5., 25.),
                RowFactory.create(parseDate("2021-01-03"), parseDate("2021-01-10"), "Germany", 2.5, 100.),
                RowFactory.create(parseDate("2021-01-03"), parseDate("2021-01-10"), "UK", 4.5, -50.)
        );
        Dataset<Row> df = spark.createDataFrame(rows, increaseSchema());

        // Compute the rank of the countries with the highest increase for each period
        df = MaskTheWeek.computeRank(df, 4);

        // Check each day
        LocalDate localDate = LocalDate.parse("2021-01-07");
        for (int day = 0; day < 3; day++) {
            // Compute start and end dates
            Date start_date = Date.valueOf(localDate.minusDays(7));
            Date end_date = Date.valueOf(localDate);

            // Get the records for this time period
            Dataset<Row> dfp = df.where(col("end_date").equalTo(end_date));
            // Collect to a list so we can check the rows
            // The schema is: start_date, end_date, country, avg, avg_increase, rank
            List<Row> results = dfp.collectAsList();

            // Iterate over countries
            for (int i = 0; i < results.size(); i++) {
                Row r = results.get(i);

                // Check dates
                assertEquals(start_date, r.get(0));
                assertEquals(end_date, r.get(1));

                switch (day) {
                    case 0: { // Day 1
                        // Since the increment is null, the order of countries is irrelevant
                        assertTrue(r.get(2).equals("Italy") || r.get(2).equals("Germany") || r.get(2).equals("UK") || r.get(2).equals("Spain"));
                        assertTrue(r.getInt(5) < 4);
                        break;
                    }
                    case 1: { // Day 2
                        switch (i) {
                            case 0: {
                                assertEquals("Spain", r.get(2));
                                assertEquals(1, r.getInt(5));
                                break;
                            }
                            case 1: {
                                assertEquals("UK", r.get(2));
                                assertEquals(2, r.getInt(5));
                                break;
                            }
                            case 2: {
                                assertEquals("Italy", r.get(2));
                                assertEquals(3, r.getInt(5));
                                break;
                            }
                            case 3: {
                                assertEquals("Germany", r.get(2));
                                assertEquals(4, r.getInt(5));
                                break;
                            }
                            default:
                                fail();
                        }
                        break;
                    }
                    case 2: { // Day 3
                        switch (i) {
                            case 0: {
                                assertEquals("Germany", r.get(2));
                                assertEquals(1, r.getInt(5));
                                break;
                            }
                            case 1: {
                                assertEquals("Italy", r.get(2));
                                assertEquals(2, r.getInt(5));
                                break;
                            }
                            case 2: {
                                assertEquals("Spain", r.get(2));
                                assertEquals(3, r.getInt(5));
                                break;
                            }
                            case 3: {
                                assertEquals("UK", r.get(2));
                                assertEquals(4, r.getInt(5));
                                break;
                            }
                            default:
                                fail();
                        }
                        break;
                    }
                    default:
                        fail();
                }
            }
        }
    }
}