import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)
from typing import Optional


def configure_logging() -> None:
    """
    Configures the logging settings for the script.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def load_data(
    spark: SparkSession, file_path: str, schema: StructType
) -> Optional[DataFrame]:
    """
    Loads data from a CSV file into a DataFrame with the specified schema.

    Args:
    spark (SparkSession): The SparkSession object.
    file_path (str): The path to the CSV file.
    schema (StructType): The schema for the DataFrame.

    Returns:
    Optional[DataFrame]: The loaded DataFrame or None if there is an error.
    """
    try:
        df = spark.read.schema(schema).csv(file_path)
        logging.info("Data loaded successfully.")
        return df
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return None


def filter_min_temps(df: DataFrame) -> DataFrame:
    """
    Filters the DataFrame to include only TMIN entries.

    Args:
    df (DataFrame): The input DataFrame.

    Returns:
    DataFrame: A DataFrame containing only TMIN entries.
    """
    return df.filter(df.measure_type == "TMIN")


def calculate_min_temps_by_station(df: DataFrame) -> DataFrame:
    """
    Calculates the minimum temperature for each station.

    Args:
    df (DataFrame): The input DataFrame containing TMIN entries.

    Returns:
    DataFrame: A DataFrame with the minimum temperature for each station.
    """
    return df.groupBy("stationID").min("temperature")


def convert_to_fahrenheit(df: DataFrame) -> DataFrame:
    """
    Converts the temperature from Celsius to Fahrenheit and sorts the DataFrame.

    Args:
    df (DataFrame): The input DataFrame with temperatures in Celsius.

    Returns:
    DataFrame: A DataFrame with temperatures in Fahrenheit, sorted by temperature.
    """
    return (
        df.withColumn(
            "temperature",
            func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2),
        )
        .select("stationID", "temperature")
        .sort("temperature")
    )


def main() -> None:
    """
    Main function to configure Spark, load data, and perform temperature analysis.

    Steps:
    1. Configure logging for the script.
    2. Create a SparkSession.
    3. Define the schema for the CSV file.
    4. Load the temperature data.
    5. Display the schema of the loaded data.
    6. Filter the data to include only TMIN entries.
    7. Calculate the minimum temperature for each station.
    8. Convert temperatures to Fahrenheit and sort the results.
    9. Collect and print the results.
    10. Stop the Spark session.
    """
    configure_logging()

    try:
        # Step 2: Create a SparkSession
        spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()
        logging.info("Spark session created.")
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        return

    try:
        # Step 3: Define the schema for the CSV file
        schema = StructType(
            [
                StructField("stationID", StringType(), True),
                StructField("date", IntegerType(), True),
                StructField("measure_type", StringType(), True),
                StructField("temperature", FloatType(), True),
            ]
        )

        # Step 4: Load the temperature data
        file_path = "file:///SparkCourse/SparkSQL/1800.csv"
        df: Optional[DataFrame] = load_data(spark, file_path, schema)
        if df is None:
            raise ValueError("DataFrame is None, exiting.")
    except Exception as e:
        logging.error(f"Error during data loading and validation: {e}")
        return

    try:
        # Step 5: Display the schema of the loaded data
        logging.info("Displaying the schema of the loaded data:")
        df.printSchema()

        # Step 6: Filter the data to include only TMIN entries
        min_temps = filter_min_temps(df)

        # Step 7: Calculate the minimum temperature for each station
        min_temps_by_station = calculate_min_temps_by_station(min_temps)

        # Step 8: Convert temperatures to Fahrenheit and sort the results
        min_temps_by_station_f = convert_to_fahrenheit(min_temps_by_station)

        # Step 9: Collect and print the results
        results = min_temps_by_station_f.collect()
        for result in results:
            logging.info(f"{result['stationID']}\t{result['temperature']:.2f}F")
    except Exception as e:
        logging.error(f"Error during DataFrame operations: {e}")
    finally:
        # Step 10: Stop the Spark session
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    main()
