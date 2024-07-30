import logging
from pyspark.sql import SparkSession, DataFrame


def configure_logging() -> None:
    """
    Configures the logging settings for the script.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def load_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Loads data from a CSV file into a DataFrame.

    Args:
    spark (SparkSession): The SparkSession object.
    file_path (str): The path to the CSV file.

    Returns:
    DataFrame: The loaded DataFrame.
    """
    try:
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(file_path)
        )
        logging.info("Data loaded successfully.")
        return df
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return None


def main() -> None:
    """
    Main function to configure Spark, load data, and perform various DataFrame operations.

    Steps:
    1. Configure logging for the script.
    2. Create a SparkSession.
    3. Load the fake friends dataset.
    4. Display the inferred schema.
    5. Display the name column.
    6. Filter out anyone over 21 and display the result.
    7. Group by age and display the count.
    9. Stop the Spark session.
    """
    configure_logging()

    try:
        # Step 2: Create a SparkSession
        spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
        logging.info("Spark session created.")
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        return

    try:
        # Step 3: Load the fake friends dataset
        file_path = "file:///SparkCourse/SparkSQL/fakefriends-header.csv"
        people: DataFrame = load_data(spark, file_path)
        if people is None:
            raise ValueError("DataFrame is None, exiting.")
    except Exception as e:
        logging.error(f"Error during data loading and validation: {e}")
        return

    try:
        # Step 4: Display the inferred schema
        logging.info("Here is our inferred schema:")
        people.printSchema()

        # Step 5: Display the name column
        logging.info("Displaying the name column:")
        people.select("name").show()

        # Step 6: Filter out anyone over 21 and display the result
        logging.info("Filtering out anyone over 21:")
        people.filter(people.age < 21).show()

        # Step 7: Group by age and display the count
        logging.info("Grouping by age:")
        people.groupBy("age").count().show()

    except Exception as e:
        logging.error(f"Error during DataFrame operations: {e}")
    finally:
        # Step 9: Stop the Spark session
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    main()
