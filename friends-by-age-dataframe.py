import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from typing import Optional


def configure_logging() -> None:
    """
    Configures the logging settings for the script.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def load_data(spark: SparkSession, file_path: str) -> Optional[DataFrame]:
    """
    Loads data from a CSV file into a DataFrame.

    Args:
    spark (SparkSession): The SparkSession object.
    file_path (str): The path to the CSV file.

    Returns:
    Optional[DataFrame]: The loaded DataFrame or None if there is an error.
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


def calculate_average_friends_by_age(df: DataFrame) -> DataFrame:
    """
    Groups the DataFrame by age and calculates the average number of friends.

    Args:
    df (DataFrame): The input DataFrame.

    Returns:
    DataFrame: A DataFrame with the average number of friends by age.
    """
    return df.groupBy("age").agg(
        func.round(func.avg("friends"), 2).alias("friends_avg")
    )


def main() -> None:
    """
    Main function to configure Spark, load data, and perform various DataFrame operations.

    Steps:
    1. Configure logging for the script.
    2. Create a SparkSession.
    3. Load the fake friends dataset.
    4. Select the age and numFriends columns.
    5. Calculate the average number of friends by age.
    6. Display the results sorted by age.
    7. Stop the Spark session.
    """
    configure_logging()

    try:
        # Step 2: Create a SparkSession
        spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()
        logging.info("Spark session created.")
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        return

    try:
        # Step 3: Load the fake friends dataset
        file_path = "file:///SparkCourse/SparkSQL/fakefriends-header.csv"
        df: Optional[DataFrame] = load_data(spark, file_path)
        if df is None:
            raise ValueError("DataFrame is None, exiting.")
    except Exception as e:
        logging.error(f"Error during data loading and validation: {e}")
        return

    try:
        # Step 4: Select the age and numFriends columns
        friends_by_age = df.select("age", "friends")

        # Step 5: Calculate the average number of friends by age
        average_friends_by_age = calculate_average_friends_by_age(friends_by_age)

        # Step 6: Display the results sorted by age
        logging.info("Displaying the average number of friends by age:")
        average_friends_by_age.sort("age").show()
    except Exception as e:
        logging.error(f"Error during DataFrame operations: {e}")
    finally:
        # Step 7: Stop the Spark session
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    main()
