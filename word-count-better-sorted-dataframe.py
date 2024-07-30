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
    Loads data from a text file into a DataFrame.

    Args:
    spark (SparkSession): The SparkSession object.
    file_path (str): The path to the text file.

    Returns:
    Optional[DataFrame]: The loaded DataFrame or None if there is an error.
    """
    try:
        df = spark.read.text(file_path)
        logging.info("Data loaded successfully.")
        return df
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return None


def process_words(df: DataFrame) -> DataFrame:
    """
    Processes the DataFrame to extract words, normalize to lowercase, and count occurrences.

    Args:
    df (DataFrame): The input DataFrame.

    Returns:
    DataFrame: A DataFrame with the word counts.
    """
    # Split using a regular expression that extracts words
    words = df.select(func.explode(func.split(df.value, "\\W+")).alias("word"))
    words_without_empty_string = words.filter(words.word != "")

    # Normalize everything to lowercase
    lowercase_words = words_without_empty_string.select(
        func.lower(words_without_empty_string.word).alias("word")
    )

    # Count up the occurrences of each word
    word_counts = lowercase_words.groupBy("word").count()

    return word_counts


def main() -> None:
    """
    Main function to configure Spark, load data, and perform word count operations.

    Steps:
    1. Configure logging for the script.
    2. Create a SparkSession.
    3. Load the book text data.
    4. Process the data to count word occurrences.
    5. Sort the word counts.
    6. Display the sorted word counts.
    7. Stop the Spark session.
    """
    configure_logging()

    try:
        # Step 2: Create a SparkSession
        spark = SparkSession.builder.appName("WordCount").getOrCreate()
        logging.info("Spark session created.")
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        return

    try:
        # Step 3: Load the book text data
        file_path = "file:///SparkCourse/SparkSQL/book.txt"
        df: Optional[DataFrame] = load_data(spark, file_path)
        if df is None:
            raise ValueError("DataFrame is None, exiting.")
    except Exception as e:
        logging.error(f"Error during data loading and validation: {e}")
        return

    try:
        # Step 4: Process the data to count word occurrences
        word_counts: DataFrame = process_words(df)

        # Step 5: Sort the word counts
        word_counts_sorted = word_counts.sort("count")

        # Step 6: Display the sorted word counts
        logging.info("Displaying the sorted word counts:")
        word_counts_sorted.show(word_counts_sorted.count())
    except Exception as e:
        logging.error(f"Error during DataFrame operations: {e}")
    finally:
        # Step 7: Stop the Spark session
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    main()
