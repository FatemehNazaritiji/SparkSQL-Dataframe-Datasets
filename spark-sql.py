import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Row
from typing import List, Optional


def configure_logging() -> None:
    """
    Configures the logging settings for the script.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(asctime)s - %(message)s"
    )


def mapper(line: str) -> Optional[Row]:
    """
    Parses a line of the CSV file and returns a Row object.

    Args:
    line (str): A line of text from the CSV file.

    Returns:
    Optional[Row]: A Row object with the parsed fields, or None if there is an error.
    """
    try:
        fields = line.split(",")
        return Row(
            ID=int(fields[0]),
            name=fields[1],
            age=int(fields[2]),
            numFriends=int(fields[3]),
        )
    except (IndexError, ValueError) as e:
        logging.error(f"Error parsing line: {line}. Error: {e}")
        return None


def main() -> None:
    """
    Main function to configure Spark, process the fake friends dataset,
    and run SQL queries on the DataFrame.

    Steps:
    1. Configure logging for the script.
    2. Create a SparkSession.
    3. Load the fake friends dataset.
    4. Parse each line to extract fields and create an RDD of Row objects.
    5. Infer the schema, and register the DataFrame as a table.
    6. Run SQL query to find teenagers and log the results.
    7. Use DataFrame functions to show counts by age.
    8. Stop the Spark session.
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
        lines = spark.sparkContext.textFile("fakefriends.csv")
        logging.info("Dataset loaded.")
    except Exception as e:
        logging.error(f"Error loading dataset: {e}")
        return

    try:
        # Step 4: Parse each line to extract fields and create an RDD of Row objects
        people_rdd = lines.map(mapper).filter(lambda x: x is not None)

        # Step 5: Infer the schema, and register the DataFrame as a table
        schema_people: DataFrame = spark.createDataFrame(people_rdd).cache()
        schema_people.createOrReplaceTempView("people")
        logging.info("DataFrame created and registered as table.")
    except Exception as e:
        logging.error(f"Error processing dataset: {e}")
        return

    try:
        # Step 6: Run SQL query to find teenagers and log the results
        teenagers: DataFrame = spark.sql(
            "SELECT * FROM people WHERE age >= 13 AND age <= 19"
        )
        logging.info("SQL query executed for teenagers.")

        # Print the results
        for teen in teenagers.collect():
            logging.info(
                f"ID: {teen['ID']: <5} "
                f"Name: {teen['name']: <10} "
                f"Age: {teen['age']: <3} "
                f"Friends: {teen['numFriends']: <5}"
            )
    except Exception as e:
        logging.error(f"Error running SQL query: {e}")
        return

    try:
        # Step 7: Use DataFrame functions to show counts by age
        schema_people.groupBy("age").count().orderBy("age").show()
        logging.info("DataFrame functions executed for group by age.")
    except Exception as e:
        logging.error(f"Error executing DataFrame functions: {e}")
        return
    finally:
        # Step 8: Stop the Spark session
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    main()
