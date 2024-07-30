import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
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


def calculate_total_spent_by_customer(df: DataFrame) -> DataFrame:
    """
    Calculates the total amount spent by each customer.

    Args:
    df (DataFrame): The input DataFrame.

    Returns:
    DataFrame: A DataFrame with the total amount spent by each customer.
    """
    return df.groupBy("cust_id").agg(
        func.round(func.sum("amount_spent"), 2).alias("total_spent")
    )


def main() -> None:
    """
    Main function to configure Spark, load data, and calculate total spent by customer.

    Steps:
    1. Configure logging for the script.
    2. Create a SparkSession.
    3. Define the schema for the CSV file.
    4. Load the customer orders data.
    5. Calculate the total amount spent by each customer.
    6. Sort the results by total amount spent.
    7. Display the sorted results.
    8. Stop the Spark session.
    """
    configure_logging()

    try:
        # Step 2: Create a SparkSession
        spark = (
            SparkSession.builder.appName("TotalSpentByCustomer")
            .master("local[*]")
            .getOrCreate()
        )
        logging.info("Spark session created.")
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        return

    try:
        # Step 3: Define the schema for the CSV file
        customer_order_schema = StructType(
            [
                StructField("cust_id", IntegerType(), True),
                StructField("item_id", IntegerType(), True),
                StructField("amount_spent", FloatType(), True),
            ]
        )

        # Step 4: Load the customer orders data
        file_path = "file:///SparkCourse/SparkSQL/customer-orders.csv"
        customers_df: Optional[DataFrame] = load_data(
            spark, file_path, customer_order_schema
        )
        if customers_df is None:
            raise ValueError("DataFrame is None, exiting.")
    except Exception as e:
        logging.error(f"Error during data loading and validation: {e}")
        return

    try:
        # Step 5: Calculate the total amount spent by each customer
        total_by_customer = calculate_total_spent_by_customer(customers_df)

        # Step 6: Sort the results by total amount spent
        total_by_customer_sorted = total_by_customer.sort("total_spent")

        # Step 7: Display the sorted results
        logging.info(
            "Displaying the total amount spent by each customer, sorted by total spent:"
        )
        total_by_customer_sorted.show(total_by_customer_sorted.count())
    except Exception as e:
        logging.error(f"Error during DataFrame operations: {e}")
    finally:
        # Step 8: Stop the Spark session
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    main()
