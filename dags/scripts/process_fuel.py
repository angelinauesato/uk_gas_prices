import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit, current_timestamp, coalesce, row_number, desc, trim, lower
from pyspark.sql.window import Window

def get_safe_col(df, path, alias_name):
    """
    Safely checks if a nested column exists. 
    If it doesn't, it returns a null literal to prevent AnalysisExceptions.
    """
    try:
        # Attempt to select the column to see if it exists in the schema
        df.select(col(path))
        return col(path).cast("string").alias(alias_name)
    except:
        # If the field (like B7) is missing from the JSON, return a null column
        return lit(None).cast("string").alias(alias_name)

if __name__ == "__main__":
    brand = sys.argv[1]
    date_str = sys.argv[2]
    
    # Get database info from Environment Variables
    pg_url = os.environ.get("PG_URL")
    pg_user = os.environ.get("PG_USER")
    pg_pass = os.environ.get("PG_PASSWORD")

    spark = SparkSession.builder \
        .appName(f"Process_{brand}") \
        .getOrCreate()

    # Paths
    input_path = f"s3a://uk-gas-price/gas_prices_uk/{brand}/{date_str}.json"
    print(f"Reading data from: {input_path}")

    # Read JSON
    df = spark.read.option("multiline", "true").json(input_path)

    # DEBUG: See exactly what columns Spark found
    print(f"DEBUG: Found columns: {df.columns}")
    
    # Case-insensitive check for 'stations'
    stations_col = [c for c in df.columns if c.lower() == 'stations']

    # Flatten the standard UK fuel JSON structure: { stations: [...] }
    if stations_col:
        actual_col_name = stations_col[0]
        print(f"Processing '{actual_col_name}' for {brand}...")

        df_exploded = df.select(explode(col(actual_col_name)).alias("st"))

        df_stations = df_exploded.select(
                lit(brand).alias("brand"),
                col("st.site_id").cast("string").alias("site_id"), 
                col("st.brand").alias("station_name"),
                col("st.address").alias("address"),
                col("st.postcode").alias("postcode"),
                col("st.location.latitude").cast("string").alias("latitude"),
                col("st.location.longitude").cast("string").alias("longitude"),
                coalesce(get_safe_col(df_exploded, "st.prices.B7", "b7"), lit("0")).alias("price_b7"),
                coalesce(get_safe_col(df_exploded, "st.prices.E10", "e10"), lit("0")).alias("price_e10"),
                coalesce(get_safe_col(df_exploded, "st.prices.E5", "e5"), lit("0")).alias("price_e5"),
                coalesce(get_safe_col(df_exploded, "st.prices.SDV", "sdv"), lit("0")).alias("price_sdv"),lit(date_str).alias("extracted_date"),
                current_timestamp().alias("processed_at")
            )
        
        """
            Save the full load data:
                I am using append, then even the data comes duplicated from source, the dedupe script will do the job to fix it.
        """
        df_stations.show(5)
        df_stations.write \
            .format("jdbc") \
            .option("url", pg_url) \
            .option("dbtable", "retailers_fuel_prices") \
            .option("user", pg_user) \
            .option("password", pg_pass) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"Successfully appended {brand} data to Postgres.")
        
    else:
        print(f"ERROR: No 'stations' column (case-insensitive) found for {brand}. Columns available: {df.columns}")

    spark.stop()