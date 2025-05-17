import findspark
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

sys.path.append(str(Path(__file__).parent.parent))
from product_category_pyspark.data_processor import get_product_category_pairs

def initialize_spark() -> SparkSession:
    """Initialize and configure Spark session."""
    findspark.init()
    return SparkSession.builder \
        .appName("ProductCategoryAnalysis") \
        .getOrCreate()

def get_schemas() -> tuple:
    """Return predefined schemas for products, categories and links."""
    products_schema = StructType([
        StructField("product_id", IntegerType(), nullable=False),
        StructField("product_name", StringType(), nullable=False)
    ])
    
    categories_schema = StructType([
        StructField("category_id", IntegerType(), nullable=False),
        StructField("category_name", StringType(), nullable=False)
    ])
    
    links_schema = StructType([
        StructField("product_id", IntegerType(), nullable=False),
        StructField("category_id", IntegerType(), nullable=False)
    ])
    
    return products_schema, categories_schema, links_schema

def create_test_data(spark: SparkSession, schemas: tuple) -> tuple:
    """Create test DataFrames for products, categories and their relationships."""
    products_schema, categories_schema, links_schema = schemas
    
    products_data = [
        (1, "Продукт 1"),
        (2, "Продукт 2"),
        (3, "Продукт 3"),
        (4, "Продукт 4")
    ]
    
    categories_data = [
        (101, "Категория A"),
        (102, "Категория B"),
        (103, "Категория C")
    ]
    
    links_data = [
        (1, 101),
        (1, 102),
        (2, 102),
        (3, 103)
    ]
    
    products_df = spark.createDataFrame(products_data, schema=products_schema)
    categories_df = spark.createDataFrame(categories_data, schema=categories_schema)
    links_df = spark.createDataFrame(links_data, schema=links_schema)
    
    return products_df, categories_df, links_df

def main():
    spark = initialize_spark()
    schemas = get_schemas()
    
    products_df, categories_df, links_df = create_test_data(spark, schemas)

    result = get_product_category_pairs(products_df, categories_df, links_df)
    
    print("\nProduct-Category Relationships:")
    result.show()
    
    spark.stop()

if __name__ == "__main__":
    main()