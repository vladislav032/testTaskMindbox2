from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def get_product_category_pairs(products_df: DataFrame, 
                             categories_df: DataFrame, 
                             product_category_links_df: DataFrame) -> DataFrame:
    """
    Возвращает датафрейм со всеми парами "Имя продукта – Имя категории" 
    и продуктами без категорий.
    
    Параметры:
    - products_df: Датафрейм с колонками ['product_id', 'product_name']
    - categories_df: Датафрейм с колонками ['category_id', 'category_name']
    - product_category_links_df: Датафрейм с колонками ['product_id', 'category_id']
    
    Возвращает:
    - Датафрейм с колонками ['product_name', 'category_name']
    """
    # Соединяем все три датафрейма через left join
    result_df = (products_df
                .join(product_category_links_df, 'product_id', 'left')
                .join(categories_df, 'category_id', 'left')
                .select(
                    col('product_name'),
                    col('category_name')
                ).orderBy("product_name")
               )
    
    return result_df