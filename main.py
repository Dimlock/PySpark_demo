from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ProductCategoryJoin") \
    .getOrCreate()

products_data = [
    (1, "Хлеб"),
    (2, "Молоко"),
    (3, "Сыр"),
    (4, "Масло"),
    (5, "Яблоко")
]
products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])

categories_data = [
    (1, "Молочные"),
    (2, "Фрукты"),
    (3, "Выпечка"),
    (4, "Завтрак")
]
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])

relations_data = [
    (1, 3),
    (1, 4),
    (2, 1),
    (3, 1),
    (5, 2)
]
relations_df = spark.createDataFrame(relations_data, ["product_id", "category_id"])

product_with_categories = products_df \
    .join(relations_df, on="product_id", how="left") \
    .join(categories_df, on="category_id", how="left") \
    .select("product_name", "category_name")

products_without_categories = products_df \
    .join(relations_df, on="product_id", how="left_anti") \
    .select("product_name")

print("Продукт – Категория")
product_with_categories.show()

print("Продукты без категорий")
products_without_categories.show()
