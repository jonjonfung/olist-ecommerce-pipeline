import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = "olist-ecommerce-pipeline-john"
silver = f"s3://{bucket}/silver"

def read_csv(path):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(path)

# ---- ORDERS ----
orders = read_csv(f"s3://{bucket}/bronze/orders/olist_orders_dataset.csv")
orders = orders.dropDuplicates()
orders = orders.dropna(subset=["order_id", "customer_id"])
orders.write.mode("overwrite").parquet(f"{silver}/orders/")
print("✅ Orders cleaned")

# ---- CUSTOMERS ----
customers = read_csv(f"s3://{bucket}/bronze/customers/olist_customers_dataset.csv")
customers = customers.dropDuplicates()
customers = customers.dropna(subset=["customer_id"])
customers.write.mode("overwrite").parquet(f"{silver}/customers/")
print("✅ Customers cleaned")

# ---- ORDER ITEMS ----
order_items = read_csv(f"s3://{bucket}/bronze/order_items/olist_order_items_dataset.csv")
order_items = order_items.dropDuplicates()
order_items = order_items.dropna(subset=["order_id", "product_id"])
order_items = order_items.withColumn("price", col("price").cast(DoubleType()))
order_items = order_items.withColumn("freight_value", col("freight_value").cast(DoubleType()))
order_items.write.mode("overwrite").parquet(f"{silver}/order_items/")
print("✅ Order items cleaned")

# ---- ORDER PAYMENTS ----
payments = read_csv(f"s3://{bucket}/bronze/order_payments/olist_order_payments_dataset.csv")
payments = payments.dropDuplicates()
payments = payments.dropna(subset=["order_id"])
payments = payments.withColumn("payment_value", col("payment_value").cast(DoubleType()))
payments = payments.withColumn("payment_installments", col("payment_installments").cast(IntegerType()))
payments.write.mode("overwrite").parquet(f"{silver}/order_payments/")
print("✅ Payments cleaned")

# ---- ORDER REVIEWS ----
reviews = read_csv(f"s3://{bucket}/bronze/order_reviews/olist_order_reviews_dataset.csv")
reviews = reviews.dropDuplicates()
reviews = reviews.dropna(subset=["review_id", "order_id"])
reviews = reviews.withColumn("review_score", col("review_score").cast(IntegerType()))
reviews.write.mode("overwrite").parquet(f"{silver}/order_reviews/")
print("✅ Reviews cleaned")

# ---- PRODUCTS ----
products = read_csv(f"s3://{bucket}/bronze/products/olist_products_dataset.csv")
products = products.dropDuplicates()
products = products.dropna(subset=["product_id"])
products = products.withColumn("product_weight_g", col("product_weight_g").cast(DoubleType()))
products = products.withColumn("product_length_cm", col("product_length_cm").cast(DoubleType()))
products = products.withColumn("product_height_cm", col("product_height_cm").cast(DoubleType()))
products = products.withColumn("product_width_cm", col("product_width_cm").cast(DoubleType()))
products.write.mode("overwrite").parquet(f"{silver}/products/")
print("✅ Products cleaned")

# ---- SELLERS ----
sellers = read_csv(f"s3://{bucket}/bronze/sellers/olist_sellers_dataset.csv")
sellers = sellers.dropDuplicates()
sellers = sellers.dropna(subset=["seller_id"])
sellers.write.mode("overwrite").parquet(f"{silver}/sellers/")
print("✅ Sellers cleaned")

# ---- GEOLOCATION ----
geolocation = read_csv(f"s3://{bucket}/bronze/geolocation/olist_geolocation_dataset.csv")
geolocation = geolocation.dropDuplicates()
geolocation = geolocation.withColumn("geolocation_lat", col("geolocation_lat").cast(DoubleType()))
geolocation = geolocation.withColumn("geolocation_lng", col("geolocation_lng").cast(DoubleType()))
geolocation.write.mode("overwrite").parquet(f"{silver}/geolocation/")
print("✅ Geolocation cleaned")

# ---- PRODUCT CATEGORY TRANSLATION ----
translation = read_csv(f"s3://{bucket}/bronze/product_category_translation/product_category_name_translation.csv")
translation = translation.dropDuplicates()
translation.write.mode("overwrite").parquet(f"{silver}/product_category_translation/")
print("✅ Product category translation cleaned")

job.commit()
print("🎉 All tables cleaned and saved to silver layer!")
