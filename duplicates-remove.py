from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("drop_duplicates").getOrCreate()

# Input Data
data = [
    ('102', 'Cheeseburger', 'American', '13.95'),
    ('104', 'Veggie Burger', 'American', '10.50'),
    ('105', 'Mac & Cheese', 'American', '7.00'),
    ('105', 'Mac & Cheese', 'American', '7.00'),
    ('106', 'French Fries', 'American', '7.00'),
    ('108', 'Aloo paratha', 'Asian', '14.50'),
    ('109', 'Rice & Sambar', 'Asian', '17.95'),
    ('111', 'Dosa', 'Asian', '11.95'),
    ('114', 'Iddly', 'Asian', '9.00'),
    ('114', 'Iddly', 'Asian', '9.00')
]
# DataFrame Columns Name
columns = ['menu_item_id','item_name','category','price']

df = spark.createDataFrame(data,columns)
print("Input data")
df.show(truncate=False)


































