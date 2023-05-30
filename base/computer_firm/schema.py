from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.types import IntegerType, DoubleType, ShortType
from base.session import spark
import os


product_schema = StructType([
    StructField('maker', StringType()),
    StructField('model', StringType()),
    StructField('type', StringType()),
    ])

laptop_schema = StructType([
    StructField('code', IntegerType()),
    StructField('model', StringType()),
    StructField('speed', ShortType()),
    StructField('ram', ShortType()),
    StructField('hd', DoubleType()),
    StructField('price', DoubleType()),
    StructField('screen', ShortType())
    ])

pc_schema = StructType([
    StructField('code', IntegerType()),
    StructField('model', StringType()),
    StructField('speed', ShortType()),
    StructField('ram', ShortType()),
    StructField('hd', DoubleType()),
    StructField('cd', StringType()),
    StructField('price', DoubleType())
    ])

printer_schema = StructType([
    StructField('code', IntegerType()),
    StructField('model', StringType()),
    StructField('color', StringType()),
    StructField('type', StringType()),
    StructField('price', DoubleType())
    ]) 
    
DIR_PATH = '/mnt/sda5/Yandex.Disk/pro/courses/sql-exe/base/computer_firm'
PRODUCT_PATH = os.path.join(DIR_PATH, 'product.csv')
PRINTER_PATH = os.path.join(DIR_PATH, 'printer.csv')
LAPTOP_PATH = os.path.join(DIR_PATH, 'laptop.csv')
PC_PATH = os.path.join(DIR_PATH, 'pc.csv')
product = spark.read.csv(PRODUCT_PATH, schema=product_schema, 
                         header=True, sep=',')
laptop = spark.read.csv(LAPTOP_PATH, schema=laptop_schema, 
                         header=True, sep=',')
printer = spark.read.csv(PRINTER_PATH, schema=printer_schema, 
                         header=True, sep=',')
pc = spark.read.csv(PC_PATH, schema=pc_schema, 
                         header=True, sep=',')

if __name__ == '__main__':
   laptop.show() 
   printer.show()
   product.show()
   pc.show()
