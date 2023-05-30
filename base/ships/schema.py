from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.types import IntegerType, DoubleType, ShortType, TimestampType
from base.session import spark
import os


outcomes_schema = StructType([
    StructField('ship', StringType()),
    StructField('battle', StringType()),
    StructField('result', StringType())
    ])

ships_schema = StructType([
    StructField('name', StringType()),
    StructField('class', StringType()),
    StructField('launched', IntegerType())
    ])

classes_schema = StructType([
    StructField('class', StringType()),
    StructField('type', StringType()),
    StructField('country', StringType()),
    StructField('numguns', ShortType()),
    StructField('bore', DoubleType()),
    StructField('displacement', IntegerType())
    ])

battles_schema = StructType([
    StructField('name', StringType()),
    StructField('date', TimestampType())
    ]) 
    
DIR_PATH = '/mnt/sda5/Yandex.Disk/pro/courses/sql-exe/base/ships'
OUTCOMES_PATH = os.path.join(DIR_PATH, 'outcomes.csv')
SHIPS_PATH = os.path.join(DIR_PATH, 'ships.csv')
CLASSES_PATH = os.path.join(DIR_PATH, 'classes.csv')
BATTLES_PATH = os.path.join(DIR_PATH, 'battles.csv')

outcomes = spark.read.csv(OUTCOMES_PATH, schema=outcomes_schema, 
                         header=True, sep=',')
ships = spark.read.csv(SHIPS_PATH, schema=ships_schema, 
                         header=True, sep=',')
classes = spark.read.csv(CLASSES_PATH, schema=classes_schema, 
                         header=True, sep=',')

battles = spark.read.csv(BATTLES_PATH, schema=battles_schema, 
                         header=True, sep=',')

if __name__ == '__main__':
   outcomes.show() 
   ships.show()
   classes.show()
   battles.show()