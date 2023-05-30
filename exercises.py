from base.computer_firm import laptop, printer, product
from base.ships import ships, battles, outcomes, classes
from pyspark.sql.window import Window
import pyspark.sql.functions as f 
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from functools import reduce
#%% ALIASES
c = classes
s = ships 
o = outcomes
p = product
#%%
txt = """
58. Для каждого типа продукции и каждого производителя из таблицы Product c точностью до двух десятичных знаков найти процентное отношение числа моделей данного типа данного производителя к общему числу моделей этого производителя.
"""
print(txt)

create_table = lambda col: p.select('maker').distinct() \
    .withColumn('type', f.lit(col)).withColumn('count', f.lit(0))

columns = ['PC', 'Laptop', 'Printer']
dist = reduce(DataFrame.union, map(create_table, columns))

df = p.groupBy('maker', 'type').agg(f.count('*').alias('count'))

wSpec = Window.partitionBy('maker')
round_ = lambda col: f.round(col*100, 2).alias('prc')
dist.union(df) \
    .groupBy('maker', 'type').agg(f.sum('count').alias('gr1')) \
    .withColumn('gr2', f.sum('gr1').over(wSpec)) \
    .select('maker', 'type', round_(col('gr1')/col('gr2'))) \
    .show()
#%%
txt = """
57. Для классов, имеющих потери в виде потопленных кораблей и не менее 3 кораблей в базе данных, вывести имя класса и число потопленных кораблей.
"""
print(txt)

df1 = s.join(o, s.name == o.ship, 'inner') \
    .select('class', 'name', 'result')
df2 = c.join(o, c['class'] == o['ship'], 'inner') \
    .select('class', 'ship', 'result')    

df_sunken = df1.union(df2) \
    .distinct() \
    .filter(col('result') == 'sunk') \
    .groupBy('class').agg(f.count('*').alias('sunken'))

df_more2 = c.join(o, c['class'] == o['ship'], 'inner') \
    .select('class', 'ship') \
    .union(s.select('class', 'name')) \
    .distinct() \
    .sort('class') \
    .groupBy('class') \
    .agg(f.count('*').alias('count')) \
    .filter(col('count') > 2) \
    
df_result = df_sunken.join(df_more2, 'class', 'inner') \
    .select(col('class').alias('cls'), 'sunken')
df_result.show()
#%%
txt = """
56. Для каждого класса определите число кораблей этого класса, потопленных в сражениях. Вывести: класс и число потопленных кораблей.
"""
print(txt)

df1 = s.join(o, col('name') == col('ship')).select('class', 'name', 'result')
df2 = c.join(o, col('class') == col('ship')).select('class', 'ship', 'result')

when = lambda column: f.when(col(column) == 'sunk', f.lit(1)) \
    .otherwise(f.lit(0)).alias('sunks')

df1.union(df2) \
    .join(c, 'class', 'right') \
    .select('class', when('result')) \
    .groupBy('class').agg(f.sum('sunks').alias('sunks')).show()
#%%
txt = """
55. Для каждого класса определите год, когда был спущен на воду первый корабль этого класса. Если год спуска на воду головного корабля неизвестен, определите минимальный год спуска на воду кораблей этого класса. Вывести: класс, год.
"""
print(txt)

df = c.join(s, 'class', 'left') \
    .select('class', 'launched') \
    .groupBy('class') \
    .agg(f.min('launched').alias('year')) \
    .select(
        col('class'),
        f.when(col('year').isNull(), '').otherwise(col('year')).alias('year')
        )
df.show()    
#%%
txt = """
54. С точностью до 2-х десятичных знаков определите среднее число орудий всех линейных кораблей (учесть корабли из таблицы Outcomes).
"""
print(txt)

df1 = c.join(s, 'class', 'inner').select('name', 'numguns', 'type')
df2 = c.join(o, c['class'] == o['ship'], 'inner').select('ship', 'numguns', 'type')

df1.union(df2) \
    .distinct() \
    .filter(col('type') == 'bb') \
    .agg(f.mean('numguns').alias('Avg_numG')) \
    .select(f.round('Avg_numG', 2).alias('Avg_numG')) \
    .show()

#%%
txt = """
53. Определите среднее число орудий для классов линейных кораблей.
Получить результат с точностью до 2-х десятичных знаков.
"""
print(txt)

classes.filter(col('type') == 'bb') \
    .agg(f.mean('numguns').alias('Avg-numGuns')) \
    .select(f.round('Avg-numGuns', 2).alias('Avg-numGuns')) \
    .show()
#%%
txt = """
52. Определить названия всех кораблей из таблицы Ships, которые могут быть линейным японским кораблем,
имеющим число главных орудий не менее девяти, калибр орудий менее 19 дюймов и водоизмещение не более 65 тыс.тонн
"""
print(txt)

df1 = s.join(c, s['class'] == c['class'], 'outer') \
    .filter(classes.country == 'Japan')\
    .filter(classes.bore <= 65000)\
    .filter(classes.numguns >= 9) \
    .select('name')

df2 = s.join(o, s['name'] == o['ship'], 'right') \
    .select('ship')

df1.union(df2).show()
#%%

txt = """
51. Найдите названия кораблей, имеющих наибольшее число орудий среди всех имеющихся кораблей такого же водоизмещения (учесть корабли из таблицы Outcomes).
"""
print(txt)

dist = o.select('ship').distinct() 

wSpec = Window.partitionBy('displacement').orderBy(f.col('numGuns').desc())

df = s.join(c, 'class', 'right') 
    
df.join(dist, dist['ship'] == df['class'], 'left') \
    .withColumn('rank', f.rank().over(wSpec)) \
    .filter(f.col('rank') == 1) \
    .select(f.when(
        col('name').isNull(), 
        col('class')).otherwise(col('name')
            ).alias('name')) \
    .sort('name') \
    .show()        
