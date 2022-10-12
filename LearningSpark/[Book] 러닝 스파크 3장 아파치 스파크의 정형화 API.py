#!/usr/bin/env python
# coding: utf-8

# # Page 45

# In[1]:


# 저수준의 RDD API 패턴 코드

# 파이썬 예제
from pyspark import SparkContext
sc = SparkContext()

# (name, age) 형태의 튜플로 된 RDD를 생성한다.
dataRDD = sc.parallelize([('Brooke', 20), ('Denny', 31), ('Jules', 30), ('TD', 35), ('Brooke', 25)])

# 집계와 평균을 위한 람다 표현식과 함께 map과 reduceByKey 트랜스포메이션을 사용한다.
agesRDD = (dataRDD.map(lambda x: (x[0] (x[1], 1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0]/x[1][1])))


# In[2]:


dataRDD


# In[3]:


agesRDD


# # Page 46

# In[4]:


# 고수준 DSL 연산자들과 데이터 프레임 API를 쓴 코드

# 파이썬 예제
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# SparkSession으로부터 데이터 프레임을 만든다
spark = (SparkSession.builder.appName('AuthorsAges').getOrCreate())

# 데이터 프레임 생성
data_df = spark.createDataFrame([('Brooke', 20), ('Denny', 31), ('Jules', 30), ('TD', 35), ('Brooke', 25)], ['name', 'age'])

# 동일한 이름으로 그룹화하여 나이별로 계산해 평균을 구한다.
avg_df = data_df.groupBy('name').agg(avg('age'))

# 최종 실행 결과를 보여준다.
avg_df.show()


# # Page 52

# In[5]:


# 세 개의 이름이 붙은 칼럼을 가진 데이터 프레임을 위한 스키마를 프로그래밍 스타일로 정의하려면 스파크 데이터 프레임 API를 사용

# 파이선 예제 
from pyspark.sql.types import *
schema = StructType([StructField('author', StringType(), False),
                    StructField('title', StringType(), False),
                    StructField('pages', IntegerType(), False)])


# In[6]:


# DDL을 써서 동일한 스키마 정의

# 파이썬 예제
schema = 'author STRING, title STRING, pages INT'


# In[9]:


# 어떤 방식이든 원하는 쪽으로 스파크를 정의할 수 있다.
# 이후 예제들에서 양쪽 다 사용하게 될 것이다.

# 파이썬 예제
from pyspark.sql import SparkSession

# DDL을 써서 스키마를 정의한다.
schema = StructType([
   StructField("Id", IntegerType(), False),
   StructField("First", StringType(), False),
   StructField("Last", StringType(), False),
   StructField("Url", StringType(), False),
   StructField("Published", StringType(), False),
   StructField("Hits", IntegerType(), False),
   StructField("Campaigns", ArrayType(StringType()), False)])

# 기본 데이터 생성
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
       [2, "Brooke","Wenig","https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
       [3, "Denny", "Lee", "https://tinyurl.3","6/7/2019",7659, ["web", "twitter", "FB", "LinkedIn"]],
       [4, "Tathagata", "Das","https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
       [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
       [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
      ]

# 메인 프로그램
if __name__ == "__main__":
    # SparkSession 생성
    spark = (SparkSession
       .builder
       .appName("Example-3_6")
       .getOrCreate())
    # 위에 정의했던 스키마로 데이터 프레임 생성
    blogs_df = spark.createDataFrame(data, schema)
   
    # 데이터 프레임 내용을 보여준다. 위에서 만든 데이터를 보여주게 된다.
    #blogs_df.show()
    print()
    
    # 데이터 프레임 처리에 사용된 스키마를 출력한다.
    print(blogs_df.printSchema())


# # Page 58

# In[10]:


# 파이썬 예제
from pyspark.sql import Row

blog_row = Row(6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"])
blog_row[1]


# # Page 59

# In[12]:


# 파이썬 예제
rows = [Row('Matei Zaharia', 'CA'), Row('Reynold Xin', 'CA')]
authors_df = spark.createDataFrame(rows, ['Authors', 'State'])
authors_df


# # Page 60

# In[ ]:




