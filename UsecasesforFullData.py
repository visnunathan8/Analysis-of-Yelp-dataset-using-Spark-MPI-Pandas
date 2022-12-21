import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql import Row
import time
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()

reviews = spark.read.format('json').load('/Users/sharanyu/Documents/yelp compressed/yelp_academic_dataset_review.json')
users = spark.read.format('json').load('/Users/sharanyu/Documents/yelp compressed/yelp_academic_dataset_user.json')
checkin = spark.read.format('json').load('/Users/sharanyu/Documents/yelp compressed/yelp_academic_dataset_checkin.json')
tip = spark.read.format('json').load('/Users/sharanyu/Documents/yelp compressed/yelp_academic_dataset_tip.json')
business = spark.read.format('json').load('/Users/sharanyu/Documents/yelp compressed/yelp_academic_dataset_business.json')

def usecase1(users,reviews,business):
    df = users.join(reviews, reviews.user_id == users.user_id)

    df = df.join(business, business.business_id == reviews.business_id)

    df = df.where(df.categories.contains("Restaurants"))

    df = df.withColumn("WiFi", F.col("attributes.WiFi")).withColumn("WheelChairAccessible",F.col("attributes.WheelchairAccessible"))

    df_query = df.select(business.name,business.city,business.state,df.average_stars,df.WiFi,df.WheelChairAccessible)

    df_query = df_query.filter((df_query.average_stars > 4.0) & (df_query.WiFi.contains('free')) & (df_query.WheelChairAccessible.contains('True')))
    
    df_query.count()
    
    return df_query

def usecase2(users,reviews,business):
    df = users.join(reviews, reviews.user_id == users.user_id)

    df = df.join(business, business.business_id == reviews.business_id)
    
    df = df.where((df.categories.contains("Gas Stations")) & (df.categories.contains('Food')) | (df.categories.contains('Fast Food')) | (df.categories.contains('Restaurants')) | (df.categories.contains('Pizza')))
    
    df_query = df.filter((df.average_stars > 3.5) & (df.state=="FL"))
    
    df_query = df_query.select(business.name,df_query.average_stars,df_query.hours,df_query.city,df_query.postal_code)
    
    df_queryfinal = df_query.withColumn('Friday',F.col('hours.Friday')).withColumn('Saturday',F.col('hours.Saturday')).withColumn('Sunday',F.col('hours.Sunday'))
    
    df_queryfinal=df_queryfinal.drop('hours')
    
    df_queryfinal.count()
    
    return df_queryfinal

def usecase3(users,reviews,business):
    df = users.join(reviews, reviews.user_id == users.user_id)

    df = df.join(business, business.business_id == reviews.business_id)
    
    df = df.filter(df.elite != '')
    
    def complimentsum(a,b,c,d,e,f,g,h,i,j,k):
        col_sum = a+b+c+d+e+f+g+h+i+k
        return col_sum
    
    new_f = F.udf(complimentsum, IntegerType())
    
    df_usercomp = df.withColumn("total_compliments",
                              new_f("compliment_hot", "compliment_more", "compliment_profile","compliment_cute","compliment_list","compliment_note","compliment_plain","compliment_cool","compliment_funny","compliment_writer","compliment_photos"))
    
    df_usercomp = df_usercomp.filter((df_usercomp.fans > 100) | (users.review_count > 500) | (df_usercomp.total_compliments > 1000))
    
    df_usercomp = df_usercomp.filter(df_usercomp.average_stars > 4.0)
    
    df_usercomp = df_usercomp.select(business.name,business.review_count,business.categories,business.city,business.state)
    
    df_usercomp.count()
    
    return df_usercomp

# calling usecase 1

start_time = time.time()
restaurants_whlchraccess_wifi = usecase1(users,reviews,business)
print("usecase1 took --- %s seconds ---" % (time.time() - start_time))

# calling usecase 2

start_time = time.time()
business_gasstation_weeknds = usecase2(users,reviews,business)
print("usecase2 took --- %s seconds ---" % (time.time() - start_time))


# calling usecase 3

start_time = time.time()
certified_businesses = usecase3(users,reviews,business)
print("usecase3 took --- %s seconds ---" % (time.time() - start_time))




