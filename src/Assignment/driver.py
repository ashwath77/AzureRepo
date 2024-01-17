# Databricks notebook source
# MAGIC %run /Users/ashwathgowda216@gmail.com/AdfRepo/src/Adf_Assignments/utils

# COMMAND ----------

# DBTITLE 1,Mountpoint_Information
source='wasbs://salesviewdevtst@adlstogen2.blob.core.windows.net/'
mountpoint = '/mnt/mountpointstorageadf'
key='fs.azure.account.key.adlstogen2.blob.core.windows.net'
value='D2RGMipb09LJoDXbNmHeBUrfCva/Zm/hO+qhcjkgRhFVfEvTmk2aaPErH31ChGTJorsWbtk3c222+ASt5WtDDw=='
mounting_layer_json(source,mountpoint,key,value)

# COMMAND ----------

# DBTITLE 1,Reading the custamor file.
options={'header':'true','delimiter':',','inferSchema':'True'}
df_customer=read("csv", "dbfs:/mnt/mountpointstorageadf/bronze/sales_view/customer/20240107_sales_customer.csv", options )

#customer_data
df_customer.display()

# COMMAND ----------

# DBTITLE 1,Customer columns with snake_case
from pyspark.sql.types import *
from pyspark.sql.functions import *
a=udf(snake_case,StringType())
lst = list(map(lambda x:snake_case(x),df_customer.columns))
df = df_customer.toDF(*lst)

# COMMAND ----------

df.columns
df.display()

# COMMAND ----------

df = split_name_column(df,'name')
df = extract_domain_column(df, 'email_id', '@','\.')
df = create_gender_column(df, 'gender')
df = split_joining_date_column(df, 'joining_date')
df= create_expenditure_status_column(df, 'spent')
df = format_date_column(df, 'date', input_format='dd-MM-yyyy', output_format='yyyy-MM-dd')

# COMMAND ----------

# DBTITLE 1,Writing customer table in Sliver
path='dbfs:/mnt/Silver'
db_name='sales_view'
tb_name= 'customer'
save_as_delta(df, path, db_name, tb_name, ["employee_id"])

# COMMAND ----------

# DBTITLE 1,Reading the Product file
# Read Product File
options={'header':'true','delimiter':',','inferSchema':'True'}
df_product=read("csv", "dbfs:/mnt/mountpointstorageadf/bronze/sales_view/product/20240107_sales_product.csv", options )

# COMMAND ----------

# DBTITLE 1,product columns with snake_case
from pyspark.sql.types import *
from pyspark.sql.functions import *
a=udf(snake_case,StringType())
lst = list(map(lambda x:snake_case(x),df_product.columns))
df = df_product.toDF(*lst)

# COMMAND ----------

df.display()

# COMMAND ----------

df = create_sub_category_column(df, 'category_id', 'sub_category')
df.display()

# COMMAND ----------

# DBTITLE 1,Writing product table in Sliver
path='dbfs:/mnt/Silver'
db_name='sales_view'
tb_name= 'product'

save_as_delta(df, path, db_name, tb_name, ["product_id"])

# COMMAND ----------

# DBTITLE 1,Reading the Store file.
# Read store File
options={'header':'true','delimiter':',','inferSchema':'True'}
df_store=read("csv", "dbfs:/mnt/mountpointstorageadf/bronze/sales_view/store/20240105_sales_store.csv", options )

# COMMAND ----------

# DBTITLE 1,Store columns with snake_case
a=udf(snake_case,StringType())
lst = list(map(lambda x:snake_case(x),df_store.columns))
df = df_store.toDF(*lst)

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,Reading store category columns and the value is exatracted from email
df1 = extract_email_column(df_store, 'email_address', '@', '\.')
df1.display()

# COMMAND ----------

# DBTITLE 1,Writing store table in Sliver
path='dbfs:/mnt/Silver'
db_name='sales_view'
tb_name= 'Store'

save_as_delta(df, path, db_name, tb_name, ["store_id"])

# COMMAND ----------

# DBTITLE 1,Reading the Sales file.
# Read store File
options={'header':'true','delimiter':',','inferSchema':'True'}
df_sales=read("csv", "dbfs:/mnt/mountpointstorageadf/bronze/sales_view/sales/20240105_sales_data.csv", options)

# COMMAND ----------

df_sales.display()

# COMMAND ----------

a=udf(snake_case,StringType())
lst = list(map(lambda x:snake_case(x),df_sales.columns))
df = df_sales.toDF(*lst)

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,Writing sales table in Sliver
path='dbfs:/mnt/Silver'
db_name='sales_view'
tb_name= 'sales'

save_as_delta(df, path, db_name, tb_name, ["product_id"])

# COMMAND ----------

# Select Required Columns from Product and Store Tables
gold_df = select_required_columns(product_df, store_df)

# COMMAND ----------

customer_sales_gold_path = "gold/sales_view/store_product_sales_analysis"
customer_sales_gold_df = spark.read.format("delta").table(customer_sales_gold_path)


# COMMAND ----------

final_df = transform_data_for_gold_layer(customer_sales_gold_df, gold_df)

# COMMAND ----------

final_gold_path = "gold/sales_view/store_product_sales_analysis"
final_df.write.format("delta").mode("overwrite").save(final_gold_path)