Sales View Data Processing Overview

1. Create "sales_view_devtst" container in ADLS and organize folders: "customer," "product," "store," and "sales."
2. Fetch latest modified file in ADLS, parameterize for dynamic functionality, and configure parameters for all days' files.
3. Create folder structure in "Bronze/sales_view/" and copy raw data from ADF pipeline.
4. Transform headers to snake case, split "Name" into "first_name" and "last_name," extract domain from "Email," create "gender" based on "M" and "F," format "Joining date" to "yyyy-MM-dd," and upsert into "customer" table in silver layer.
5. Transform headers to snake case, create "sub_category" based on "Category," and upsert into "product" table in silver layer.
6. Transform headers to snake case, extract "store category" from "Email," format "created_at" and "updated_at," and upsert into "store" table in silver layer.
7. Transform headers to snake case and upsert into "customer_sales" table in silver layer.
8. Extract data from product and store tables, read delta table using UDF functions, combine data for "StoreProductSalesAnalysis," and write to "StoreProductSalesAnalysis" table in gold layer.layer.
