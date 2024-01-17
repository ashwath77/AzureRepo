Sales View Data Processing Overview

1. -Create "sales_view_devtst" container in ADLS.
   -Organize folders: "customer," "product," "store," and "sales."
2. - Fetch latest modified file in ADLS.
   - Parameterize for dynamic functionality.
   - Configure parameters for all days' files.
3. - Create folder structure in "Bronze/sales_view/."
   - Copy raw data from ADF pipeline.
4. - Transform headers to snake case.
   - Split "Name" into "first_name" and "last_name."
   - Extract domain from "Email."
   - Create "gender" based on "M" and "F."
   - Format "Joining date" to "yyyy-MM-dd."
   - Upsert into "customer" table in silver layer.
5. - Transform headers to snake case.
   - Create "sub_category" based on "Category."
   - Upsert into "product" table in silver layer.
6. - Transform headers to snake case.
   - Extract "store category" from "Email."
   - Format "created_at" and "updated_at."
   - Upsert into "store" table in silver layer.
7. - Transform headers to snake case.
   - Upsert into "customer_sales" table in silver layer.
8. - Extract data from product and store tables.
   - Read delta table using UDF functions.
   - Combine data for "StoreProductSalesAnalysis."
   - Write to "StoreProductSalesAnalysis" table in gold layer.
