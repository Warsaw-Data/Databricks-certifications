# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Navigating Databricks SQL and Attaching to SQL Warehouses
# MAGIC
# MAGIC * Navigate to Databricks SQL  
# MAGIC   * Make sure that SQL is selected from the workspace option in the sidebar (directly below the Databricks logo)
# MAGIC * Make sure a SQL warehouse is on and accessible
# MAGIC   * Navigate to SQL Warehouses in the sidebar
# MAGIC   * If a SQL warehouse exists and has the State **`Running`**, you'll use this SQL warehouse
# MAGIC   * If a SQL warehouse exists but is **`Stopped`**, click the **`Start`** button if you have this option (**NOTE**: Start the smallest SQL warehouse you have available to you) 
# MAGIC   * If no SQL warehouses exist and you have the option, click **`Create SQL Warehouse`**; name the SQL warehouse something you'll recognize and set the cluster size to 2X-Small. Leave all other options as default.
# MAGIC   * If you have no way to create or attach to a SQL warehouse, you'll need to contact a workspace administrator and request access to compute resources in Databricks SQL to continue.
# MAGIC * Navigate to home page in Databricks SQL
# MAGIC   * Click the Databricks logo at the top of the side nav bar
# MAGIC * Locate the **Sample dashboards** and click **`Visit gallery`**
# MAGIC * Click **`Import`** next to the **Retail Revenue & Supply Chain** option
# MAGIC   * Assuming you have a SQL warehouse available, this should load a dashboard and immediately display results
# MAGIC   * Click **Refresh** in the top right (the underlying data has not changed, but this is the button that would be used to pick up changes)
# MAGIC
# MAGIC # Updating a DBSQL Dashboard
# MAGIC
# MAGIC * Use the sidebar navigator to find the **Dashboards**
# MAGIC   * Locate the sample dashboard you just loaded; it should be called **Retail Revenue & Supply Chain** and have your username under the **`Created By`** field. **NOTE**: the **My Dashboards** option on the right hand side can serve as a shortcut to filter out other dashboards in the workspace
# MAGIC   * Click on the dashboard name to view it
# MAGIC * View the query behind the **Shifts in Pricing Priorities** plot
# MAGIC   * Hover over the plot; three vertical dots should appear. Click on these
# MAGIC   * Select **View Query** from the menu that appears
# MAGIC * Review the SQL code used to populate this plot
# MAGIC   * Note that 3 tier namespacing is used to identify the source table; this is a preview of new functionality to be supported by Unity Catalog
# MAGIC   * Click **`Run`** in the top right of the screen to preview the results of the query
# MAGIC * Review the visualization
# MAGIC   * Under the query, a tab named **Table** should be selected; click **Price by Priority over Time** to switch to a preview of your plot
# MAGIC   * Click **Edit Visualization** at the bottom of the screen to review settings
# MAGIC   * Explore how changing settings impacts your visualization
# MAGIC   * If you wish to apply your changes, click **Save**; otherwise, click **Cancel**
# MAGIC * Back in the query editor, click the **Add Visualization** button to the right of the visualization name
# MAGIC   * Create a bar graph
# MAGIC   * Set the **X Column** as **`Date`**
# MAGIC   * Set the **Y Column** as **`Total Price`**
# MAGIC   * **Group by** **`Priority`**
# MAGIC   * Set **Stacking** to **`Stack`**
# MAGIC   * Leave all other settings as defaults
# MAGIC   * Click **Save**
# MAGIC * Back in the query editor, click the default name for this visualization to edit it; change the visualization name to **`Stacked Price`**
# MAGIC * Add the bottom of the screen, click the three vertical dots to the left of the **`Edit Visualization`** button
# MAGIC   * Select **Add to Dashboard** from the menu
# MAGIC   * Select your **`Retail Revenue & Supply Chain`** dashboard
# MAGIC * Navigate back to your dashboard to view this change
# MAGIC
# MAGIC # Create a New Query
# MAGIC
# MAGIC * Use the sidebar to navigate to **Queries**
# MAGIC * Click the **`Create Query`** button
# MAGIC * Make sure you are connected to a SQL warehouse. In the **Schema Browser**, click on the current metastore and select **`samples`**. 
# MAGIC   * Select the **`tpch`** database
# MAGIC   * Click on the **`partsupp`** table to get a preview of the schema
# MAGIC   * While hovering over the **`partsupp`** table name, click the **>>** button to insert the table name into your query text
# MAGIC * Write your first query:
# MAGIC   * **`SELECT * FROM`** the **`partsupp`** table using the full name imported in the last step; click **Run** to preview results
# MAGIC   * Modify this query to **`GROUP BY ps_partkey`** and return the **`ps_partkey`** and **`sum(ps_availqty)`**; click **Run** to preview results
# MAGIC   * Update your query to alias the 2nd column to be named **`total_availqty`** and re-execute the query
# MAGIC * Save your query
# MAGIC   * Click the **Save** button next to **Run** near the top right of the screen
# MAGIC   * Give the query a name you'll remember
# MAGIC * Add the query to your dashboard
# MAGIC   * Click the three vertical buttons at the bottom of the screen
# MAGIC   * Click **Add to Dashboard**
# MAGIC   * Select your **`Retail Revenue & Supply Chain`** dashboard
# MAGIC * Navigate back to your dashboard to view this change
# MAGIC   * If you wish to change the organization of visualizations, click the three vertical buttons in the top right of the screen; click **Edit** in the menu that appears and you'll be able to drag and resize visualizations

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>