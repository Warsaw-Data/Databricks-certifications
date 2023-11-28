-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create and govern data objects with Unity Catalog
-- MAGIC
-- MAGIC In this notebook you will learn how to:
-- MAGIC * Create catalogs, schemas, tables, views and user-defined functions
-- MAGIC * Control access to these objects
-- MAGIC * Use dynamic views to protect columns and rows within tables
-- MAGIC * Explore grants on various objects in Unity Catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prerequisites
-- MAGIC
-- MAGIC If you would like to follow along with this lab, you must:
-- MAGIC * Have metastore admin permissions in order to create and manage a catalog
-- MAGIC * Have an *analysts* group containing another user with Databricks SQL access
-- MAGIC   * See notebook: Managing principals in Unity Catalog)
-- MAGIC * Have a SQL warehouse to which the user mentioned above has access
-- MAGIC   * See notebook: Creating compute resources for Unity Catalog access

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC
-- MAGIC Run the following cell to perform some setup. In order to avoid conflicts in a shared training environment, this will generate a unique catalog name exclusively for your use, which we will employ shortly.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-06.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Unity Catalog's three-level namespace
-- MAGIC
-- MAGIC Anyone with SQL experience will likely be familiar with the traditional two-level namespace to address tables or views within a schema as follows, as shown in the following example query:
-- MAGIC
-- MAGIC     SELECT * FROM myschema.mytable;
-- MAGIC
-- MAGIC Unity Catalog introduces the concept of a **catalog** into the hierarchy. As a container for schemas, the catalog provides a new way for organizations to segregate their data. There can be as many catalogs as you like, which in turn can contain as many schemas as you like (the concept of a **schema** is unchanged by Unity Catalog; schemas contain data objects like tables, views, and user-defined functions).
-- MAGIC
-- MAGIC To deal with this additional level, complete table/view references in Unity Catalog use a three-level namespace. The following query exemplifies this:
-- MAGIC
-- MAGIC     SELECT * FROM mycatalog.myschema.mytable;
-- MAGIC
-- MAGIC This can be handy in many use cases. For example:
-- MAGIC
-- MAGIC * Separating data relating to business units within your organization (sales, marketing, human resources, etc)
-- MAGIC * Satisfying SDLC requirements (dev, staging, prod, etc)
-- MAGIC * Establishing sandboxes containing temporary datasets for internal use

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a new catalog
-- MAGIC Let's create a new catalog in our metastore. The variable **`${DA.my_new_catalog}`** was displayed by the setup cell above, containing a unique string generated based on your username.
-- MAGIC
-- MAGIC Run the **`CREATE`** statement below, and click the **Data** icon in the left sidebar to confirm this new catalog was created.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ${DA.my_new_catalog}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Select a default catalog
-- MAGIC
-- MAGIC SQL developers will probably also be familiar with the **`USE`** statement to select a default schema, thereby shortening queries by not having to specify it all the time. To extend this convenience while dealing with the extra level in the namespace, Unity Catalog augments the language with two additional statements, shown in the examples below:
-- MAGIC
-- MAGIC     USE CATALOG mycatalog;
-- MAGIC     USE SCHEMA myschema;  
-- MAGIC     
-- MAGIC Let's select the newly created catalog as the default. Now, any schema references will be assumed to be in this catalog unless explicitly overridden by a catalog reference.

-- COMMAND ----------

USE CATALOG ${DA.my_new_catalog}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and use a new schema
-- MAGIC Next, let's create a schema in this new catalog. We won't need to generate another unique name for this schema, since we're now using a unique catalog that is isolated from the rest of the metastore. Let's also set this as the default schema. Now, any data references will be assumed to be in the catalog and schema we created, unless explicitely overridden by a two- or three-level reference.
-- MAGIC
-- MAGIC Run the code below, and click the **Data** icon in the left sidebar to confirm this schema was created in the new catalog we created.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS example;
USE SCHEMA example

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Set up tables and views
-- MAGIC
-- MAGIC With all the necessary containment in place, let's set up tables and views. For this example, we'll use mock data to create and populate a *silver* managed table with synthetic patient heart rate data and a *gold* view that averages heart rate data per patient on a daily basis.
-- MAGIC
-- MAGIC Run the cells below, and click the **Data** icon in the left sidebar to explore the contents of the *example* schema. Note that we don't need to specify three levels when specifying the table or view names below, since we selected a default catalog and schema.

-- COMMAND ----------

CREATE OR REPLACE TABLE heartrate_device (device_id INT, mrn STRING, name STRING, time TIMESTAMP, heartrate DOUBLE);

INSERT INTO heartrate_device VALUES
  (23, "40580129", "Nicholas Spears", "2020-02-01T00:01:58.000+0000", 54.0122153343),
  (17, "52804177", "Lynn Russell", "2020-02-01T00:02:55.000+0000", 92.5136468131),
  (37, "65300842", "Samuel Hughes", "2020-02-01T00:08:58.000+0000", 52.1354807863),
  (23, "40580129", "Nicholas Spears", "2020-02-01T00:16:51.000+0000", 54.6477014191),
  (17, "52804177", "Lynn Russell", "2020-02-01T00:18:08.000+0000", 95.033344842);
  
SELECT * FROM heartrate_device

-- COMMAND ----------

CREATE OR REPLACE VIEW agg_heartrate AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time)
);
SELECT * FROM agg_heartrate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Querying the table above works as expected since we are the data owner. That is, we have ownership of the data object we're querying. Querying the view also works because we are the owner of both the view and the table it's referencing. Thus, no object-level permissions are required to access these resources.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create the _Analysts_ Group
-- MAGIC
-- MAGIC In order to grant access to a specific group of users, we need to create that group and then set permission levels. In order to do this, you would need admin acess to your workspace.

-- COMMAND ----------

-- MAGIC %md ## Grant access to data objects
-- MAGIC
-- MAGIC Unity Catalog employs an explicit permission model by default; no permissions are implied or inherited from containing elements. Therefore, in order to access any data objects, users will need **USAGE** permission on all containing elements; that is, the containing schema and catalog.
-- MAGIC
-- MAGIC Now let's allow members of the *analysts* group to query the *gold* view. In order to do this, we need to grant the following permissions:
-- MAGIC 1. USAGE on the catalog and schema
-- MAGIC 1. SELECT on the data object (e.g. view)

-- COMMAND ----------

-- GRANT USAGE ON CATALOG ${DA.my_new_catalog} TO analysts;
-- GRANT USAGE ON SCHEMA example TO analysts;
-- GRANT SELECT ON VIEW agg_heartrate to analysts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query the view as an analyst
-- MAGIC
-- MAGIC With a data object hierarchy and all the appropriate grants in place, let's attempt to perform a query on the *gold* view as a different user. Recall in the **Prerequisites** section of this lab, we made a reference to having a group called *analysts* containing another user.
-- MAGIC
-- MAGIC In this section, we'll run queries as that user to verify our configuration, and observe the impact when we make changes.
-- MAGIC
-- MAGIC To prepare for this section, **you will need to log in to Databricks using a separate browser session**. This could be a private session, a different profile if your browser supports profiles, or a different browser altogether. Do not merely open a new tab or window using the same browser session; this will lead to login conflicts.
-- MAGIC
-- MAGIC 1. In a separate browser session, <a href="https://accounts.cloud.databricks.com/workspace-select" target="_blank">paste this link to log in to Databricks</a> using the analyst user credentials.
-- MAGIC 1. Switch to the **SQL** persona.
-- MAGIC 1. Go to the **Queries** page and click **Create query**.
-- MAGIC 1. Select the shared SQL warehouse that was created while following the *Creating compute resources for Unity Catalog access* demo.
-- MAGIC 1. Return to this notebook and continue following along. When prompted, we will be switching to the Databricks SQL session and executing queries.
-- MAGIC
-- MAGIC The following cell generates a fully qualified query statement that specifies all three levels for the view, since we will be running this in an environment that doesn't have variables or a default catalog and schema set up. Run the query generated below in the Databricks SQL session. Since all appropriate grants are in place for analysts to access the view, the output should resemble what we saw earlier when querying the *gold* view.

-- COMMAND ----------

SELECT "SELECT * FROM ${DA.my_new_catalog}.example.agg_heartrate" AS Query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query the table as an analyst
-- MAGIC Back in the same query in the Databricks SQL session, let's replace *gold* with *silver* and run the query. This time it fails, because we never set up permissions on the *silver* table. 
-- MAGIC
-- MAGIC Querying *gold* works because the query represented by a view is essentially executed as the owner of the view. This important property enables some interesting security use cases; in this way, views can provide users with a restricted view of sensitive data, without providing access to the underlying data itself. We will see more of this shortly.
-- MAGIC
-- MAGIC For now, you can close and discard the *silver* query in the Databricks SQL session; we will not be using it any more.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and grant access to a user-defined function
-- MAGIC
-- MAGIC Unity Catalog is capable of managing user-defined functions within schemas as well. The code below sets up a simple function that masks all but the last two characters of a string, and then tries it out. Once again, we are the data owner so no grants are required.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION mask(x STRING)
  RETURNS STRING
  RETURN CONCAT(REPEAT("*", LENGTH(x) - 2), RIGHT(x, 2)
); 
SELECT mask('sensitive data') AS data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To allow members of the *analysts* group to run our function, they need **EXECUTE** on the function, along with the requisite **USAGE** grants on the schema and catalog that we've mentioned before.

-- COMMAND ----------

-- GRANT EXECUTE ON FUNCTION mask to analysts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Run a function as an analyst
-- MAGIC
-- MAGIC Now we'll try the function as an analyst in the Databricks SQL session. Paste the fully qualified query statement generated below into a new query to run this function in the Databricks SQL session. Since all appropriate grants are in place for analysts to access the function, the output should resemble what we just saw above.

-- COMMAND ----------

SELECT "SELECT ${DA.my_new_catalog}.example.mask('sensitive data') AS data" AS Query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Protect table columns and rows with dynamic views
-- MAGIC
-- MAGIC We have seen that Unity Catalog's treatment of views provides the ability for views to protect access to tables; users can be granted access to views that manipulate, transform, or obscure data from a source table, without needing to provide direct access to the source table.
-- MAGIC
-- MAGIC Dynamic views provide the ability to do fine-grained access control of columns and rows within a table, conditional on the principal running the query. Dynamic views are an extension to standard views that allow us to do thing like:
-- MAGIC * partially obscure column values or redact them entirely
-- MAGIC * omit rows based on specific criteria
-- MAGIC
-- MAGIC Access control with dynamic views is achieved through the use of functions within the definition of the view. These functions include:
-- MAGIC * **`current_user()`**: returns the email address of the user querying the view
-- MAGIC * **`is_account_group_member()`**: returns TRUE if the user querying the view is a member of the specified group
-- MAGIC
-- MAGIC Note: please refrain from using the legacy function **`is_member()`**, which references workspace-level groups. This is not good practice in the context of Unity Catalog.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Redact columns
-- MAGIC
-- MAGIC Suppose we want analysts to be able to see aggregated data trends from the *gold* view, but we don't want to disclose patient PII. Let's redefine the view to redact the *mrn* and *name* columns using the **`is_account_group_member()`**.
-- MAGIC
-- MAGIC Note: this is a simple training example that doesn't necessarily align with general best practices. For a production system, a more secure approach would be to redact column values for all users who are *not* members of a specific group.

-- COMMAND ----------

CREATE OR REPLACE VIEW agg_heartrate AS
SELECT
  CASE WHEN
    is_account_group_member('analysts') THEN 'REDACTED'
    ELSE mrn
  END AS mrn,
  CASE WHEN
    is_account_group_member('analysts') THEN 'REDACTED'
    ELSE name
  END AS name,
  MEAN(heartrate) avg_heartrate,
  DATE_TRUNC("DD", time) date
  FROM heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time)

-- COMMAND ----------

-- MAGIC %md Re-issue the grant.

-- COMMAND ----------

-- GRANT SELECT ON VIEW agg_heartrate to analysts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's query the view.

-- COMMAND ----------

SELECT * FROM agg_heartrate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For us, this yields unfiltered output since we are not a member of the *analysts* group. 
-- MAGIC
-- MAGIC Now, revisit the Databricks SQL session and rerun the query on the *gold* view as an analyst. Run the cell below to generate this query. 
-- MAGIC
-- MAGIC We will see that the *mrn* and *name* colum values have been redacted.

-- COMMAND ----------

SELECT "SELECT * FROM ${DA.my_new_catalog}.example.agg_heartrate" AS Query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Restrict rows
-- MAGIC
-- MAGIC Now let's suppose we want a view that, rather than aggregating and redacting columns, simply filters out rows from the source. Let's  apply the same **`is_account_group_member()`** function to create a view that passes through only rows whose *device_id* is less than 30. Row filtering is done by applying the conditional as a **`WHERE`** clause.

-- COMMAND ----------

CREATE OR REPLACE VIEW agg_heartrate AS
SELECT
  mrn,
  time,
  device_id,
  heartrate
FROM heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('analysts') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

-- MAGIC %md Re-issue the grant.

-- COMMAND ----------

-- GRANT SELECT ON VIEW agg_heartrate to analysts

-- COMMAND ----------

SELECT * FROM agg_heartrate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For us, this displays all ten records. Now, revisit the Databricks SQL session and rerun the query on the *gold* view as an analyst. We will see that three records are missing. Those records contained values for *device_id* that were caught by the filter.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data masking
-- MAGIC One final use case for dynamic views is data masking, or partially obscuring data. In the first example, we redacted columns entirely. Masking is similar in principle except we are displaying some of the data rather than replacing it entirely. And for this simple example, we'll leverage the *mask()* user-defined function that we created earlier to mask the *mrn* column for our analysts, though SQL provides a fairly comprehensive library of built-in data manipulation functions that can be leveraged to mask data in a number of different ways. It's good practice to leverage those when you can.

-- COMMAND ----------

CREATE OR REPLACE VIEW agg_heartrate AS
SELECT
  CASE WHEN
    is_account_group_member('analysts') THEN mask(mrn)
    ELSE mrn
  END AS mrn,
  time,
  device_id,
  heartrate
FROM heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('analysts') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

-- MAGIC %md Re-issue the grant.

-- COMMAND ----------

-- GRANT SELECT ON VIEW agg_heartrate to analysts

-- COMMAND ----------

SELECT * FROM agg_heartrate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For us, this displays undisturbed records. Now, revisit the Databricks SQL session and rerun the query on the *gold* view as an analyst. All values in the *mrn* column will be masked.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Explore objects
-- MAGIC
-- MAGIC Let's explore some SQL statements to examine our data objects and permissions. Let's begin by taking stock of the objects we have in the *examples* schema.

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW VIEWS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the above two statements, we didn't specify a schema since we are relying on the defaults we selected. Alternatively, we could have been more explicit using a statement like **`SHOW TABLES IN example`**.
-- MAGIC
-- MAGIC Now let's step up a level in the hierarchy and take inventory of the schemas in our catalog. Once again, we are leveraging the fact that we have a default catalog selected. If we wanted to be more explicit, we could use something like **`SHOW SCHEMAS IN ${DA.my_new_catalog}`**.

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The *example* schema, of course, is the one we created earlier. The *default* schema is created by default as per SQL conventions when creating a new catalog.
-- MAGIC
-- MAGIC Finally, let's list the catalogs in our metastore.

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC There may be more entries than you were expecting. At a minimum, you will see:
-- MAGIC * A catalog beginning with the prefix *dbacademy_*, which is the one we created earlier.
-- MAGIC * *hive_metastore*, which is not a real catalog in the metastore, but rather a virtual representation of the workspace local Hive metastore. Use this to access workspace-local tables and views.
-- MAGIC * *main*, a catalog which is created by default with each new metastore.
-- MAGIC * *samples*, another virtual catalog that presents example datasets provided by Databricks
-- MAGIC
-- MAGIC There may be more catalogs present depending on the historical activity in your metastore.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Explore permissions
-- MAGIC
-- MAGIC Now let's explore permissions using **`SHOW GRANTS`**, starting with the *gold* view and working our way up.

-- COMMAND ----------

-- SHOW GRANTS ON VIEW agg_heartrate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Currenly there is only the **SELECT** grant that we just set up. Now let's check the grants on *silver*.

-- COMMAND ----------

-- SHOW GRANTS ON TABLE heartrate_device

-- COMMAND ----------

-- MAGIC %md
-- MAGIC There are no grants on this table currently. Only we, the data owner, can access this table directly. Anyone with permission to access the *gold* view, for which we are also the data owner, has the ability to access this table indirectly.
-- MAGIC
-- MAGIC Now let's look at the containing schema.

-- COMMAND ----------

-- SHOW GRANTS ON SCHEMA example

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Currently we see the **USAGE** grant we set up earlier.
-- MAGIC
-- MAGIC Now let's examine the catalog.

-- COMMAND ----------

-- SHOW GRANTS ON CATALOG ${DA.my_new_catalog}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Likewise, we see **USAGE** which we granted moments ago.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Revoke access
-- MAGIC
-- MAGIC No data governance platform would be complete without the ability to revoke previously issued grants. Let's start by examining access to the *mask()* function.

-- COMMAND ----------

-- SHOW GRANTS ON FUNCTION mask

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's revoke this grant.

-- COMMAND ----------

-- REVOKE EXECUTE ON FUNCTION mask FROM analysts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's re-examine the access, which will now be empty.

-- COMMAND ----------

-- SHOW GRANTS ON FUNCTION mask

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Revisit the Databricks SQL session an re-run the query against the *gold* view as an analyst. Notice that this still works as it did before. Does this surprise you? Why or why not?
-- MAGIC
-- MAGIC Remember that the view is effectively running as its owner, who also happens to own the function and the source table. Just like the analyst didn't require direct access to the table being queried since the view owner has ownership of the table, the function continues to work for the same reason.
-- MAGIC
-- MAGIC Now let's try something different. Let's break the permission chain by revoking **USAGE** on the catalog.

-- COMMAND ----------

-- REVOKE USAGE ON CATALOG ${DA.my_new_catalog} FROM analysts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Back in Databricks SQL, re-run the *gold* query as the analyst, and we see now that even though we have proper permissions on the view and schema, the missing privilege higher up in the hierarchy will break access to this resource. This illustrates Unity Catalog's explicit permission model in action: no permissions are implied or inherited.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean up
-- MAGIC Let's run the following cell to remove the catalog that we created earlier. The **`CASCADE`** qualifier will remove the catalog along with any contained elements.

-- COMMAND ----------

USE CATALOG hive_metastore;
DROP CATALOG IF EXISTS ${DA.my_new_catalog} CASCADE;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>