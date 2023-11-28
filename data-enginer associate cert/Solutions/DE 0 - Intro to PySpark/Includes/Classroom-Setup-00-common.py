# Databricks notebook source
def _setup_tables(create_raw=False):
    print()
    
    DA.clone_source_table("sales", f"{DA.paths.datasets}/ecommerce/delta", "sales_hist")
    DA.clone_source_table("events", f"{DA.paths.datasets}/ecommerce/delta", "events_hist")
    if create_raw:
        DA.clone_source_table("events_raw", f"{DA.paths.datasets}/ecommerce/delta", "events_raw")    
    DA.clone_source_table("users", f"{DA.paths.datasets}/ecommerce/delta", "users_hist")
    DA.clone_source_table("products", f"{DA.paths.datasets}/ecommerce/delta", "item_lookup")

    print()
