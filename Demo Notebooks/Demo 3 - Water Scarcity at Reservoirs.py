# Databricks notebook source
# MAGIC %md
# MAGIC # Example - Water Cycle Year over Year
# MAGIC
# MAGIC This notebook showcases how the Data Analytics Platform can be utilized to illustrate annual trends and variations in water availability. The issue of water scarcity presents significant risks to cities, utilities, and corporations worldwide. Monitoring and comprehending water availability are crucial for effectively managing and utilizing the available water resources. Additionally, it enables proactive planning measures when water scarcity becomes a known and imminent threat. By leveraging the Data Analytics Platform, users can gain insights into water availability patterns, facilitating informed decision-making and sustainable water resource management practices.

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports
# MAGIC **NOTE:** Any Python/Scala/R packages are supported and can be added to the Data Analytics Platform

# COMMAND ----------

# !pip install -U numpy
!pip install prophet
# Generic imports
import pandas as pd
import numpy as np
import logging
import datetime
# import plotly.express as px
from matplotlib import pyplot as plt
import plotly.express as px
import os
import plotly.graph_objects as go
import base64



# COMMAND ----------

# let's modify this

# COMMAND ----------

# MAGIC %md
# MAGIC # Define parameters for data load
# MAGIC
# MAGIC For this example, we will load data for two reservoirs from the data lake. The parameters we will define for use in the data query are:
# MAGIC * START_DATETIME (Datetime) - define start of data query
# MAGIC * END_DATETIME (Datetime) - define end of data query
# MAGIC * INSTRUMENT_IDS (Tuple) - define list of instrument ids for data query
# MAGIC * LOCATION_NAMES (List) - define list of known location names assoicated with instrument ids
# MAGIC

# COMMAND ----------

START_DATETIME = datetime.datetime(2020, 1, 1, 0, 0, 0)
END_DATETIME = datetime.datetime.today()

INSTRUMENT_IDS = (4015,21508)

LOCATION_NAMES = ['WILLOW CREEK LAKE AT HEPPNER, OR', 'LAKE MEAD, AZ']

# COMMAND ----------

# MAGIC %md
# MAGIC These locations were chosen to illustrate how Divirod data can help to monitor the health of reservoirs over time. In this example, one of the chosen reservoirs is a "healthy" reservoir, which is re-filling with a predictable year over year seasonal cycle. The other is an "un-healthy" reservoir. An understanding of trends in this reseroivr will be very important for water users to plan appropriately for less water availability.

# COMMAND ----------

# MAGIC %md
# MAGIC # 1) Load data from Data Lake
# MAGIC The Divirod data lake has pre-loaded 3rd party data (USGS and NOAA, among others). This data is augmented by Divirod locations as they get deployed.
# MAGIC All data is available in one unified format so that they can be combined and pulled into analytics workflows directly from the data lake.

# COMMAND ----------

from delta.tables import *

# use Delta and Spark to load data efficiently
instrument_info = spark.sql("""SELECT * FROM divirod_delta_lake.water_level.instrument_information""")

water_data_spark = DeltaTable.forName(spark, 'divirod_delta_lake.water_level.water_level') \
                     .toDF() \
                     .where(f'instrument_id IN {INSTRUMENT_IDS} AND time > "{START_DATETIME}" AND time < "{END_DATETIME}"').join(instrument_info[["name", "instrument_id", "lat", "lon", "site_type"]], 'instrument_id', 'left')


# Perform a basic calculation to initialize the Spark DataFrame
n_data = water_data_spark.persist().count()
print(f"Loaded {n_data} row for {len(INSTRUMENT_IDS)} locations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1) Convert dataset for convenience.
# MAGIC If the size of the dataset allows, libraries such as python pandas allow for a more convenient and more interactive exploration of data. For this example, given we are looking at only two reservoirs, we will convert to pandas to the sake of transparency.

# COMMAND ----------

from pyspark.sql.functions import col
# convert from spark to python
# water_df = water_data_spark.filter(col('instrument_id')==4015).toPandas()
water_df = water_data_spark.toPandas()
# add a datetime column
water_df['time'] = pd.to_datetime(water_df.epoch, unit='ms')
# filter relevant columns
water_df = water_df[['instrument_id', 'lat', 'lon', 'time', 'epoch', 'datum_native', 'height_native', 'name']].copy()
# sort the data
water_df.sort_values(by='epoch',inplace=True)

water_df['year'] = water_df.time.dt.year
water_df['month'] = water_df.time.dt.month

# COMMAND ----------

# MAGIC %md
# MAGIC # 2) Interactive analytics

# COMMAND ----------

# Separate the data for the two reservoirs
name_0 = LOCATION_NAMES[0]
water_df_0 = water_df.query(f'instrument_id == {INSTRUMENT_IDS[0]}')
name_1 = LOCATION_NAMES[1]
water_df_1 = water_df.query(f'instrument_id == {INSTRUMENT_IDS[1]}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1) Year Over Year

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reservoir with "Normal" Seasonal Cycle

# COMMAND ----------

water_df_0['year'] = water_df_0['time'].dt.year
water_df_0['month'] = water_df_0['time'].dt.month

water_level_data_plot = pd.pivot_table(
    water_df_0, 
    index=water_df_0['time'].dt.dayofyear, 
    columns=water_df_0['year'],
    values='height_native', 
    aggfunc='mean'
)

# COMMAND ----------

water_level_data_plot

# COMMAND ----------

def create_res_plt(water_level_df, location_name):
    water_level_df['year'] = water_level_df['time'].dt.year
    water_level_df['month'] = water_level_df['time'].dt.month

    water_level_data_plot = pd.pivot_table(
        water_level_df, 
        index=water_level_df['time'].dt.dayofyear, 
        columns=water_level_df['year'],
        values='height_native', 
        aggfunc='mean'
    )

    # Create an empty figure
    fig_res = go.Figure()

    # Add traces
    colors = ["#30D158", "#0A84FF", "#AC8E68", "#FF453A"]
    for idx, year in enumerate(water_level_data_plot.columns):
        fig_res.add_trace(
            go.Scatter(
                x=water_level_data_plot.index,
                y=water_level_data_plot[year],
                mode='lines',
                name=str(year),
                hoverinfo='text',  
                text=[f"Height (ft): {height:.2f}" for height in water_level_data_plot[year]],
                line=dict(color=colors[idx % len(colors)])
            )
        )

        with open("/dbfs/FileStore/demo-data/divirod_grayscale_watermark.png", "rb") as image_file:
            base64_image = base64.b64encode(image_file.read()).decode("utf-8")
        
        data_url = f"data:image/png;base64,{base64_image}"

        fig_res.update_layout(
            images=[
            dict(
                source=data_url,
                x=0.5,
                y=0.5,
                xref="paper",
                yref="paper",
                sizex=0.8,
                sizey=0.4,
                xanchor="center",
                yanchor="middle",
                opacity=0.2
            )
        ]
        )

    # Update layout
    fig_res.update_layout(
        title=f"Year over Year Water Level at {location_name}",
        xaxis_title="Day of Year",
        yaxis_title="Gage Height (ft)",
        template="plotly_white",
        width=1000,
        height=600
    )

    # Show figure
    return fig_res


# COMMAND ----------

willow_creek_plot = create_res_plt(water_df_0, name_0)
willow_creek_plot.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The above graph illustrates that this graph has historically re-filled at a predictable rate. More than that, the 2023 re-fill rate is in line with expectations. Water planners and other users of water from this reservoir can have confidence that water availability will be fairly consistent with previous years, limiting operational changes required due to water avaiablity impacts.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reservoir with Decreasing Water Availability

# COMMAND ----------

lake_mead_plot = create_res_plt(water_df_1, name_1)
lake_mead_plot.show()

# COMMAND ----------

# MAGIC %md
# MAGIC While Lake Mead is an extreme example, the above graph very clearly illustrates a "un-healthy" reservoir that is not re-filling at an appropriate rate. This means that water availability is not nearly as predictable as the Willow Creek example given above. This year over year inconsistency can have huge impacts for both water consumption and water rights implications. Understanding which water sources exihibt un-predictable or downward-trending annual re-fill behavior allows water users to understand the risk associated with their water usage.

# COMMAND ----------


