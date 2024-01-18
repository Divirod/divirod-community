# Databricks notebook source
pip install folium geopandas keplergl pydeck pydeck-carto H3 ruptures

# COMMAND ----------

import pandas as pd
import geopandas as gpd
import numpy as np 

# data viz
import folium
import plotly.express as px
from IPython.display import IFrame
import pydeck as pdk
from keplergl import KeplerGl
import h3.api.numpy_int as h3
from shapely.geometry import Polygon
import plotly.graph_objects as go
import base64


# COMMAND ----------

# map out lat lon of verde creek and also map any/all sensors which run along the creek for proximity

# COMMAND ----------

# MAGIC %md
# MAGIC # Problem Statement 1: 
# MAGIC ## Single Site Continuous Monitoring - Water Replenishment Projects
# MAGIC I am on the ESG team at my company and in charge of evaluating the success of our water replenishment projects. I want to pull in the data for a specific project to determine if the stated goals of the project are being met, allowing us claim water credits with certainty.
# MAGIC
# MAGIC For this example, we'll use the [Eureka Ditch Project](https://businessforwater.org/projects/eureka-ditch-project), which is led by the Nature Conservancy - Arizona Chapter in conjunction with other [partners](https://query.prod.cms.rt.microsoft.com/cms/api/am/binary/RW1eD7g). The goal of this project is to repair leaky sections of the ditch, which is used for primarily for irrigation, in order to reduce the volumn of water diverted from the Verde River.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query relevant gauge location information

# COMMAND ----------

# MAGIC %md
# MAGIC To start, I know that the project runs along the Verde River in Arizona, but I can't quite remember the exact USGS gauge we rely on for monitoring. I'll start by using PySpark to send a SQL query to the Divirod Data Lake to pull all gauges with 'Verde River' in the title. 

# COMMAND ----------

verde_river_locations = spark.sql(
    """
    SELECT * 
    FROM divirod_delta_lake.water_level.instrument_information
    WHERE name LIKE '%VERDE RIVER%'
    """
)
verde_river_locations_pd = verde_river_locations.toPandas()

# COMMAND ----------

# examine the dataframe
verde_river_locations_pd

# COMMAND ----------

# add Eureka ditch location so we can map it against the other Verde River gauges
eureka_info = {
    'name': 'EUREKA DITCH',
    'lat': 34.611473,
    'lon': -111.891261
}
new_row = {col: eureka_info.get(col, pd.NA) for col in verde_river_locations_pd.columns}
verde_river_locations_pd = verde_river_locations_pd.append(new_row, ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Map Verde River Gauge Locations

# COMMAND ----------

# plot these on a map 
map_width, map_height = '800px', '400px' 

verde_map = folium.Map(location=[verde_river_locations_pd['lat'].mean(), verde_river_locations_pd['lon'].mean()], zoom_start=8, width=map_width, height=map_height)

# create map and color Eureka differently
for index, row in verde_river_locations_pd.iterrows():
    tooltip = f"Name: {row['name']}, ID: {row['instrument_id']}"

    if row['name'] == 'EUREKA DITCH':
        icon = folium.Icon(color='red')
    else:
        icon = folium.Icon(color='blue')
    
    folium.Marker([row['lat'], row['lon']], icon=icon, tooltip=tooltip).add_to(verde_map)

verde_map

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query & Plot Single Gauge Data Stream

# COMMAND ----------

# MAGIC %md
# MAGIC Now I can be sure that the Verde River Above Camp Verde, AZ (5067) is the gauge that I should be evaluating for this project. I want to pull all the historical data for this gauge in order to perform analysis to determine if our water replenishment efforts are panning out as expected.

# COMMAND ----------

# query all of the data available for the USGS gauge 
verde_project_gauge_data = spark.sql(
    """
    SELECT *
    FROM divirod_delta_lake.water_level.water_level
    WHERE instrument_id = 5067
    ORDER BY time
    """
)
verde_project_gauge_data_pd = verde_project_gauge_data.toPandas()

# COMMAND ----------

# plot water level data at location of interest
verde_gauge_fig = px.line(verde_project_gauge_data_pd, x='time', y='height_native', title='Water level at Verde River Gauge', color_discrete_sequence=['#3d5e8e'])
with open("/dbfs/FileStore/demo-data/divirod_grayscale_watermark.png", "rb") as image_file:
            base64_image = base64.b64encode(image_file.read()).decode("utf-8")
        
data_url = f"data:image/png;base64,{base64_image}"

verde_gauge_fig.update_layout(
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
verde_gauge_fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Change Point Detection - Project Evaluation
# MAGIC This particular project began construction in Fall of 2019. There was not a gauge installed at the Camp Verde site however, until October of 2019. We can only spectulate as to whether or not project operators worked with the USGS to get this gauge installed specifically ot monitor the results of this restoration project. If this is the case, then this process likely took multiple months and tens of thousands of dollars to accomplish - and only to collect project data with no baseline to compare to. This is an example of why deploying a Divirod sensor to collect baseline flow level *prior* to a water replenishment project is vital. 
# MAGIC
# MAGIC Because we don't have baseline data however, for the sake of this example, let's assume that the project was installed at an unknown date sometime in 2020. We'll perform just one example of how a data scientist working on this project may determine if the project is meeting the expected replenishment targets.
# MAGIC
# MAGIC For this demonstration, we'll use a very over-simplified change point detection approach to illustrate how continuous monitoring of an ESG location can provide the data required to validate water replenishment projects. **NOTE: This is only meant to serve as an example, in order to draw any conclusions regarding project success, much more detailed analysis should be performed and seasonailty river conditions should be accounted for (all of which rely on the collection of continuous data from the project location)**.

# COMMAND ----------

import ruptures as rpt
import numpy as np
import pandas as pd
import plotly.graph_objs as go

# COMMAND ----------

def identify_change_point(df: pd.DataFrame):
    """
    Given an input pandas dataframe, identify a change point in the water level stream of data (assuming there is one). Output a plotly chart to illustrate the detected change. Also output the change point timestamp and the mean water level before and after the detected change point.
    """
    data = df['height_native'].values
    algo = rpt.Binseg(model="normal").fit(data)
    result = algo.predict(n_bkps=1)
    change_point = result[0]

    mean_before = np.mean(data[:change_point])
    mean_after = np.mean(data[change_point:])

    print(f"Change point at time: {df['time'][change_point]}")
    print(f"Mean before change point: {mean_before}")
    print(f"Mean after change point: {mean_after}")
    
    # plot with plotly chart
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['time'], y=df['height_native'], mode='lines', name='Height', line=dict(color='#3d5e8e')))
    change_point_time = df['time'].iloc[change_point]
    fig.add_vline(x=change_point_time, line_width=2, line_dash="dash", line_color="red")

    # add shading
    fig.add_vrect(x0=df['time'].min(), x1=change_point_time, 
                  annotation_text="Before change point", annotation_position="top left",
                  fillcolor="#404e5c", opacity=0.2, line_width=0)
    fig.add_vrect(x0=change_point_time, x1=df['time'].max(), 
                  annotation_text="After change point", annotation_position="top right",
                  fillcolor="#dde354", opacity=0.2, line_width=0)

    fig.update_layout(title="Height Time Series with Change Point. (Simple example - no seasonality)", xaxis_title="Time", yaxis_title="Height")

    with open("/dbfs/FileStore/demo-data/divirod_grayscale_watermark.png", "rb") as image_file:
            base64_image = base64.b64encode(image_file.read()).decode("utf-8")
        
    data_url = f"data:image/png;base64,{base64_image}"

    fig.update_layout(
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
    
    fig.show()

# COMMAND ----------

identify_change_point(verde_project_gauge_data_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC In the above example, much more data and analysis is required to confirm whetehr or not the project is sessing success. Some further analysis might include: 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Some other questions to consider
# MAGIC * Was this location chosen for its proximity to the USGS gauge? Could there be other/better locations that can be opened up by similar continuous monitoring at a specified location?
# MAGIC * Unfortunately, there is not much data prior to when the install began, which makes it hard to state with certainty that the project actually had any measurable effect on reducing the amount of water diverted from the Verde River. 
# MAGIC * Even better would have been a network of gauges around the vicinity of the project. For example, a system of gauages before and after the Eureka diversion, as well as in the ditch itself, would be significantly better for measuring the progress of this restoration project. Divirod is able to provide this network to supplement the publically available gauges for a fraction of the cost and time required by public agencies.
# MAGIC
# MAGIC Now, imagine I'm able to also pull in and map all of my water replenishment projects into this same notebook
# MAGIC
# MAGIC * Which areas or types of projects have seen the most success? 
# MAGIC * Are there any regional or operational patterns that seem to be correlated with project success? 
# MAGIC
# MAGIC These types of questions can be answered quickly and thoroughly with Divirod's Water Risk Explorer Platform

# COMMAND ----------

# MAGIC %md
# MAGIC # Problem Statement 2:
# MAGIC ## Understanding water trends at scale
# MAGIC I want to understand trends across large geographic areas and need access to a large, consistent, uniform dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unified data

# COMMAND ----------

# MAGIC %md
# MAGIC The Divirod data lake combines, standardizes, and quality checks water related from across a multitude of providers. This ensures users are able to formulate a complete picture of water data without worrying about compiling the data themselves. For example, we can quickly query the provider list for all gauges in the United States. Normally the data from these providers comes in completely different formats, intervals, and even units. Divirod has compiled data from all providers into a single dataset.

# COMMAND ----------

# use pyspark to query a provider list for gauges in the US
us_provider_list = spark.sql(
    """
    SELECT DISTINCT(provider_name)
    FROM divirod_delta_lake.water_level.instrument_information
    WHERE country_code = 'USA'
    """
)

# COMMAND ----------

# data from the USGS, USBR, and NOAA is all comiled and standardized to a unified schema
display(us_provider_list)

# COMMAND ----------

# MAGIC %md
# MAGIC This is not limited to the US - big data users can be sure that the water data they are looking at for the US is directly comparable to the water they are looking at in Germany, etc. etc.
# MAGIC
# MAGIC For the purposes of this walk through however, we will focus on the US.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Note on Proprietary data storage

# COMMAND ----------

# MAGIC %md
# MAGIC For some users, water data will be the only focus. For others however, the data available in Divirod's data lake will be used as just one input to drive further modeling or analysis. Divirod offers storage for customers (starting at 1 TB of data) if they desire to house additional, propietary data within the  Water Risk Explorer platform. Additionally, users may query APIs **TODO: NEED TO DETERMINE OR GIVE AN EXAMPLE, OR AT LEAST ADDRESS, HOW A SECRETS MANAGER MIGHT WORK HERE**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Driving Inights with Divirod's Datalake + Third Party Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Center Environmental Impact Evaluation

# COMMAND ----------

# MAGIC %md
# MAGIC Divirod's Water Risk Explorer comes with the ability to upload and store proprietary datasets. These can be accessed in the DBFS Filestore system. This example will use electricity emissions data from [ElectricityMaps](https://www.electricitymaps.com/) and a datacenter location dataset for analysis. 
# MAGIC
# MAGIC NOTE: Users may just as easily query and API instead of storing data in the DBFS system. For the purpose of this example, we will rely on DBFS.

# COMMAND ----------

# MAGIC %md
# MAGIC For this example, we'll illustrate how this combination of expansive, unified data in the data lake can help drive insights that are not directly related to water, when combined with third party data. 
# MAGIC
# MAGIC Let's say I am (or am working for) a policy maker, investor, ESG officer, or developer that needs to understand the environmental impact of a portfolio of projects. For this example, let's take a look at data centers in the US. Data centers require extremely large amounts of both electricity (for power) and water (for cooling). By combining the Divirod's data with third party data, we're able to derive insights into the environmental impact of my portfolio of data centers (or properties, investments, etc.).
# MAGIC
# MAGIC This example will use the water data contained in Divirod's data lake, alongside grid emissions data provided by ElectricityMaps. The publicly available data from ElectricityMaps is only published through 2022, so we will focus on that year for now... but remember, the Divirod data lake contains water data as recent as up to a few minutes ago!

# COMMAND ----------

# read in & format data from DBFS
h3_short = pd.read_csv('/dbfs/FileStore/demo-data/electricity_data_202/h3_short.csv')
major_us_data_centers = pd.read_csv('/dbfs/FileStore/demo-data/electricity_data_202/us_data_centers.csv')

major_us_data_centers = major_us_data_centers[major_us_data_centers['Latitude']!='-']
major_us_data_centers['Latitude'] = major_us_data_centers['Latitude'].astype(float)
major_us_data_centers['Longitude'] = major_us_data_centers['Longitude'].astype(float)

h3_short['month'] = pd.to_datetime(h3_short['month'])
h3_short['month'] = h3_short['month'].dt.strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

def create_kepler_html(data_dict, config, height):
  """
  Create an interactive KeplerGL map based on the data and configuration passed.
  """
  # Create a Kepler.gl map
  map_1 = KeplerGl(height=height, config=config)

  # Add data to the map
  for name, data in data_dict.items():
      map_1.add_data(data=data, name=name)
  
  # Get HTML representation of the map and decode it
  html = map_1._repr_html_().decode("utf-8")

  # Additional script for adjusting the height
  additional_script = """<script>
  var targetHeight = "{height}px";
  var interval = window.setInterval(function() {{
    if (document.body && document.body.style && document.body.style.height !== targetHeight) {{
      document.body.style.height = targetHeight;
    }}
  }}, 250);</script>""".format(height=height)
  
  # Combine and return the final HTML
  final_html = html + additional_script
  return final_html

# COMMAND ----------

# set up Kepler GL config
kepler_config = {
    "version": "v1",
    "config": {
        "visState": {
            "filters": [
                {
                    "dataId": ["env_factors"],
                    "id": "env_factors",
                    "name": ["month"],
                    "type": "timeRange",
                    "value": [1640995200000, 1643393553000],
                    "plotType": "histogram",
                    "animationWindow": "free",
                    # "yAxis": null,
                    "view": "default",
                    "speed": 1,
                    "enabled": True,
                }
            ],
            "layers": [
                {
                    "id": "087tml2",
                    "type": "point",
                    "config": {
                        "dataId": "data_centers",
                        "label": "data_centers",
                        "color": [34, 63, 154],
                        "highlightColor": [252, 242, 26, 255],
                        "columns": {"lat": "Latitude", "lng": "Longitude"},
                        "isVisible": True,
                        "visConfig": {
                            "radius": 63,
                            "fixedRadius": False,
                            "opacity": 0.8,
                            "outline": False,
                            "thickness": 2,
                            # "strokeColor": null,
                            "colorRange": {
                                "name": "Global Warming",
                                "type": "sequential",
                                "category": "Uber",
                                "colors": [
                                    "#5A1846",
                                    "#900C3F",
                                    "#C70039",
                                    "#E3611C",
                                    "#F1920E",
                                    "#FFC300",
                                ],
                            },
                            "strokeColorRange": {
                                "name": "Global Warming",
                                "type": "sequential",
                                "category": "Uber",
                                "colors": [
                                    "#5A1846",
                                    "#900C3F",
                                    "#C70039",
                                    "#E3611C",
                                    "#F1920E",
                                    "#FFC300",
                                ],
                            },
                            "radiusRange": [0, 50],
                            "filled": True,
                        },
                        "hidden": False,
                        "textLabel": [
                            {
                                # "field": null,
                                "color": [255, 255, 255],
                                "size": 18,
                                "offset": [0, 0],
                                "anchor": "start",
                                "alignment": "center",
                                "outlineWidth": 0,
                                "outlineColor": [255, 0, 0, 255],
                                "background": False,
                                "backgroundColor": [0, 0, 200, 255],
                            }
                        ],
                    },
                    "visualChannels": {
                        # "colorField": null,
                        "colorScale": "quantile",
                        # "strokeColorField": null,
                        "strokeColorScale": "quantile",
                        # "sizeField": null,
                        "sizeScale": "linear",
                    },
                },
                {
                    "id": "env_factors",
                    "type": "hexagonId",
                    "config": {
                        "dataId": "env_factors",
                        "label": "grid_emissions_intensity",
                        "color": [119, 110, 87],
                        "highlightColor": [252, 242, 26, 255],
                        "columns": {"hex_id": "h3_hexagon"},
                        "isVisible": True,
                        "visConfig": {
                            "colorRange": {
                                "name": "Global Warming",
                                "type": "sequential",
                                "category": "Uber",
                                "colors": [
                                    "#FFC300",
                                    "#F1920E",
                                    "#E3611C",
                                    "#C70039",
                                    "#900C3F",
                                    "#5A1846",
                                ],
                                "reversed": True,
                            },
                            "filled": True,
                            "opacity": 0.35,
                            "outline": False,
                            # "strokeColor": null,
                            "strokeColorRange": {
                                "name": "Global Warming",
                                "type": "sequential",
                                "category": "Uber",
                                "colors": [
                                    "#5A1846",
                                    "#900C3F",
                                    "#C70039",
                                    "#E3611C",
                                    "#F1920E",
                                    "#FFC300",
                                ],
                            },
                            "strokeOpacity": 0.8,
                            "thickness": 2,
                            "coverage": 0.56,
                            "enable3d": False,
                            "sizeRange": [0, 500],
                            "coverageRange": [0, 1],
                            "elevationScale": 5,
                            "enableElevationZoomFactor": True,
                        },
                        "hidden": False,
                        "textLabel": [
                            {
                                # "field": null,
                                "color": [255, 255, 255],
                                "size": 18,
                                "offset": [0, 0],
                                "anchor": "start",
                                "alignment": "center",
                                "outlineWidth": 0,
                                "outlineColor": [255, 0, 0, 255],
                                "background": False,
                                "backgroundColor": [0, 0, 200, 255],
                            }
                        ],
                    },
                    "visualChannels": {
                        "colorField": {
                            "name": "Carbon Intensity gCO₂eq/kWh (direct)",
                            "type": "real",
                        },
                        "colorScale": "quantile",
                        # "strokeColorField": null,
                        "strokeColorScale": "quantile",
                        # "sizeField": null,
                        "sizeScale": "linear",
                        # "coverageField": null,
                        "coverageScale": "linear",
                    },
                },
                {
                    "id": "env_factors_water",
                    "type": "hexagonId",
                    "config": {
                        "dataId": "env_factors",
                        "label": "water_level",
                        "color": [255, 203, 153],
                        "highlightColor": [252, 242, 26, 255],
                        "columns": {"hex_id": "h3_hexagon"},
                        "isVisible": True,
                        "visConfig": {
                            "colorRange": {
                                "name": "Uber Viz Diverging 1.5",
                                "type": "diverging",
                                "category": "Uber",
                                "colors": [
                                    "#C22E00",
                                    "#DD7755",
                                    "#F8C0AA",
                                    "#BAE1E2",
                                    "#5DBABF",
                                    "#00939C",
                                ],
                                "reversed": True,
                            },
                            "filled": True,
                            "opacity": 1,
                            "outline": False,
                            # "strokeColor": null,
                            "strokeColorRange": {
                                "name": "Global Warming",
                                "type": "sequential",
                                "category": "Uber",
                                "colors": [
                                    "#5A1846",
                                    "#900C3F",
                                    "#C70039",
                                    "#E3611C",
                                    "#F1920E",
                                    "#FFC300",
                                ],
                            },
                            "strokeOpacity": 0.8,
                            "thickness": 2,
                            "coverage": 1,
                            "enable3d": False,
                            "sizeRange": [0, 500],
                            "coverageRange": [0, 1],
                            "elevationScale": 5,
                            "enableElevationZoomFactor": True,
                        },
                        "hidden": False,
                        "textLabel": [
                            {
                                # "field": null,
                                "color": [255, 255, 255],
                                "size": 18,
                                "offset": [0, 0],
                                "anchor": "middle",
                                "alignment": "center",
                                "outlineWidth": 0,
                                "outlineColor": [255, 0, 0, 255],
                                "background": False,
                                "backgroundColor": [0, 0, 200, 255],
                            }
                        ],
                    },
                    "visualChannels": {
                        "colorField": {"name": "level", "type": "integer"},
                        "colorScale": "quantile",
                        # "strokeColorField": null,
                        "strokeColorScale": "quantile",
                        # "sizeField": null,
                        "sizeScale": "linear",
                        # "coverageField": null,
                        "coverageScale": "linear",
                    },
                },
            ],
            "effects": [],
            "interactionConfig": {
                "tooltip": {
                    "fieldsToShow": {
                        "env_factors": [
                            {"name": "level"},
                            {"name": "z_score"},
                            {
                                "name": "Carbon Intensity gCO₂eq/kWh (direct)",
                            },
                        ],
                        "data_centers": [{"name": "Owner"}],
                    },
                    "compareMode": False,
                    "compareType": "absolute",
                    "enabled": True,
                },
                "brush": {"size": 0.5, "enabled": False},
                "geocoder": {"enabled": False},
                "coordinate": {"enabled": False},
            },
            "layerBlending": "normal",
            "overlayBlending": "normal",
            "splitMaps": [],
            "animationConfig": { "speed": 1},
            "editor": {"features": [], "visible": True},
        },
        "mapState": {
            "bearing": 0,
            "dragRotate": False,
            "latitude": 32.54087946920454,
            "longitude": -106.34215543157445,
            "pitch": 0,
            "zoom": 3.257294507944831,
            "isSplit": False,
            "isViewportSynced": True,
            "isZoomLocked": False,
            "splitMapViewports": [],
        },
        "mapStyle": {
            # "styleType": "dark-matter",
            "topLayerGroups": {},
            "visibleLayerGroups": {
                "label": True,
                "road": True,
                "border": False,
                "building": True,
                "water": True,
                "land": True,
                "3d building": False,
            },
            "threeDBuildingColor": [
                15.035172933000911,
                15.035172933000911,
                15.035172933000911,
            ],
            "backgroundColor": [0, 0, 0],
            "mapStyles": {},
        },
    },
}

# COMMAND ----------

# create data dicitonary to feed into KeplerGL Map
kepler_map_data = {
    "env_factors": h3_short,
    "data_centers": major_us_data_centers
}
# create html for interactive map
map_html = create_kepler_html(data_dict=kepler_map_data, config=kepler_config, height=700)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interactive Map
# MAGIC * Center hexagons represent grid emissions intensity (darker = higher carbon intensity)
# MAGIC * Outer hexagons represent the Divirod WLI Index 
# MAGIC * Purple dots represent the major data centers in the continental US
# MAGIC
# MAGIC Expand the key in the top left corner to okay around with layer ineractions, filtering data, etc.
# MAGIC
# MAGIC This map is a variation of Divirod's Water Level Index (WLI) Map. The WLI is an internal metric which captures the level of risk from either too much or too little water for *every* gauge in the data lake. Explore the public facing WLI Map [here](https://wli.divirod.com/), and the internal WLI Map at the gauge level is available for Divirod customers [here](https://static.divirod.com/waterrisk/water-risk-analytics.html). Both of these are updated on a daily basis, with the underlying data available to customers as well! For more information on how the WLI is calculated, see the methodology document [here](https://wli.divirod.com/methodology.pdf). 

# COMMAND ----------

displayHTML(map_html)

# COMMAND ----------

# MAGIC %md
# MAGIC You can imagine, by combining Divirod's real time water data with the actual electricity and water usage from each data center, users are able to continuously monitor the environmental footprint and/or risk of a portfolio of assets.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Individual Data Center Risk Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's do some analysis using all of the water level data in the data lake.

# COMMAND ----------

# display csv of major us data centers
major_us_data_centers

# COMMAND ----------

# query ALL gauges in the US contained in the data lake
us_instruments = spark.sql(
    """
    SELECT *
    FROM divirod_delta_lake.water_level.instrument_information
    WHERE country_code = 'USA'
    """
)

# COMMAND ----------

from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import DoubleType
import math

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance in miles between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a)) 
    r = 3956 # radius of earth in miles
    return c * r

def filter_and_sort_dataframe(df, lat, lon, distance=20):
    """
    Filter and sort a Spark DataFrame to include rows within a specified distance
    of a given latitude and longitude, and add a 'distance' column.
    """
    # define a UDF for Haversine distance
    haversine_udf = udf(haversine_distance, DoubleType())

    # add a new column with distances
    df_with_distance = df.withColumn("distance_mi", haversine_udf(lit(lat), lit(lon), col("lat"), col("lon")))

    # filter rows where distance is less than or equal to the specified distance
    df_filtered = df_with_distance.filter(col("distance_mi") <= distance)

    # sort the DataFrame by distance
    return df_filtered.sort("distance_mi")


# COMMAND ----------

# use pheonix gauge as an example
# find all gauges within a 30 mile radius of the pheonix data center
nearest_gauges = filter_and_sort_dataframe(df=us_instruments, lat=33.4484, lon=-112.0740, distance=30)

# COMMAND ----------

display(nearest_gauges)

# COMMAND ----------

# MAGIC %md
# MAGIC From the above table, we can see that the nearest gauge is ~6.5 miles from the Pheonix data center. While this location isn't ideal, without placing a gauge ourselves, it is the best proxy we have to work with to understand water risk at the location of this data center. We'll use this gauge in our analysis in the next steps.

# COMMAND ----------

# identify the nearest gauge location
nearby_gauge_ids =[row['instrument_id'] for row in nearest_gauges.select('instrument_id').collect()]
nearby_gauge_names = [row['name'] for row in nearest_gauges.select('name').collect()]
nearest_gauge_id = nearby_gauge_ids[0]
nearest_gauge_name = nearby_gauge_names[0]

# COMMAND ----------

# print nearest gauge isntrument ID and name
nearest_gauge_id, nearest_gauge_name

# COMMAND ----------

# query all of the available water data for the nearest gauge location
nearest_gauge_data = spark.sql(
    f"""
    SELECT *
    FROM divirod_delta_lake.water_level.water_level
    WHERE instrument_id = {nearest_gauge_id}
    ORDER BY time
    """
)

# COMMAND ----------

# convert from spark to pandas
water_df = nearest_gauge_data.toPandas()
# add a datetime column
water_df['time'] = pd.to_datetime(water_df.epoch, unit='ms')
# filter relevant columns
water_df = water_df[['instrument_id', 'time', 'epoch', 'datum_native', 'height_native']].copy()
# sort the data by time
water_df.sort_values(by='epoch',inplace=True)

water_df['year'] = water_df.time.dt.year
water_df['month'] = water_df.time.dt.month

# COMMAND ----------

def create_res_plt(water_level_df: pd.DataFrame, location_name: str):
    """
    Create a year over year plotly graph for the water level timeseries data contained in the input dataframe. 
    """
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
    colors = ["#30D158", "#0A84FF", "#AC8E68", "#FF453A", "black"]
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
        yaxis_title="Gage Height (m)",
        template="plotly_white",
        width=1000,
        height=600
    )

    # Show figure
    return fig_res


# COMMAND ----------

# MAGIC %md
# MAGIC ### Year over Year Analysis for Individual Gauge
# MAGIC Let's take a look at the data from the nearest gauge compared to past years, this could be used as an input variable for modeling to determine if we need to antipicate modifying operations or re-assigning risk based on water availability compared to previous years.

# COMMAND ----------

create_res_plt(water_level_df=water_df, location_name=nearest_gauge_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Users take this in a multitude of directions based on user requirements - 
# MAGIC * Classification of data centers/assets based on risk
# MAGIC * Forecasting of water level to impact future operation based on upstream gauges
# MAGIC * Etc.
