# Databricks notebook source
# Be sure to run this cell for necesary imports and function definitions
import requests
import json
import pandas as pd

def get_locations_map(use_delta=False, data_type=None, custom_table=None):
    
    if custom_table:
        info_dict = custom_table.toPandas().dropna().transpose().to_dict()
        locations = [{'id':x['instrument_id'], 'name':x['name'], 'coordinates':[x['lon'], x['lat']], 'provider':x['provider_id']} for x in info_dict.values()]
    elif use_delta:
        info = spark.read.table('divirod_delta_lake.water_level.instrument_information')
        if data_type:
            info_dict = info.toPandas().dropna().query(f"data_type == '{data_type}'").transpose().to_dict()
        else:
            info_dict = info.toPandas().dropna().transpose().to_dict()
        locations = [{'id':x['instrument_id'], 'name':x['name'], 'coordinates':[x['lon'], x['lat']], 'provider':x['provider_id']} for x in info_dict.values()]
    else:
        locations = requests.get(
            'https://static.divirod.com/assets/all_locations.json').json()

    return """
      <!DOCTYPE html>
      <html lang="en">
        <head>
          <link
            rel="stylesheet"
            href="https://unpkg.com/leaflet@1.9.2/dist/leaflet.css"
            integrity="sha256-sA+zWATbFveLLNqWO2gtiw3HL/lh1giY/Inf1BJ0z14="
            crossorigin=""
          />
          <title>test</title>
          <link
            rel="icon"
            type="image/x-icon"
            href="https://divirod.com/favicon.ico"
          />
          <style>
            #map {
              height: 600px;
              cursor: crosshair;
            }
          </style>
        </head>

        <body>
          <div id="map"></div>
        </body>
        <script
          src="https://unpkg.com/leaflet@1.9.2/dist/leaflet.js"
          integrity="sha256-o9N1jGDZrf5tS+Ft4gbIK7mYMipq9lqpVJ91xHSyKhg="
          crossorigin=""
        ></script>
        <script src="https://cdn.rawgit.com/hayeswise/Leaflet.PointInPolygon/v1.0.0/wise-leaflet-pip.js"></script>
        <script src="https://static.divirod.com/public-map/main_map/leaflet-heat.js"></script>

        <script>
          var locations = '%LOCATIONS%';
          locations = locations
            .filter((location) => location.coordinates)
            .filter((location) => !location.prospect)
            .map((location) => ({
              ...location,
              latLng: new L.LatLng(location.coordinates[1], location.coordinates[0]),
            }));
          const map = L.map("map").setView([31.505, -40.09], 3);

          var polyline, polygon;
          var vertexGroup = new L.LayerGroup();

          function reset() {
            if (vertexGroup) {
              vertexGroup.eachLayer(function (layer) {
                vertexGroup.removeLayer(layer);
              });
            }
            if (map) {
              map.closePopup();
            }

            polyline = new L.Polyline([]);
            polygon = null;
          }

          reset();

          function finishShape() {
            polygon = new L.Polygon(polyline.getLatLngs());
            vertexGroup.addLayer(polygon);

            const locationIDs = locations
              .filter((location) => polygon.contains(location.latLng))
              .map((location) => location.id)
              .sort((a, b) => a - b);

            const locationIDString = JSON.stringify(locationIDs).replace(/,/g, ", ");
            const locationCount = locationIDs.length;
            var popup = L.popup()
              .setLatLng(polygon.getCenter())
              .setContent(
                `
              <div>
              <span><b>${locationCount} location${
                  locationCount !== 1 ? "s" : ""
                }</b></span>
              <br />
              <textarea readonly type="text" id="idField">${locationIDString}</textarea>
              <br />
              <button onclick="reset()">new polygon</button>
              </div>`
              )
              .openOn(map);
            var copyText = document.getElementById("idField");
            copyText.select();
          }

          function addVertex(e) {
            if (polygon) {
              reset();
            }

            var marker = new L.Marker(e.latlng, {
              icon: new L.DivIcon(),
            });

            if (vertexGroup.getLayers().length === 0) {
              marker.on("click", finishShape, this);
            }

            vertexGroup.addLayer(marker);
            polyline.addLatLng(e.latlng);

            if (polyline.getLatLngs().length === 2) {
              vertexGroup.addLayer(polyline);
            }
          }

          map.addLayer(vertexGroup);
          map.on("click", addVertex);

          const heatmapLayer = L.heatLayer(
            locations.map((location) => [
              location.coordinates[1],
              location.coordinates[0],
              10,
            ]),
            { radius: 10, minOpacity: 0.7 }
          );
          heatmapLayer.addTo(map);

          const markersLayer = L.layerGroup().addTo(map);
          var usingHeatmap = true;

          const redraw = () => {
            const bounds = map.getBounds();
            const locs = locations.filter((location) => {
              return bounds.contains(location.latLng);
            });
            if (locs.length > 1000) {
              if (!usingHeatmap) {
                markersLayer.eachLayer((layer) => {
                  markersLayer.removeLayer(layer);
                });
                map.addLayer(heatmapLayer);
                usingHeatmap = true;
              }
            } else if (usingHeatmap) {
              map.removeLayer(heatmapLayer);
              locs.forEach((location) => {
                const marker = L.marker(location.latLng);
                markersLayer.addLayer(marker);
              });
              usingHeatmap = false;
            }
          };

          map.on("zoomend", redraw);
          map.on("dragend", (e) => {
            redraw();
          });

          const tiles = L.tileLayer(
            "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            {
              maxZoom: 19,
            }
          ).addTo(map);
        </script>
      </html>
    """.replace("'%LOCATIONS%'", json.dumps(locations))

# COMMAND ----------

# MAGIC %md
# MAGIC # Example - Inland Flood Risk
# MAGIC In this notebook, we present a demonstration of how data sourced from the Divirod data lake can be utilized to visualize and comprehend the propagation of floods along a river.
# MAGIC
# MAGIC Inland floods can pose significant risks to various types of infrastructure, including roads, bridges, buildings, and more. With Divirod's vast data lake, customers gain access to real-time data that aids in preparedness and facilitates damage prevention, whenever possible, during flooding events.
# MAGIC
# MAGIC This workflow will guide you through an example showcasing how Divirod's data can be leveraged to track and monitor the potential risks associated with inland flooding.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Background: 2022 Flood in Montana 
# MAGIC
# MAGIC [During June 2022, the state of Montana in the United States experienced severe and destructive floods](https://en.wikipedia.org/wiki/2022_Montana_floods) across multiple major watersheds, including the Yellowstone River. The floods were triggered by heavy rainfall and snowmelt, leading to the evacuation of significant areas within Yellowstone National Park.
# MAGIC
# MAGIC In this demo, we will explore the data available in the Divirod data lake pertaining to measurement locations along the Yellowstone River. By analyzing this data, we can gain insights into the impact and behavior of the river during the flooding events that occurred during that period.

# COMMAND ----------

# MAGIC %md
# MAGIC # 1) Select Data
# MAGIC - We select start and end times based on the flood occurance. These will serve as inputs into a SQL query later in this workflow
# MAGIC   * start_date (String): "YYYY-mm-dd"
# MAGIC   * end_date (String): "YYYY-mm-dd"
# MAGIC - We select location IDS using the interactive map tool.
# MAGIC   * selected_river_locations (List): Location ids relevant to area of analaysis

# COMMAND ----------

# MAGIC %md
# MAGIC Our platform offers modern mapping tools that simplify the selection of geospatial data through an intuitive interface. This user-friendly interface is designed to cater to both technical and non-technical users, making the process effortless.
# MAGIC
# MAGIC To utilize the mapping interface, follow these steps:
# MAGIC
# MAGIC 1. Zoom in on the map and draw a polygon around the units of interest.
# MAGIC 2. Once the polygon is closed, a list of unit IDs will be generated.
# MAGIC 3. This list of unit IDs can be used to filter data queries and retrieve specific information related to the selected units.
# MAGIC
# MAGIC In the example provided in this notebook, we have pre-defined location IDs for the specific problem at hand. Please see Demo Notebook 1 if you would like to experiment more with the map interface.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC For the purpose of this exercise, we will use pre-selected location ids that we know are relevant to the problem we will model in this notebook.
# MAGIC
# MAGIC The locations relevant to the 2022 Montana floods used in this demo are the following location ids: **12573, 14053, 14354, 11074**.
# MAGIC
# MAGIC We will be working with two tables within our datalake:
# MAGIC * instrument_info
# MAGIC * water_level
# MAGIC
# MAGIC The relevant columns from instrument_info are:
# MAGIC * instrument_id (Int): Unique identifier for instrument that is taking measurements
# MAGIC * time (Timestamp): Timestamp of measurement
# MAGIC * provider_id (Integer): internal id used to identidy data provider
# MAGIC * station_id (Integer): internal id used to identidy data station
# MAGIC * last_update (Long): Timestamp is ms when the value was last updated - UTC
# MAGIC * created_at (Timestamp): Timestamp is ms when the value was created - UTC
# MAGIC * data_type (String): The type of measurement observed; water level, volume, precipitation (WL = water level)
# MAGIC * site_type (String): The type of the body of water where the measurement was observed; reservoir, coastal, river (RI = river)
# MAGIC * lower_critical_level (Double): Based on observed, the water level where scarcity is imminent
# MAGIC * lower_warning_level (Double): Based on the observed, the water level where scarcity is a risk
# MAGIC * upper_critical_level (Double): Based on the observed, the water level overflow/flooding risk is imminent
# MAGIC * upper_warning_level (Double): Based on the observed, the water level where overflow/flooding is a risk
# MAGIC * name (String): Name of the station
# MAGIC * lon (Double): Longitude of measurement
# MAGIC * lat (Double): Latitude of measurement
# MAGIC
# MAGIC
# MAGIC The relevant columns from water_level are:
# MAGIC * instrument_id (Int): Unique identifier for instrument that is taking measurements
# MAGIC * height_native (Double): Measurement value as provided by the data provider **NOTE: This is the gauge height in the native datum of the location**
# MAGIC   <br>Note: For a list of the data contained in the water_level table, see our [water_level data dictionary](https://static.divirod.com/documents/data-dictionary.pdf)

# COMMAND ----------

# Locations
# Designate problem specific instrument ids
# For this example, these are pre-defined for our focus area
selected_river_locations = [12573, 14053, 14354, 11074]
selected_river_locations_str = '(' + ','.join([str(x) for x in selected_river_locations]) + ')'

# Select start and end dates for water level monitoring prior to and during flood period
start_date = "2022-06-11"
end_date = "2022-06-30"

# Query the Divirod Data Lake
instrument_info = spark.sql("""SELECT * FROM divirod_delta_lake.water_level.instrument_information""")
river_data = spark.sql(f"""SELECT instrument_id, height_native, time FROM divirod_delta_lake.water_level.water_level
                            WHERE instrument_id IN{selected_river_locations_str}
                            AND time >= "{start_date}"
                            AND time <= "{end_date}"
                            AND height_native > -999999""").join(instrument_info, 'instrument_id', 'left')

# Convert to Pandas for plotting
river_data_df = river_data.toPandas()

# COMMAND ----------

# display a spark dataframe
display(river_data)

# COMMAND ----------

# can also view the data in a pandas dataframe format (remember to only convert from a spark dataframe to a pandas dataframe if the dataset is relatively small)
river_data_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2) Plot Data
# MAGIC Plotting the timeseries readings from each sensor location allows us to visualize how the floodwave propogates over time. Here we plot the un-normalized readings. The first plot has not been normalized to account for differences in elevation along the river path.

# COMMAND ----------

river_data_df_plot.head()

# COMMAND ----------

import plotly.express as px
import base64


def create_timeseries_plot():
    river_data_df_plot = river_data_df.sort_values('time')

    fig = px.line(
        river_data_df_plot, 
        x='time', 
        y='height_native', 
        color='name',
        color_discrete_sequence= ["#FF453A", "#0A84FF", "#AC8E68", "#30D158"],
        labels={'time': 'Time (in UTC)', 'height_native': 'Height (in ft)'}
    )

    # Customize the layout
    fig.update_layout(
        title='Yellowstone River Gauge Height Readings',
        legend_title='Gauge',
        xaxis=dict(title='Time (in UTC)'),
        yaxis=dict(title='Height (in ft)'),

        template="plotly_white",
        width=1000,
        height=600,
        legend=dict(orientation='v', x=0.60, y=1.05),
    )

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
    return fig



# COMMAND ----------

# MAGIC %md
# MAGIC The below plot illustartes the nuances that can occur with river flood scenarios. The Yellowstone Lake Outlet gauge does not show any signs of flooding. However, later down stream the floodwave can be clearly seen propagating through Corwin Springs, Livingston, and eventually hitting the populous town of Billings, MT.

# COMMAND ----------

fig_yellowstone_gauges = create_timeseries_plot()
fig_yellowstone_gauges.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # 3) Show The Data on a Map
# MAGIC
# MAGIC When it is relevant to have a geographical representation of the data, its best to start with a simple map. Below is the code to vizualize the selected data on a simple map. 

# COMMAND ----------

def create_map():
    river_data_summary_df = river_data_df_plot.groupby('instrument_id').agg(
        max_height = ('height_native', max),
        min_height = ('height_native', min),
        lat = ('lat', 'mean'),
        lon = ('lon', 'mean'),
        name = ('name', 'first')

    ).reset_index()

    color_sequence = ["#FF453A", "#0A84FF", "#AC8E68", "#30D158"]

    fig = px.scatter_mapbox(
        river_data_summary_df, 
        lat="lat", 
        lon="lon", 
        hover_name="name", 
        hover_data=["min_height", "max_height"],
        zoom=5, 
        height=300, 
        color='name', 
        color_discrete_sequence=color_sequence
        )

    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_layers=[
            {
                "below": 'traces',
                "opacity": 0.3,
                "sourcetype": "raster",
                "sourceattribution": "United States Geological Survey",
                "source": [
                    "https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryOnly/MapServer/tile/{z}/{y}/{x}"
                ]
            }
        ])

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
            opacity=0.4
        )
    ]
    )

    fig.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0},
        height=400
    )
    return fig

# COMMAND ----------

interactive_map = create_map()
interactive_map.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Hover over each station in the map above for additional station information
