# Databricks notebook source
# Be sure to run this cell for necesary imports and function definitions
import requests
import json

def get_locations_map(use_delta=False, data_type=None, custom_table=None):
    
    if custom_table:
        info_dict = custom_table.toPandas().dropna(subset=['lat', 'lon', 'instrument_id', 'provider_id']).transpose().to_dict()
        locations = [{'id':x['instrument_id'], 'name':x['name'], 'coordinates':[x['lon'], x['lat']], 'provider':x['provider_id']} for x in info_dict.values()]
    elif use_delta:
        info = spark.read.table('divirod_delta_lake.water_level.instrument_information')
        if data_type:
            info_dict = info.toPandas().dropna(subset=['lat', 'lon', 'instrument_id', 'provider_id']).query(f"data_type == '{data_type}'").transpose().to_dict()
        else:
            info_dict = info.toPandas().dropna(subset=['lat', 'lon', 'instrument_id', 'provider_id']).transpose().to_dict()
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
# MAGIC # Example - Ease of Access to Data
# MAGIC At Divirod, our objective is twofold: to offer a comprehensive water data lake that serves as a valuable resource, and to ensure that accessing the data is simple and intuitive. This notebook provides an overview of the data lake's content and demonstrates how to access data for specific locations.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 1) Load Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1) What's in the data lake

# COMMAND ----------

# MAGIC %md
# MAGIC ### Billions of measurements
# MAGIC The Divirod data lake is enriched with billions of measurements, including pre-loaded data from reputable sources such as USGS and NOAA, among others. Additionally, as Divirod locations are deployed, they contribute to the data lake. Importantly, all the data in the lake is presented in a consistent, standardized format.
# MAGIC
# MAGIC Through the use of data streaming, the entire process from the initial sensor reading on location to data ingestion, cleaning, processing, and ultimately reaching the customer interface can be accomplished in as little as 30 seconds. This streamlined approach ensures swift and efficient access to the most up-to-date information.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) AS n_measurements FROM divirod_delta_lake.water_level.water_level

# COMMAND ----------

# MAGIC %md
# MAGIC ### Thousands of Locations
# MAGIC At Divirod, we are continuously gathering data from thousands of locations worldwide. Presently, our Divirod data lake encompasses over 12,000 diverse locations. This extensive network allows us to collect a wide range of data from various geographical areas, enabling us to provide comprehensive insights and analysis.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT instrument_id) as n_locations FROM divirod_delta_lake.water_level.water_level

# COMMAND ----------

# MAGIC %md
# MAGIC The heatmap in the next section illustrates our current geographic footprint

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2) Easy Data Selection
# MAGIC
# MAGIC Our platform incorporates modern mapping tools that simplify the process of selecting geospatial data through an intuitive interface. Whether you are a technical expert or not, our user-friendly interface ensures effortless selection of geospatial data.
# MAGIC
# MAGIC To utilize the mapping interface, follow these steps:
# MAGIC
# MAGIC 1. Zoom in on the map and draw a polygon around the specific units you are interested in.
# MAGIC 2. Once the polygon is closed, a list of unit IDs will appear.
# MAGIC 3. Copy this list of unit IDs into the "selected_instruments" list in Command 12.
# MAGIC 4. The subsequent analysis demonstrated in this demo will be performed on the units you have selected.
# MAGIC
# MAGIC By following these steps, you can conveniently narrow down your focus to the desired units and proceed with the analysis seamlessly.

# COMMAND ----------

displayHTML(get_locations_map(use_delta=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select the Instruments That You Want

# COMMAND ----------

# MAGIC %md
# MAGIC Replace the empty list below with the list of unit IDs that you copied from the selection process above. Alternatively, if there is a predefined list of selected instruments, uncomment it by removing the comment markers.

# COMMAND ----------

# un-comment the below code (and comment out the empty selected_instruments if you just want to trial with pre-selected units)
# selected_instruments = [2194, 2203, 8235, 8290, 8363, 8365, 8481, 8485, 8519, 8537, 9023, 9329]
selected_instruments = []
selected_instruments_str = '(' + ','.join([str(x) for x in selected_instruments]) + ')'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Specify the Time Frame
# MAGIC
# MAGIC The Divirod data lake currently houses data spanning a period of three years. This dataset offers a comprehensive range of information for analysis and exploration. In addition to the existing data, we have the capability to incorporate historical data archives from third-party sources like NOAA and USGS when required. This ensures that our data lake remains up-to-date and includes a wealth of historical information for comprehensive analysis and research purposes.
# MAGIC
# MAGIC The specified dates will serve as inputs into a later SQL query
# MAGIC  * start_date (String): "YYYY-mm-dd"
# MAGIC  * end_date (String): "YYYY-mm-dd"
# MAGIC

# COMMAND ----------

# For the purpose of this demo, we will use a pre-define time range for the query
start_date = "2022-12-01"
end_date = "2023-01-31"

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Syntax for Data Selection
# MAGIC SQL is a common programming language used by data analysts to manage and manipulate relational databases. Its syntax involves using keywords to retrieve and sort specific data. SQL is a powerful tool for analyzing and extracting insights from large data sets, making it a familiar and essential language for data analysts.

# COMMAND ----------

selected_instruments_str

# COMMAND ----------

instrument_info = spark.sql("""SELECT * FROM divirod_delta_lake.water_level.instrument_information""")
recent_data = spark.sql(f"""SELECT * FROM divirod_delta_lake.water_level.water_level
                            WHERE instrument_id IN{selected_instruments_str}
                            AND time >= "{start_date}"
                            AND time <= "{end_date}"
                            AND height_native > -999999""").join(instrument_info, 'instrument_id', 'left')

# COMMAND ----------

# display the data in a spark dataframe for the selected units over the defined time period
display(recent_data) 

# COMMAND ----------

# MAGIC %md
# MAGIC This data can now be used for trend analysis for as inputs into further modeling!

# COMMAND ----------


