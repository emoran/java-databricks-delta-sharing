# DeltaSharingParquetClient

A **Java** client that calls the [Delta Sharing](https://docs.delta.io/latest/delta-sharing.html) REST API directly, downloads Parquet files (including those with *Deletion Vectors* enabled), and materialises the results in multiple convenient formats.

---

## ✨ Features

* List **shares**, **schemas** and **tables** available to your profile.
* Fetch detailed **table metadata** (protocol & schema).
* Query data and retrieve it as:
  * `org.json.JSONArray`
  * `List<org.json.JSONObject>`
  * `Map<String, JSONObject>` keyed by an arbitrary column
  * `List<DataRecord>` – a lightweight POJO wrapper
  * `Map<String, DataRecord>`
* Automatically downloads Parquet data to a local *temp* directory, parses it with **AvroParquetReader**, then cleans up.
* Limits the number of rows returned via `MAX_ROWS` (default 100) for quick experimentation.

---

## 🗂 Project layout
└── src/main/java/org/example/
└── DeltaSharingParquetClient.java   # main client implementation & demo main()

> **Tip:** If you only need the library, copy `DeltaSharingParquetClient.java` into your project and remove the `main` method.

---

## 🔧 Prerequisites

| Tool            | Version (tested) |
|-----------------|------------------|
| JDK             | 11 or 17         |
| Maven           | 3.9.x            |
| Access to a Delta Sharing server | – |
| `.share` profile file containing `endpoint` & `bearerToken` | – |

All other dependencies (Parquet, Avro, Hadoop, org.json) are fetched from Maven Central.

---

## 📦 Building

```bash
mvn clean package

This produces target/delta-sharing-parquet-client-<version>.jar.

