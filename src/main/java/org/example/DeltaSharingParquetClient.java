package org.example;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * A Java client that uses the Delta Sharing REST API directly
 * to access tables with DeletionVectors enabled, with Parquet parsing.
 */
public class DeltaSharingParquetClient {

    private static final String OUTPUT_JSON_FILE = "delta_share_data.json";
    private static final int MAX_ROWS = 100;
    private static final String TEMP_DIR = "./temp_parquet";

    private final HttpClient httpClient;
    private final String endpoint;
    private final String bearerToken;
    private final Configuration hadoopConf;

    /**
     * Custom class to represent data records with a more structured approach.
     */
    public static class DataRecord {
        private Map<String, Object> fields = new HashMap<>();

        public void setField(String name, Object value) {
            fields.put(name, value);
        }

        public Object getField(String name) {
            return fields.get(name);
        }

        public boolean hasField(String name) {
            return fields.containsKey(name);
        }

        public Map<String, Object> getAllFields() {
            return new HashMap<>(fields);
        }

        @Override
        public String toString() {
            return fields.toString();
        }
    }

    /**
     * Creates a new REST client using the specified profile file.
     *
     * @param profilePath Path to the Delta Sharing profile file
     * @throws IOException If the profile file cannot be read
     */
    public DeltaSharingParquetClient(String profilePath) throws IOException {
        // Read the profile file to get endpoint and bearer token
        String profileContent = new String(Files.readAllBytes(Paths.get(profilePath)));
        JSONObject profile = new JSONObject(profileContent);

        this.endpoint = profile.getString("endpoint");
        this.bearerToken = profile.getString("bearerToken");

        // Create an HTTP client
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        // Create Hadoop configuration for Parquet reading
        this.hadoopConf = new Configuration();

        // Important Hadoop settings for local file access
        this.hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        this.hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        // Ensure temp directory exists
        File tempDir = new File(TEMP_DIR);
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }

        System.out.println("Initialized REST client with endpoint: " + endpoint);
    }

    /**
     * Gets a list of all available shares.
     *
     * @return A list of share names
     * @throws IOException          If the HTTP request fails
     * @throws InterruptedException If the HTTP request is interrupted
     */
    public List<JSONObject> listShares() throws IOException, InterruptedException {
        String sharesUrl = endpoint + "/shares";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(sharesUrl))
                .header("Authorization", "Bearer " + bearerToken)
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("Failed to list shares: " + response.statusCode() + " " + response.body());
        }

        // Parse the response to get shares
        JSONObject jsonResponse = new JSONObject(response.body());
        JSONArray items = jsonResponse.getJSONArray("items");

        List<JSONObject> shares = new ArrayList<>();
        for (int i = 0; i < items.length(); i++) {
            shares.add(items.getJSONObject(i));
        }

        return shares;
    }

    /**
     * Gets a list of all schemas in a share.
     *
     * @param shareName The name of the share
     * @return A list of schema names
     * @throws IOException          If the HTTP request fails
     * @throws InterruptedException If the HTTP request is interrupted
     */
    public List<JSONObject> listSchemas(String shareName) throws IOException, InterruptedException {
        String schemasUrl = endpoint + "/shares/" + shareName + "/schemas";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(schemasUrl))
                .header("Authorization", "Bearer " + bearerToken)
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("Failed to list schemas: " + response.statusCode() + " " + response.body());
        }

        // Parse the response to get schemas
        JSONObject jsonResponse = new JSONObject(response.body());
        JSONArray items = jsonResponse.getJSONArray("items");

        List<JSONObject> schemas = new ArrayList<>();
        for (int i = 0; i < items.length(); i++) {
            schemas.add(items.getJSONObject(i));
        }

        return schemas;
    }

    /**
     * Gets a list of all tables in a schema.
     *
     * @param shareName  The name of the share
     * @param schemaName The name of the schema
     * @return A list of table names
     * @throws IOException          If the HTTP request fails
     * @throws InterruptedException If the HTTP request is interrupted
     */
    public List<JSONObject> listTables(String shareName, String schemaName) throws IOException, InterruptedException {
        String tablesUrl = endpoint + "/shares/" + shareName + "/schemas/" + schemaName + "/tables";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tablesUrl))
                .header("Authorization", "Bearer " + bearerToken)
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("Failed to list tables: " + response.statusCode() + " " + response.body());
        }

        // Parse the response to get tables
        JSONObject jsonResponse = new JSONObject(response.body());
        JSONArray items = jsonResponse.getJSONArray("items");

        List<JSONObject> tables = new ArrayList<>();
        for (int i = 0; i < items.length(); i++) {
            tables.add(items.getJSONObject(i));
        }

        return tables;
    }

    /**
     * Gets the metadata for a table.
     *
     * @param shareName  The name of the share
     * @param schemaName The name of the schema
     * @param tableName  The name of the table
     * @return The table metadata
     * @throws IOException          If the HTTP request fails
     * @throws InterruptedException If the HTTP request is interrupted
     */
    public JSONObject getTableMetadata(String shareName, String schemaName, String tableName) throws IOException, InterruptedException {
        String tableUrl = endpoint + "/shares/" + shareName + "/schemas/" + schemaName + "/tables/" + tableName + "/metadata";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tableUrl))
                .header("Authorization", "Bearer " + bearerToken)
                .header("delta-sharing-capabilities", "responseFormat=delta;readerfeatures=deletionVectors")
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("Failed to get table metadata: " + response.statusCode() + " " + response.body());
        }

        // Parse the metadata line by line
        String[] lines = response.body().split("\n");

        if (lines.length < 2) {
            throw new IOException("Invalid metadata response: fewer than 2 lines");
        }

        // The first line is the protocol, the second line is the metadata
        JSONObject protocol = new JSONObject(lines[0]);
        JSONObject metadata = new JSONObject(lines[1]);

        // Get schema information from metadata
        if (metadata.has("metaData") && metadata.getJSONObject("metaData").has("deltaMetadata") &&
                metadata.getJSONObject("metaData").getJSONObject("deltaMetadata").has("schemaString")) {
            String schemaString = metadata.getJSONObject("metaData")
                    .getJSONObject("deltaMetadata")
                    .getString("schemaString");
            System.out.println("Schema: " + schemaString);
        }

        // Combine into a single object for easier use
        JSONObject result = new JSONObject();
        result.put("protocol", protocol);
        result.put("metadata", metadata);

        return result;
    }

    /**
     * Queries a table and returns the data.
     *
     * @param shareName  The name of the share
     * @param schemaName The name of the schema
     * @param tableName  The name of the table
     * @param limitRows  Maximum number of rows to return
     * @return The table data as JSON
     * @throws IOException          If the HTTP request fails
     * @throws InterruptedException If the HTTP request is interrupted
     */
    public JSONArray queryTable(String shareName, String schemaName, String tableName, int limitRows) throws IOException, InterruptedException {
        // Get data as list and convert to JSONArray
        List<JSONObject> records = queryTableAsList(shareName, schemaName, tableName, limitRows);
        JSONArray result = new JSONArray();
        for (JSONObject record : records) {
            result.put(record);
        }
        return result;
    }

    /**
     * Queries a table and returns the data as a List of JSONObjects.
     *
     * @param shareName  The name of the share
     * @param schemaName The name of the schema
     * @param tableName  The name of the table
     * @param limitRows  Maximum number of rows to return
     * @return The table data as a List of JSONObjects
     * @throws IOException          If the HTTP request fails
     * @throws InterruptedException If the HTTP request is interrupted
     */
    public List<JSONObject> queryTableAsList(String shareName, String schemaName, String tableName, int limitRows) throws IOException, InterruptedException {
        String queryUrl = endpoint + "/shares/" + shareName + "/schemas/" + schemaName + "/tables/" + tableName + "/query";

        // Create JSON request body - this is essential for the query endpoint
        JSONObject requestBody = new JSONObject();

        // Add limit parameter if specified
        if (limitRows > 0) {
            requestBody.put("maxFiles", limitRows);
        }

        // Create the request with the JSON body
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(queryUrl))
                .header("Authorization", "Bearer " + bearerToken)
                .header("Content-Type", "application/json")
                .header("delta-sharing-capabilities", "responseFormat=delta;readerfeatures=deletionVectors")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody.toString()))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("Failed to query table: " + response.statusCode() + " " + response.body());
        }

        // Print the raw response for debugging
        System.out.println("Raw response from query endpoint (first 500 chars):");
        System.out.println(response.body().substring(0, Math.min(500, response.body().length())));

        // Get access to table files from the response
        List<JSONObject> files = new ArrayList<>();
        String[] lines = response.body().split("\n");

        // Extract schema from the metadata line
        JSONObject metadataJson = new JSONObject(lines[1]);
        String schemaString = null;
        if (metadataJson.has("metaData") &&
                metadataJson.getJSONObject("metaData").has("deltaMetadata") &&
                metadataJson.getJSONObject("metaData").getJSONObject("deltaMetadata").has("schemaString")) {
            schemaString = metadataJson.getJSONObject("metaData")
                    .getJSONObject("deltaMetadata")
                    .getString("schemaString");
        }

        // The first 2 lines are protocol and metadata, the rest are files
        for (int i = 2; i < lines.length; i++) {
            JSONObject fileInfo = new JSONObject(lines[i]);
            if (fileInfo.has("file")) {
                files.add(fileInfo.getJSONObject("file"));
            }
        }

        System.out.println("Found " + files.size() + " data files to process");

        // Now download and parse the actual data files
        List<JSONObject> allRecords = new ArrayList<>();
        int recordCount = 0;

        for (JSONObject file : files) {
            String url = file.getString("url");
            try {
                // Download and parse the Parquet file
                List<JSONObject> records = downloadAndParseParquetFile(url, schemaString);

                // Add records to the result (up to the limit)
                for (JSONObject record : records) {
                    if (recordCount < limitRows) {
                        allRecords.add(record);
                        recordCount++;
                    } else {
                        break;
                    }
                }

                if (recordCount >= limitRows) {
                    break;
                }
            } catch (Exception e) {
                System.err.println("Error processing file " + url + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        System.out.println("Retrieved " + allRecords.size() + " records");
        return allRecords;
    }

    /**
     * Queries a table and returns the data as a Map with a specified field as the key.
     *
     * @param shareName  The name of the share
     * @param schemaName The name of the schema
     * @param tableName  The name of the table
     * @param keyField   The field to use as the key in the map
     * @param limitRows  Maximum number of rows to return
     * @return The table data as a Map with the specified field as key
     * @throws IOException          If the HTTP request fails
     * @throws InterruptedException If the HTTP request is interrupted
     */
    public Map<String, JSONObject> queryTableAsMap(String shareName, String schemaName, String tableName,
                                                   String keyField, int limitRows) throws IOException, InterruptedException {
        // Get the data as a list first
        List<JSONObject> records = queryTableAsList(shareName, schemaName, tableName, limitRows);

        // Convert the list to a map using the specified key field
        Map<String, JSONObject> recordMap = new HashMap<>();

        for (JSONObject record : records) {
            if (record.has(keyField)) {
                String key = record.get(keyField).toString();
                recordMap.put(key, record);
            } else {
                System.err.println("Warning: Record is missing the key field: " + keyField);
            }
        }

        System.out.println("Converted " + recordMap.size() + " records to map with key field: " + keyField);
        return recordMap;
    }

    /**
     * Queries a table and returns structured DataRecord objects as a List.
     *
     * @param shareName  The name of the share
     * @param schemaName The name of the schema
     * @param tableName  The name of the table
     * @param limitRows  Maximum number of rows to return
     * @return The table data as a List of DataRecord objects
     * @throws IOException          If the HTTP request fails
     * @throws InterruptedException If the HTTP request is interrupted
     */
    public List<DataRecord> queryTableAsDataRecordList(String shareName, String schemaName, String tableName, int limitRows)
            throws IOException, InterruptedException {
        String queryUrl = endpoint + "/shares/" + shareName + "/schemas/" + schemaName + "/tables/" + tableName + "/query";

        // Create JSON request body - this is essential for the query endpoint
        JSONObject requestBody = new JSONObject();

        // Add limit parameter if specified
        if (limitRows > 0) {
            requestBody.put("maxFiles", limitRows);
        }

        // Create the request with the JSON body
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(queryUrl))
                .header("Authorization", "Bearer " + bearerToken)
                .header("Content-Type", "application/json")
                .header("delta-sharing-capabilities", "responseFormat=delta;readerfeatures=deletionVectors")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody.toString()))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("Failed to query table: " + response.statusCode() + " " + response.body());
        }

        // Get access to table files from the response
        List<JSONObject> files = new ArrayList<>();
        String[] lines = response.body().split("\n");

        // Extract schema from the metadata line
        JSONObject metadataJson = new JSONObject(lines[1]);
        String schemaString = null;
        if (metadataJson.has("metaData") &&
                metadataJson.getJSONObject("metaData").has("deltaMetadata") &&
                metadataJson.getJSONObject("metaData").getJSONObject("deltaMetadata").has("schemaString")) {
            schemaString = metadataJson.getJSONObject("metaData")
                    .getJSONObject("deltaMetadata")
                    .getString("schemaString");
        }

        // The first 2 lines are protocol and metadata, the rest are files
        for (int i = 2; i < lines.length; i++) {
            JSONObject fileInfo = new JSONObject(lines[i]);
            if (fileInfo.has("file")) {
                files.add(fileInfo.getJSONObject("file"));
            }
        }

        System.out.println("Found " + files.size() + " data files to process");

        // Now download and parse the actual data files
        List<DataRecord> allRecords = new ArrayList<>();
        int recordCount = 0;

        for (JSONObject file : files) {
            String url = file.getString("url");
            try {
                // Download and parse the Parquet file
                List<DataRecord> records = downloadAndParseParquetFileAsDataRecords(url, schemaString);

                // Add records to the result (up to the limit)
                for (DataRecord record : records) {
                    if (recordCount < limitRows) {
                        allRecords.add(record);
                        recordCount++;
                    } else {
                        break;
                    }
                }

                if (recordCount >= limitRows) {
                    break;
                }
            } catch (Exception e) {
                System.err.println("Error processing file " + url + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        System.out.println("Retrieved " + allRecords.size() + " records");
        return allRecords;
    }

    /**
     * Queries a table and returns structured DataRecord objects as a Map.
     *
     * @param shareName  The name of the share
     * @param schemaName The name of the schema
     * @param tableName  The name of the table
     * @param keyField   The field to use as the key in the map
     * @param limitRows  Maximum number of rows to return
     * @return The table data as a Map with the specified field as key and DataRecord as value
     * @throws IOException          If the HTTP request fails
     * @throws InterruptedException If the HTTP request is interrupted
     */
    public Map<String, DataRecord> queryTableAsDataRecordMap(String shareName, String schemaName, String tableName,
                                                             String keyField, int limitRows) throws IOException, InterruptedException {
        // Get the data as a list first
        List<DataRecord> records = queryTableAsDataRecordList(shareName, schemaName, tableName, limitRows);

        // Convert the list to a map using the specified key field
        Map<String, DataRecord> recordMap = new HashMap<>();

        for (DataRecord record : records) {
            if (record.hasField(keyField)) {
                String key = record.getField(keyField).toString();
                recordMap.put(key, record);
            } else {
                System.err.println("Warning: Record is missing the key field: " + keyField);
            }
        }

        System.out.println("Converted " + recordMap.size() + " records to map with key field: " + keyField);
        return recordMap;
    }

    /**
     * Downloads a Parquet file and parses its contents into JSON objects.
     *
     * @param url        The URL of the Parquet file
     * @param schemaJson The JSON schema of the table (optional)
     * @return A list of JSON objects representing the records in the file
     * @throws IOException If the file cannot be downloaded or parsed
     */
    private List<JSONObject> downloadAndParseParquetFile(String url, String schemaJson) throws IOException {
        List<JSONObject> results = new ArrayList<>();

        // Create a temporary file to store the Parquet data
        String filename = "temp_" + System.currentTimeMillis() + ".parquet";
        File tempFile = new File(TEMP_DIR, filename);

        try {
            // Download the file
            System.out.println("Downloading Parquet file from: " + url);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

            if (response.statusCode() != 200) {
                throw new IOException("Failed to download file: " + response.statusCode());
            }

            // Save the InputStream to a file
            try (InputStream inputStream = response.body();
                 FileOutputStream outputStream = new FileOutputStream(tempFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
            }

            System.out.println("Downloaded to: " + tempFile.getAbsolutePath());

            try {
                // Create a proper Path object from the file path
                Path hadoopPath = new Path(tempFile.toURI());

                // Get the file size to verify download
                long fileSize = tempFile.length();
                System.out.println("File size: " + fileSize + " bytes");

                if (fileSize == 0) {
                    System.err.println("Warning: Downloaded file is empty!");
                    return results;
                }

                // Read the footer to verify Parquet format
                ParquetMetadata metadata = ParquetFileReader.readFooter(hadoopConf, hadoopPath);
                System.out.println("File contains " + metadata.getBlocks().size() + " row groups");
                System.out.println("File schema: " + metadata.getFileMetaData().getSchema());

                // Create a reader that correctly points to the file
                try (org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader = AvroParquetReader
                        .<GenericRecord>builder(HadoopInputFile.fromPath(hadoopPath, hadoopConf))
                        .withConf(hadoopConf)
                        .build()) {

                    // Read all records
                    GenericRecord record;
                    int count = 0;

                    while ((record = reader.read()) != null && count < MAX_ROWS) {
                        // Convert to JSON
                        JSONObject jsonRecord = convertRecordToJson(record);
                        results.add(jsonRecord);
                        count++;

                        // Log progress periodically
                        if (count % 100 == 0) {
                            System.out.println("Processed " + count + " records...");
                        }
                    }
                }

                System.out.println("Successfully parsed " + results.size() + " records from Parquet file");
            } catch (Exception e) {
                System.err.println("Error parsing Parquet file: " + e.getMessage());
                e.printStackTrace();

                // Print file properties to help diagnose issues
                System.err.println("File exists: " + tempFile.exists());
                System.err.println("File readable: " + tempFile.canRead());
                System.err.println("File size: " + tempFile.length());
            }
        } catch (Exception e) {
            System.err.println("Error in downloadAndParseParquetFile: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up temporary file
            if (tempFile.exists()) {
                boolean deleted = tempFile.delete();
                if (!deleted) {
                    System.err.println("Warning: Could not delete temporary file: " + tempFile.getAbsolutePath());
                }
            }
        }

        return results;
    }

    /**
     * Downloads a Parquet file and parses its contents into DataRecord objects.
     *
     * @param url        The URL of the Parquet file
     * @param schemaJson The JSON schema of the table (optional)
     * @return A list of DataRecord objects representing the records in the file
     * @throws IOException If the file cannot be downloaded or parsed
     */
    private List<DataRecord> downloadAndParseParquetFileAsDataRecords(String url, String schemaJson) throws IOException {
        List<DataRecord> results = new ArrayList<>();

        // Create a temporary file to store the Parquet data
        String filename = "temp_" + System.currentTimeMillis() + ".parquet";
        File tempFile = new File(TEMP_DIR, filename);

        try {
            // Download the file
            System.out.println("Downloading Parquet file from: " + url);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

            if (response.statusCode() != 200) {
                throw new IOException("Failed to download file: " + response.statusCode());
            }

            // Save the InputStream to a file
            try (InputStream inputStream = response.body();
                 FileOutputStream outputStream = new FileOutputStream(tempFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
            }

            System.out.println("Downloaded to: " + tempFile.getAbsolutePath());

            try {
                // Create a proper Path object from the file path
                Path hadoopPath = new Path(tempFile.toURI());

                // Get the file size to verify download
                long fileSize = tempFile.length();
                System.out.println("File size: " + fileSize + " bytes");

                if (fileSize == 0) {
                    System.err.println("Warning: Downloaded file is empty!");
                    return results;
                }

                // Read the footer to verify Parquet format
                ParquetMetadata metadata = ParquetFileReader.readFooter(hadoopConf, hadoopPath);
                System.out.println("File contains " + metadata.getBlocks().size() + " row groups");
                System.out.println("File schema: " + metadata.getFileMetaData().getSchema());

                // Create a reader that correctly points to the file
                try (org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader = AvroParquetReader
                        .<GenericRecord>builder(HadoopInputFile.fromPath(hadoopPath, hadoopConf))
                        .withConf(hadoopConf)
                        .build()) {

                    // Read all records
                    GenericRecord record;
                    int count = 0;

                    while ((record = reader.read()) != null && count < MAX_ROWS) {
                        // Convert to DataRecord
                        DataRecord dataRecord = convertRecordToDataRecord(record);
                        results.add(dataRecord);
                        count++;

                        // Log progress periodically
                        if (count % 100 == 0) {
                            System.out.println("Processed " + count + " records...");
                        }
                    }
                }

                System.out.println("Successfully parsed " + results.size() + " records from Parquet file");
            } catch (Exception e) {
                System.err.println("Error parsing Parquet file: " + e.getMessage());
                e.printStackTrace();

                // Print file properties to help diagnose issues
                System.err.println("File exists: " + tempFile.exists());
                System.err.println("File readable: " + tempFile.canRead());
                System.err.println("File size: " + tempFile.length());
            }
        } catch (Exception e) {
            System.err.println("Error in downloadAndParseParquetFileAsDataRecords: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up temporary file
            if (tempFile.exists()) {
                boolean deleted = tempFile.delete();
                if (!deleted) {
                    System.err.println("Warning: Could not delete temporary file: " + tempFile.getAbsolutePath());
                }
            }
        }

        return results;
    }

    /**
     * Converts an Avro GenericRecord to a JSON object.
     *
     * @param record The Avro record to convert
     * @return A JSON representation of the record
     */
    private JSONObject convertRecordToJson(GenericRecord record) {
        JSONObject json = new JSONObject();

        if (record == null) {
            return json;
        }

        Schema schema = record.getSchema();
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Object value = record.get(fieldName);

            // Handle null values
            if (value == null) {
                json.put(fieldName, JSONObject.NULL);
                continue;
            }

            // Handle different Avro types
            switch (field.schema().getType()) {
                case RECORD:
                    // Recursively convert nested records
                    if (value instanceof GenericData.Record) {
                        json.put(fieldName, convertRecordToJson((GenericRecord) value));
                    } else {
                        json.put(fieldName, value.toString());
                    }
                    break;
                case ARRAY:
                    // Convert arrays to JSON arrays
                    if (value instanceof List) {
                        JSONArray array = new JSONArray();
                        for (Object item : (List<?>) value) {
                            if (item instanceof GenericRecord) {
                                array.put(convertRecordToJson((GenericRecord) item));
                            } else {
                                array.put(item);
                            }
                        }
                        json.put(fieldName, array);
                    } else {
                        json.put(fieldName, value);
                    }
                    break;
                case MAP:
                    // Convert maps to JSON objects
                    if (value instanceof java.util.Map) {
                        JSONObject mapJson = new JSONObject();
                        for (java.util.Map.Entry<?, ?> entry : ((java.util.Map<?, ?>) value).entrySet()) {
                            String key = entry.getKey().toString();
                            Object mapValue = entry.getValue();
                            if (mapValue instanceof GenericRecord) {
                                mapJson.put(key, convertRecordToJson((GenericRecord) mapValue));
                            } else {
                                mapJson.put(key, mapValue);
                            }
                        }
                        json.put(fieldName, mapJson);
                    } else {
                        json.put(fieldName, value);
                    }
                    break;
                default:
                    // For primitive types, just add the value
                    json.put(fieldName, value);
                    break;
            }
        }

        return json;
    }

    /**
     * Converts an Avro GenericRecord to a DataRecord object.
     *
     * @param record The Avro record to convert
     * @return A DataRecord representation of the record
     */
    private DataRecord convertRecordToDataRecord(GenericRecord record) {
        DataRecord dataRecord = new DataRecord();

        if (record == null) {
            return dataRecord;
        }

        Schema schema = record.getSchema();
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Object value = record.get(fieldName);

            // Convert the value based on type
            dataRecord.setField(fieldName, convertValue(value, field.schema().getType()));
        }

        return dataRecord;
    }

    /**
     * Converts an Avro value to an appropriate Java type based on the Avro schema type.
     *
     * @param value The value to convert
     * @param type  The Avro schema type
     * @return The converted value
     */
    private Object convertValue(Object value, Schema.Type type) {
        if (value == null) {
            return null;
        }

        switch (type) {
            case RECORD:
                if (value instanceof GenericData.Record) {
                    return convertRecordToDataRecord((GenericRecord) value);
                }
                return value.toString();
            case ARRAY:
                if (value instanceof List) {
                    List<Object> result = new ArrayList<>();
                    for (Object item : (List<?>) value) {
                        if (item instanceof GenericRecord) {
                            result.add(convertRecordToDataRecord((GenericRecord) item));
                        } else {
                            result.add(item);
                        }
                    }
                    return result;
                }
                return value;
            case MAP:
                if (value instanceof java.util.Map) {
                    Map<String, Object> result = new HashMap<>();
                    for (java.util.Map.Entry<?, ?> entry : ((java.util.Map<?, ?>) value).entrySet()) {
                        String key = entry.getKey().toString();
                        Object mapValue = entry.getValue();
                        if (mapValue instanceof GenericRecord) {
                            result.put(key, convertRecordToDataRecord((GenericRecord) mapValue));
                        } else {
                            result.put(key, mapValue);
                        }
                    }
                    return result;
                }
                return value;
            default:
                return value;
        }
    }

    /**
     * Main method to run the client.
     *
     * @param args Command-line arguments (not used)
     */
    public static void main(String[] args) {
        try {
            // Path to the profile file
            String profilePath = "/Users/emoransa/Documents/repositories/github/databricks-delta-sharing/config.share";

            // Target table - hardcoded for simplicity
            String targetShare = "share_kof_resumen_compras";
            String targetSchema = "mexico";
            String targetTable = "com_tbl_dlk_resumen_de_compras_mx";
            // Field to use as key for the map (replace with an actual field from your data)
            String keyField = "id"; // Adjust this based on your data schema

            System.out.println("Initializing Delta Sharing REST client");
            DeltaSharingParquetClient client = new DeltaSharingParquetClient(profilePath);

            // List shares
            System.out.println("Listing shares...");
            List<JSONObject> shares = client.listShares();
            System.out.println("Found " + shares.size() + " shares");
            for (JSONObject share : shares) {
                System.out.println(" - " + share.getString("name"));
            }

            // List schemas in the share
            System.out.println("Listing schemas in share: " + targetShare);
            List<JSONObject> schemas = client.listSchemas(targetShare);
            System.out.println("Found " + schemas.size() + " schemas");
            for (JSONObject schema : schemas) {
                System.out.println(" - " + schema.getString("name"));
            }

            // List tables in the schema
            System.out.println("Listing tables in schema: " + targetSchema);
            List<JSONObject> tables = client.listTables(targetShare, targetSchema);
            System.out.println("Found " + tables.size() + " tables");
            for (JSONObject table : tables) {
                System.out.println(" - " + table.getString("name"));
            }

            // Get table metadata
            System.out.println("Getting metadata for table: " + targetTable);
            JSONObject metadata = client.getTableMetadata(targetShare, targetSchema, targetTable);
            System.out.println("Successfully retrieved metadata");

            // Get data as JSONArray (original method)
            System.out.println("\n1. Querying table as JSONArray (up to " + MAX_ROWS + " rows): " + targetTable);
            JSONArray dataJson = client.queryTable(targetShare, targetSchema, targetTable, MAX_ROWS);
            System.out.println("Retrieved " + dataJson.length() + " records in JSONArray");

            // Print sample JSON data
            if (dataJson.length() > 0) {
                System.out.println("Sample record from JSONArray (first row):");
                System.out.println(dataJson.getJSONObject(0).toString(2));
            }

            // Save JSON to file (original functionality)
            System.out.println("Saving JSON data to: " + OUTPUT_JSON_FILE);
            try (FileWriter writer = new FileWriter(OUTPUT_JSON_FILE)) {
                writer.write(dataJson.toString(2));
            }

            // Get data as List of JSONObjects
            System.out.println("\n2. Querying table as List<JSONObject> (up to " + MAX_ROWS + " rows): " + targetTable);
            List<JSONObject> dataList = client.queryTableAsList(targetShare, targetSchema, targetTable, MAX_ROWS);
            System.out.println("Retrieved " + dataList.size() + " records in List");

            // Print sample list data
            if (!dataList.isEmpty()) {
                System.out.println("Sample record from List (first row):");
                System.out.println(dataList.get(0).toString(2));
            }

            // Get data as Map of JSONObjects
            System.out.println("\n3. Querying table as Map<String, JSONObject> with key field '" + keyField + "' (up to " + MAX_ROWS + " rows): " + targetTable);
            Map<String, JSONObject> dataMap = client.queryTableAsMap(targetShare, targetSchema, targetTable, keyField, MAX_ROWS);
            System.out.println("Retrieved " + dataMap.size() + " records in Map");

            // Print sample map data (first key)
            if (!dataMap.isEmpty()) {
                String firstKey = dataMap.keySet().iterator().next();
                System.out.println("Sample record from Map (key=" + firstKey + "):");
                System.out.println(dataMap.get(firstKey).toString(2));
            }

            // Get data as List of DataRecord objects (more structured approach)
            System.out.println("\n4. Querying table as List<DataRecord> (up to " + MAX_ROWS + " rows): " + targetTable);
            List<DataRecord> dataRecordList = client.queryTableAsDataRecordList(targetShare, targetSchema, targetTable, MAX_ROWS);
            System.out.println("Retrieved " + dataRecordList.size() + " records in DataRecord List");

            // Print sample DataRecord data
            if (!dataRecordList.isEmpty()) {
                System.out.println("Sample record from DataRecord List (first row):");
                System.out.println(dataRecordList.get(0).toString());

                // Print some fields from the first record
                DataRecord firstRecord = dataRecordList.get(0);
                System.out.println("Fields in first DataRecord:");
                for (String field : firstRecord.getAllFields().keySet()) {
                    System.out.println(" - " + field + ": " + firstRecord.getField(field));
                }
            }

            // Get data as Map of DataRecord objects
            System.out.println("\n5. Querying table as Map<String, DataRecord> with key field '" + keyField + "' (up to " + MAX_ROWS + " rows): " + targetTable);
            Map<String, DataRecord> dataRecordMap = client.queryTableAsDataRecordMap(targetShare, targetSchema, targetTable, keyField, MAX_ROWS);
            System.out.println("Retrieved " + dataRecordMap.size() + " records in DataRecord Map");

            // Print sample DataRecord map data (first key)
            if (!dataRecordMap.isEmpty()) {
                String firstKey = dataRecordMap.keySet().iterator().next();
                System.out.println("Sample record from DataRecord Map (key=" + firstKey + "):");
                System.out.println(dataRecordMap.get(firstKey).toString());
            }

            System.out.println("\nComplete! Data retrieved in multiple formats.");

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}