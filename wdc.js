(function() {
    // Define the connector
    var myConnector = tableau.makeConnector();

    // Define the schema for the data
    myConnector.getSchema = function(schemaCallback) {
        var cols = [
            { id: "case_", dataType: tableau.dataTypeEnum.string },
            { id: "date_of_occurrence", dataType: tableau.dataTypeEnum.date },
            { id: "block", dataType: tableau.dataTypeEnum.string },
            { id: "iucr", dataType: tableau.dataTypeEnum.string },
            { id: "primary_description", dataType: tableau.dataTypeEnum.string },
            { id: "secondary_description", dataType: tableau.dataTypeEnum.string },
            { id: "location_description", dataType: tableau.dataTypeEnum.string },
            { id: "arrest", dataType: tableau.dataTypeEnum.string },
            { id: "domestic", dataType: tableau.dataTypeEnum.string },
            { id: "beat", dataType: tableau.dataTypeEnum.int },
            { id: "ward", dataType: tableau.dataTypeEnum.int },
            { id: "fbi_cd", dataType: tableau.dataTypeEnum.string },
            { id: "x_coordinate", dataType: tableau.dataTypeEnum.string },
            { id: "y_coordinate", dataType: tableau.dataTypeEnum.string },
            { id: "latitude", dataType: tableau.dataTypeEnum.float },
            { id: "longitude", dataType: tableau.dataTypeEnum.float },
            { id: "location", dataType: tableau.dataTypeEnum.string }
        ];

        var tableInfo = {
            id: "chicagoCrimeData",
            alias: "Chicago Crime Data",
            columns: cols
        };

        schemaCallback([tableInfo]);
    };

    // Fetch data from the API
    myConnector.getData = function(table, doneCallback) {
        var apiUrl = "https://data.cityofchicago.org/resource/x2n5-8w5q.csv";

        // Use PapaParse to fetch and parse the CSV data
        Papa.parse(apiUrl, {
            download: true,
            header: true,
            dynamicTyping: true,
            skipEmptyLines: true,
            complete: function(results) {
                var tableData = results.data.map(function(record) {
                    return {
                        case_: record.case_,
                        date_of_occurrence: record.date_of_occurrence,
                        block: record.block,
                        iucr: record.iucr,
                        primary_description: record.primary_description,
                        secondary_description: record.secondary_description,
                        location_description: record.location_description,
                        arrest: record.arrest,
                        domestic: record.domestic,
                        beat: record.beat,
                        ward: record.ward,
                        fbi_cd: record.fbi_cd,
                        x_coordinate: record.x_coordinate,
                        y_coordinate: record.y_coordinate,
                        latitude: record.latitude,
                        longitude: record.longitude,
                        location: record.location
                    };
                });

                // Append the data to the Tableau table
                table.appendRows(tableData);
                doneCallback();
            }
        });
    };

    // Required shutdown function for cleanup
    myConnector.shutdown = function() {
        // Perform any cleanup tasks if needed
        console.log("WDC is shutting down.");
    };

    tableau.registerConnector(myConnector);
})();

// Initialize the WDC once the page is ready
function initWDC() {
    tableau.connectionName = "Chicago Crime Data"; // Name the connection
    tableau.submit(); // Submit the connection to Tableau
}
