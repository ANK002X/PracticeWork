// Define the connector
tableau.registerConnector({
    init: function() {
        console.log('WDC Initialized');
    },

    // Function to define the schema of the data
    getSchema: function(schemaCallback) {
        var cols = [
            { id: "case_", dataType: tableau.dataTypeEnum.string },
            { id: "date_of_occurrence", dataType: tableau.dataTypeEnum.datetime },
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

        var tableSchema = {
            id: "crime_data",
            alias: "Chicago Police Department Incident Data",
            columns: cols
        };

        schemaCallback([tableSchema]);
    },

    // Function to fetch the data from the API
    getData: function(table, doneCallback) {
        var dataUrl = "https://data.cityofchicago.org/resource/x2n5-8w5q.csv";

        // Fetch data from the CSV API endpoint
        fetch(dataUrl)
            .then(response => response.text())
            .then(csvText => {
                // Parse the CSV to JSON
                Papa.parse(csvText, {
                    header: true,
                    dynamicTyping: true,
                    complete: function(results) {
                        var data = results.data;
                        var tableData = data.map(function(row) {
                            return {
                                "case_": row.case_,
                                "date_of_occurrence": row.date_of_occurrence,
                                "block": row.block,
                                "iucr": row.iucr,
                                "primary_description": row.primary_description,
                                "secondary_description": row.secondary_description,
                                "location_description": row.location_description,
                                "arrest": row.arrest,
                                "domestic": row.domestic,
                                "beat": row.beat,
                                "ward": row.ward,
                                "fbi_cd": row.fbi_cd,
                                "x_coordinate": row.x_coordinate,
                                "y_coordinate": row.y_coordinate,
                                "latitude": row.latitude,
                                "longitude": row.longitude,
                                "location": row.location
                            };
                        });

                        // Push data into the Tableau table
                        table.appendRows(tableData);
                        doneCallback();
                    },
                    error: function(error) {
                        console.error("Error parsing CSV: ", error.message);
                        doneCallback();
                    }
                });
            })
            .catch(function(error) {
                console.error("Error fetching data: ", error);
                doneCallback();
            });
    }
});

// Initializing the connector
function initWDC() {
    tableau.connectionName = "Chicago Police Data WDC";
    tableau.submit();
}
