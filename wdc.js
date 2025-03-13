// Chicago Crime Data Web Data Connector for Tableau
// Requires Papa Parse library for CSV parsing

// Define the connector
tableau.registerConnector({
    init: function() {
        tableau.authType = tableau.authTypeEnum.none;
        
        // Handle different phases of the WDC lifecycle
        if (tableau.phase === tableau.phaseEnum.gatherDataPhase) {
            tableau.reportProgress("Initializing data gathering...");
        }
        
        if (tableau.phase === tableau.phaseEnum.interactivePhase) {
            // Add any UI initialization if needed
            console.log('WDC Interactive Phase Initialized');
        }
    },

    // Schema definition with proper data types and descriptions
    getSchema: function(schemaCallback) {
        var cols = [
            { 
                id: "case_",
                alias: "Case Number",
                dataType: tableau.dataTypeEnum.string,
                description: "Unique identifier for the incident"
            },
            { 
                id: "date_of_occurrence",
                alias: "Date of Occurrence",
                dataType: tableau.dataTypeEnum.datetime,
                description: "Date and time when the incident occurred",
                numberFormat: "yyyy-MM-dd'T'HH:mm:ss.SSS"
            },
            { 
                id: "block",
                alias: "Block",
                dataType: tableau.dataTypeEnum.string,
                description: "Block where the incident occurred"
            },
            { 
                id: "iucr",
                alias: "IUCR",
                dataType: tableau.dataTypeEnum.string,
                description: "Illinois Uniform Crime Reporting code"
            },
            { 
                id: "primary_description",
                alias: "Primary Description",
                dataType: tableau.dataTypeEnum.string,
                description: "Primary description of the crime type"
            },
            { 
                id: "secondary_description",
                alias: "Secondary Description",
                dataType: tableau.dataTypeEnum.string,
                description: "Secondary description of the crime type"
            },
            { 
                id: "location_description",
                alias: "Location Description",
                dataType: tableau.dataTypeEnum.string,
                description: "Description of the location"
            },
            { 
                id: "arrest",
                alias: "Arrest Made",
                dataType: tableau.dataTypeEnum.bool,
                description: "Whether an arrest was made"
            },
            { 
                id: "domestic",
                alias: "Domestic Violence",
                dataType: tableau.dataTypeEnum.bool,
                description: "Whether the incident was domestic-related"
            },
            { 
                id: "beat",
                alias: "Police Beat",
                dataType: tableau.dataTypeEnum.int,
                description: "Police beat where the incident occurred"
            },
            { 
                id: "ward",
                alias: "Ward",
                dataType: tableau.dataTypeEnum.int,
                description: "City ward where the incident occurred"
            },
            { 
                id: "fbi_cd",
                alias: "FBI Code",
                dataType: tableau.dataTypeEnum.string,
                description: "FBI crime classification code"
            },
            { 
                id: "x_coordinate",
                alias: "X Coordinate",
                dataType: tableau.dataTypeEnum.float,
                description: "X coordinate of the location"
            },
            { 
                id: "y_coordinate",
                alias: "Y Coordinate",
                dataType: tableau.dataTypeEnum.float,
                description: "Y coordinate of the location"
            },
            { 
                id: "latitude",
                alias: "Latitude",
                dataType: tableau.dataTypeEnum.float,
                description: "Latitude of the location"
            },
            { 
                id: "longitude",
                alias: "Longitude",
                dataType: tableau.dataTypeEnum.float,
                description: "Longitude of the location"
            },
            { 
                id: "location",
                alias: "Location",
                dataType: tableau.dataTypeEnum.string,
                description: "Combined location information"
            }
        ];

        var tableSchema = {
            id: "crime_data",
            alias: "Chicago Police Department Incident Data",
            columns: cols,
            description: "Crime incident reports from the Chicago Police Department"
        };

        schemaCallback([tableSchema]);
    },

    // Data gathering function with proper error handling and progress reporting
    getData: function(table, doneCallback) {
        const dataUrl = "https://data.cityofchicago.org/resource/x2n5-8w5q.json";
        const pageSize = 1000; // Adjust based on API limits
        let offset = 0;
        let hasMore = true;

        // Function to process each batch of data
        const processData = async () => {
            while (hasMore) {
                tableau.reportProgress(`Fetching records ${offset} to ${offset + pageSize}...`);
                
                try {
                    const response = await fetch(`${dataUrl}?$limit=${pageSize}&$offset=${offset}`);
                    
                    if (!response.ok) {
                        throw new Error(`HTTP error! status: ${response.status}`);
                    }

                    const data = await response.json();
                    
                    if (data.length === 0) {
                        hasMore = false;
                        break;
                    }

                    const tableData = data.map(row => ({
                        "case_": row.case_number || "",
                        "date_of_occurrence": row.date_of_occurrence || null,
                        "block": row.block || "",
                        "iucr": row.iucr || "",
                        "primary_description": row.primary_description || "",
                        "secondary_description": row.secondary_description || "",
                        "location_description": row.location_description || "",
                        "arrest": row.arrest === "true",
                        "domestic": row.domestic === "true",
                        "beat": parseInt(row.beat) || null,
                        "ward": parseInt(row.ward) || null,
                        "fbi_cd": row.fbi_cd || "",
                        "x_coordinate": parseFloat(row.x_coordinate) || null,
                        "y_coordinate": parseFloat(row.y_coordinate) || null,
                        "latitude": parseFloat(row.latitude) || null,
                        "longitude": parseFloat(row.longitude) || null,
                        "location": row.location || ""
                    }));

                    table.appendRows(tableData);
                    offset += pageSize;

                    // Add a small delay to prevent overwhelming the API
                    await new Promise(resolve => setTimeout(resolve, 100));

                } catch (error) {
                    tableau.abortWithError("Error fetching data: " + error.message);
                    return;
                }
            }

            tableau.reportProgress("Data gathering complete!");
            doneCallback();
        };

        processData();
    },

    // Shutdown function to clean up resources
    shutdown: function(shutdownCallback) {
        // Clean up any resources if needed
        shutdownCallback();
    }
});

// Initialize the connector when the page loads
function initWDC() {
    tableau.connectionName = "Chicago Police Data WDC";
    tableau.submit();
}
