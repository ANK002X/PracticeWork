(function () {
    'use strict';

    // Define the connector object
    var myConnector = tableau.makeConnector();

    // Initialize the connector
    myConnector.init = function(initCallback) {
        tableau.authType = tableau.authTypeEnum.none;
        
        // Set default connection name
        tableau.connectionName = "Chicago Crime Data";
        
        // Report progress for initialization
        if (tableau.phase === tableau.phaseEnum.gatherDataPhase) {
            tableau.reportProgress("Initializing data gathering...");
        }
        
        // Handle interactive phase if needed
        if (tableau.phase === tableau.phaseEnum.interactivePhase) {
            console.log('WDC Interactive Phase Initialized');
        }
        
        initCallback();
    };

    // Define the schema
    myConnector.getSchema = function(schemaCallback) {
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
                description: "The date the crime occurred"
            },
            { 
                id: "date_reported",
                alias: "Date Reported",
                dataType: tableau.dataTypeEnum.datetime,
                description: "The date the crime was reported"
            },
            { 
                id: "primary_type",
                alias: "Primary Crime Type",
                dataType: tableau.dataTypeEnum.string,
                description: "The primary crime type"
            },
            { 
                id: "description",
                alias: "Crime Description",
                dataType: tableau.dataTypeEnum.string,
                description: "Description of the crime"
            },
            { 
                id: "location_description",
                alias: "Location Description",
                dataType: tableau.dataTypeEnum.string,
                description: "Description of the crime location"
            },
            { 
                id: "arrest",
                alias: "Arrest",
                dataType: tableau.dataTypeEnum.bool,
                description: "Indicates if an arrest was made"
            },
            { 
                id: "domestic",
                alias: "Domestic",
                dataType: tableau.dataTypeEnum.bool,
                description: "Indicates if the crime was domestic"
            },
            // Add any other existing schema columns here...
        ];

        var tableSchema = {
            id: "crime_data",
            alias: "Chicago Police Department Incident Data",
            columns: cols,
            description: "Crime incident reports from the Chicago Police Department"
        };

        schemaCallback([tableSchema]);
    };

    // Define the data gathering function
    myConnector.getData = function(table, doneCallback) {
        const dataUrl = "https://data.cityofchicago.org/resource/x2n5-8w5q.json";
        const pageSize = 1000;
        let offset = 0;
        let hasMore = true;

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
                        "date_reported": row.date_reported || null,
                        "primary_type": row.primary_type || "",
                        "description": row.description || "",
                        "location_description": row.location_description || "",
                        "arrest": row.arrest === "true" || false,
                        "domestic": row.domestic === "true" || false,
                    }));

                    table.appendRows(tableData);
                    offset += pageSize;

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
    };

    // Shutdown function
    myConnector.shutdown = function(shutdownCallback) {
        shutdownCallback();
    };

    // Register the connector
    tableau.registerConnector(myConnector);
})();
