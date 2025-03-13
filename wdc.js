(function () {
    'use strict';

    // Wait for tableau to be initialized
    $(document).ready(function () {
        var myConnector = tableau.makeConnector();

        // Init function for connector
        myConnector.init = function(initCallback) {
            try {
                console.log('Initializing WDC');
                tableau.authType = tableau.authTypeEnum.none;

                // Set connection name here instead of in the click handler
                tableau.connectionName = "Chicago Crime Data";

                if (tableau.phase === tableau.phaseEnum.gatherDataPhase) {
                    tableau.reportProgress("Initializing data gathering...");
                }

                initCallback();
            } catch (error) {
                console.error("Error in init:", error);
                tableau.abortWithError("Initialization failed: " + error.message);
            }
        };

        // Schema definition with error handling
        myConnector.getSchema = function(schemaCallback) {
            try {
            const cols = [
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
                        description: "Date and time when the incident occurred"
                    },
                    { 
                        id: "date_reported",
                        alias: "Date Reported",
                        dataType: tableau.dataTypeEnum.datetime,
                        description: "Date when the incident was reported"
                    },
                    { 
                        id: "primary_type",
                        alias: "Primary Crime Type",
                        dataType: tableau.dataTypeEnum.string,
                        description: "The primary type of crime"
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
                        description: "Description of where the crime occurred"
                    },
                    { 
                        id: "arrest",
                        alias: "Arrest",
                        dataType: tableau.dataTypeEnum.bool,
                        description: "Indicates whether an arrest was made"
                    },
                    { 
                        id: "domestic",
                        alias: "Domestic",
                        dataType: tableau.dataTypeEnum.bool,
                        description: "Indicates if the incident was domestic"
                    }
                ]; // Added closing bracket for cols array

                const tableSchema = {
                    id: "crimeData",
                    alias: "Chicago Crime Data",
                    columns: cols,
                    description: "Crime data from Chicago Police Department"
                };

                schemaCallback([tableSchema]);
            } catch (error) {
                console.error("Error in getSchema:", error);
                tableau.abortWithError("Schema definition failed: " + error.message);
            }
        };

        // Modified getData function with better error handling
        myConnector.getData = function(table, doneCallback) {
            const pageSize = 500;
            let offset = 0;
            let hasMore = true;

            // Get date range from the UI or use defaults
            const endDate = new Date();
            const startDate = new Date();
            startDate.setDate(startDate.getDate() - 30);

            const startDateStr = startDate.toISOString().slice(0, 10);
            const endDateStr = endDate.toISOString().slice(0, 10);

            const baseUrl = "https://data.cityofchicago.org/resource/x2n5-8w5q.json";
            const dateFilter = `date_of_occurrence between '${startDateStr}' and '${endDateStr}'`;

            const processData = async () => {
                try {
                    while (hasMore) {
                        tableau.reportProgress(`Fetching records ${offset} to ${offset + pageSize}...`);

                        const url = `${baseUrl}?$limit=${pageSize}&$offset=${offset}&$where=${encodeURIComponent(dateFilter)}&$order=date_of_occurrence DESC`;
                        const response = await fetch(url);
                        
                        if (!response.ok) {
                            throw new Error(`HTTP error! status: ${response.status}`);
                        }

                        const data = await response.json();
                        
                        if (!data || data.length === 0) {
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
                            "arrest": Boolean(row.arrest),
                            "domestic": Boolean(row.domestic)
                        }));

                        table.appendRows(tableData);
                        offset += pageSize;

                        await new Promise(resolve => setTimeout(resolve, 250));
                    }

                    tableau.reportProgress("Data gathering complete!");
                    doneCallback();
                } catch (error) {
                    console.error("Error in getData:", error);
                    tableau.abortWithError("Data gathering failed: " + error.message);
                }
            };

            processData();
        };

        // Register the connector
        tableau.registerConnector(myConnector);
    });
})();
