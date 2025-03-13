(function () {
    'use strict';

    // Wait for tableau to be initialized
    $(document).ready(function () {
        // Create the connector object
        var myConnector = tableau.makeConnector();

        // Init function for connector
        myConnector.init = function(initCallback) {
            console.log('Initializing WDC');
            
            // Set auth type to none
            tableau.authType = tableau.authTypeEnum.none;

            // Report progress for initialization phase
            if (tableau.phase === tableau.phaseEnum.gatherDataPhase) {
                tableau.reportProgress("Initializing data gathering...");
            }

            // Must call initCallback to tell connector initialization is done
            initCallback();
        };

        // Define the schema
        myConnector.getSchema = function(schemaCallback) {
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
            ];

            const tableSchema = {
                id: "crimeData",
                alias: "Chicago Crime Data",
                columns: cols,
                description: "Crime data from Chicago Police Department"
            };

            schemaCallback([tableSchema]);
        };

        // Download the data
        myConnector.getData = function(table, doneCallback) {
            const dataUrl = "https://data.cityofchicago.org/resource/x2n5-8w5q.json";
            const pageSize = 1000;
            let offset = 0;
            let hasMore = true;

            const processData = async () => {
                while (hasMore) {
                    try {
                        tableau.reportProgress(`Fetching records ${offset} to ${offset + pageSize}...`);

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
                            "arrest": Boolean(row.arrest),
                            "domestic": Boolean(row.domestic)
                        }));

                        table.appendRows(tableData);
                        offset += pageSize;

                        // Rate limiting
                        await new Promise(resolve => setTimeout(resolve, 100));

                    } catch (error) {
                        console.error("Error fetching data:", error);
                        tableau.abortWithError("Error fetching data: " + error.message);
                        return;
                    }
                }

                tableau.reportProgress("Data gathering complete!");
                doneCallback();
            };

            processData();
        };

        // Register the connector
        tableau.registerConnector(myConnector);
    });
})();
