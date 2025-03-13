(function () {
    'use strict';

    $(document).ready(function () {
        var myConnector = tableau.makeConnector();

        myConnector.init = function(initCallback) {
            console.log('Initializing WDC');
            tableau.authType = tableau.authTypeEnum.none;

            if (tableau.phase === tableau.phaseEnum.gatherDataPhase) {
                tableau.reportProgress("Initializing data gathering...");
            }

            initCallback();
        };

        // Schema remains the same
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
        };

        // Modified getData function with date filtering and smaller page size
        myConnector.getData = function(table, doneCallback) {
            // Reduce page size to prevent timeout
            const pageSize = 500;
            let offset = 0;
            let hasMore = true;

            // Create date filter for last 30 days
            const endDate = new Date();
            const startDate = new Date();
            startDate.setDate(startDate.getDate() - 30);

            // Format dates for SODA API
            const startDateStr = startDate.toISOString().slice(0, 10);
            const endDateStr = endDate.toISOString().slice(0, 10);

            // Base URL with date filter
            const baseUrl = "https://data.cityofchicago.org/resource/x2n5-8w5q.json";
            const dateFilter = `date_of_occurrence between '${startDateStr}' and '${endDateStr}'`;

            const processData = async () => {
                while (hasMore) {
                    try {
                        tableau.reportProgress(`Fetching records ${offset} to ${offset + pageSize}...`);

                        // Add date filter and order by date
                        const url = `${baseUrl}?$limit=${pageSize}&$offset=${offset}&$where=${encodeURIComponent(dateFilter)}&$order=date_of_occurrence DESC`;
                        const response = await fetch(url);
                        
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

                        // Increased delay between requests
                        await new Promise(resolve => setTimeout(resolve, 250));

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

        tableau.registerConnector(myConnector);
    });
})();
