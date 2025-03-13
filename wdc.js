// Chicago Crime Data Web Data Connector for Tableau

(function () {
    // Wait for tableau to be initialized
    $(document).ready(function () {
        var myConnector = tableau.makeConnector();

        myConnector.init = function(initCallback) {
            tableau.authType = tableau.authTypeEnum.none;
            
            if (tableau.phase === tableau.phaseEnum.gatherDataPhase) {
                tableau.reportProgress("Initializing data gathering...");
            }
            
            if (tableau.phase === tableau.phaseEnum.interactivePhase) {
                console.log('WDC Interactive Phase Initialized');
            }
            
            initCallback();
        };

        // Schema definition
        myConnector.getSchema = function(schemaCallback) {
            var cols = [
                { 
                    id: "case_",
                    alias: "Case Number",
                    dataType: tableau.dataTypeEnum.string,
                    description: "Unique identifier for the incident"
                },
                // ...existing schema columns...
            ];

            var tableSchema = {
                id: "crime_data",
                alias: "Chicago Police Department Incident Data",
                columns: cols,
                description: "Crime incident reports from the Chicago Police Department"
            };

            schemaCallback([tableSchema]);
        };

        // Data gathering function
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
                            // ...existing mapping code...
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
    });
})();

// Initialize the connector
window.initWDC = function() {
    tableau.connectionName = "Chicago Police Data WDC";
    tableau.submit();
};
