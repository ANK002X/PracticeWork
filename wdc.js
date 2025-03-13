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

        // Schema definition
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
            ]; // Added closing bracket for cols array

            const tableSchema = {
                id: "crimeData",
                alias: "Chicago Crime Data",
                columns: cols,
                description: "Crime data from Chicago Police Department"
            };

            schemaCallback([tableSchema]); // Added schema callback
        };

        // ...existing getData function code...

        tableau.registerConnector(myConnector);
    });
})();
