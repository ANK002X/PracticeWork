(function() {
    var myConnector = tableau.makeConnector();

    // Define the WDC schema
    myConnector.getSchema = function(schemaCallback) {
        var cols = [
            { id: "column1", dataType: tableau.dataTypeEnum.string },
            { id: "column2", dataType: tableau.dataTypeEnum.int }
        ];

        var tableSchema = {
            id: "exampleData",
            alias: "Example Data",
            columns: cols
        };

        schemaCallback([tableSchema]);
    };

    // Pull the data from an API or any other source
    myConnector.getData = function(table, doneCallback) {
        var data = [
            { "column1": "Value 1", "column2": 100 },
            { "column1": "Value 2", "column2": 200 }
        ];

        table.appendRows(data);
        doneCallback();
    };

    // Register the connector with Tableau
    tableau.registerConnector(myConnector);

    // Button click to open the Tableau WDC dialog
    document.getElementById("connect").addEventListener("click", function() {
        tableau.connectionName = "My Custom Connector";
        tableau.submit();
    });
})();
