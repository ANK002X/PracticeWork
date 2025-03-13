// Define the WDC class
var myConnector = tableau.makeConnector();

// Initialization of tableau WDC
tableau.init = function () {
    // Define any connection data or settings here
    tableau.connectionData = "Some connection data";  // Can be used for storing connection-specific data
    tableau.connectionName = "Example Connection";   // Set a name for the connection
    tableau.submit();  // Submit the connection
};

// Define the schema for the data
myConnector.getSchema = function (callback) {
    var schema = {
        id: "example_schema",
        alias: "Example Data",
        columns: [
            { id: "col1", dataType: tableau.dataTypeEnum.string },
            { id: "col2", dataType: tableau.dataTypeEnum.float }
        ]
    };
    callback([schema]);  // Pass the schema to Tableau
};

// Fetch the data for the WDC
myConnector.getData = function (table, doneCallback) {
    // Sample data (replace this with your actual data-fetching logic)
    var data = [
        { "col1": "Value 1", "col2": 123 },
        { "col1": "Value 2", "col2": 456 },
        { "col1": "Value 3", "col2": 789 }
    ];

    // Add rows to the table (WDC must map the data according to schema)
    for (var i = 0; i < data.length; i++) {
        table.appendRow(data[i]);
    }

    doneCallback();  // Finished adding data to the table
};

// Register the WDC with Tableau
tableau.registerConnector(myConnector);

// Function to handle the connection setup
function connect() {
    tableau.connectionName = "Example WDC";
    tableau.connectionData = "This is a sample data connection";
    
    tableau.submit();  // Start the connection to Tableau
}

// Add event listener to trigger the connection when the button is clicked
document.getElementById("submitButton").addEventListener('click', function () {
    connect();
});

// Handle page initialization
window.onload = function () {
    tableau.init();  // Initialize tableau when the page loads
};
