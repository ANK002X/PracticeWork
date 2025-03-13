(function () {
  var myConnector = tableau.makeConnector();
  myConnector.getData = function (request, doneCallback) {
    var apiUrl = 'https://data.cityofchicago.org/resource/x2n5-8w5q.csv?$order=id&$limit=10000&$offset=0';

    $.get(apiUrl, function (data) {
      var tableData = [];
      data.forEach(function (row) {
        tableData.push({
          id: row.id,
          name: row.name,
          // Add other fields as necessary
        });
      });
      request.respond(tableData);
      doneCallback();
    });
  };

  tableau.registerConnector(myConnector);
})();
