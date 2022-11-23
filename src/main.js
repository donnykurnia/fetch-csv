
// global variables
var cc = DataStudioApp.createCommunityConnector();
var csv_data = undefined;

function sendUserError(message) {
  cc.newUserError().setText(message).throwException();
}

// https://developers.google.com/datastudio/connector/reference#getauthtype
function getAuthType() {
  var AuthTypes = cc.AuthType;
  return cc
    .newAuthTypeResponse()
    .setAuthType(AuthTypes.NONE)
    .build();
}

function getConfig(request) {
  var connectorConfig = cc.getConfig();
  
  connectorConfig.setDateRangeRequired(false);
    
  connectorConfig.newTextInput()
    .setId('url')
    .setName('Enter the URL of your CSV')
    .setHelpText('e.g. https://api.site.com/api/data.json')
    .setPlaceholder('https://');
  
  connectorConfig.newSelectSingle()
    .setId('delimiter')
    .setName('Select the delimiter between each value')
    .setAllowOverride(false)
    .addOption(connectorConfig.newOptionBuilder().setLabel('Comma').setValue(','))
    .addOption(connectorConfig.newOptionBuilder().setLabel('Semicolon').setValue(';'))
    .addOption(connectorConfig.newOptionBuilder().setLabel('Tabulation').setValue('\t'));
  
  connectorConfig.newSelectSingle()
    .setId('containsHeader')
    .setName('Does your CSV have a header row?')
    .setAllowOverride(false)
    .addOption(connectorConfig.newOptionBuilder().setLabel('True').setValue('true'))
    .addOption(connectorConfig.newOptionBuilder().setLabel('False').setValue('false'));
    
  return connectorConfig.build();
}

/* this method return a two-dimensional array containing the values in the CSV string */
function getCSVData(request, delimiter) {
  if (!request.configParams.url || !request.configParams.url.match(/^https?:\/\/.+$/g)) {
    sendUserError("Input error: Invalid URL");
  }
  var params = {
    headers: JSON.parse(request.configParams.http_headers)
  };
  var response = UrlFetchApp.fetch(request.configParams.url, params);
  var content = response.getContentText();
  if (!content) {
    sendUserError("Error during parsing content: Empty content");
  }
  return Utilities.parseCsv(content, delimiter);
}

function getFields(request) {
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  var containsHeader = request.configParams.containsHeader === 'true';
  var dataFields = [];

  if ( csv_data === undefined ) {
    csv_data = getCSVData(request, request.configParams.delimiter);
  }

  var firstLineColumns = csv_data[0];
  var i = 1;
  firstLineColumns.forEach(function(value) {
    var key;
    if (containsHeader) {
      key = value.replace(/\s/g, '_');
      dataFields.push({ key: key, type: "string", name: value});
    } else {
      key = 'column_' + i;
      dataFields.push({ key: key, type: "string"});
      i++;
    }
  });

  //first line of data, the only possible place to set number type
  var firstLineData = csv_data[0];
  if (containsHeader) {
    firstLineData = csv_data[1];
  }
  firstLineData.forEach(function(value, index) {
    if ( !isNaN(value) ) {
      dataFields[index].type = "number";
    }
  });
  //the rest of data, to turn number back to string if the data is not numeric
  var slice = 1;
  if (containsHeader) {
    slice = 2;
  }
  csv_data.slice(slice).forEach(function(row) {
    row.forEach(function(value, index) {
      if ( dataFields[index].type === "number" && isNaN(value) ) {
        dataFields[index].type = "string";
      }
    });
  });

  dataFields.forEach(function(dataField) {
    var key = dataField.key;
    var field;
    if ( dataField.type === "number" ) {
      field = fields.newMetric().setId(key).setType(types.NUMBER).setAggregation(aggregations.SUM);
    } else {
      field = fields.newDimension().setId(key).setType(types.TEXT);
    }
    if ( dataField.name !== undefined ) {
      field.setName(dataField.name);
    }
  });
  return fields;
}

function getSchema(request) {
  var fields = getFields(request).build();
  return { schema: fields };
}

function getData(request) {
  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  var fields = getFields(request);
  var requestedFields = fields.forIds(requestedFieldIds);

  if ( csv_data === undefined ) {
    csv_data = getCSVData(request, request.configParams.delimiter);
  }

  var buildedFields = fields.build();
  var fieldNameIndex = buildedFields.reduce(function(result, field, index) {
    result[field.name] = index;
    return result;
  }, {});

  var requestedFieldsIndex = requestedFieldIds.map(function(fieldId) {
    return fieldNameIndex[fieldId];
  });

  var rows = csv_data.map(function(row) {
    if (buildedFields.length !== row.length) {
      sendUserError("Error during parsing content: the number of columns on each row is not respect");
    }
    var requestedValues = [];
    requestedFieldsIndex.forEach(function(colIndex) {
      requestedValues.push(row[colIndex]);
    });
    return { values: requestedValues };
  });
  if (request.configParams.containsHeader === 'true') {
    rows = rows.slice(1);
  }

  return {
    schema: requestedFields.build(),
    rows: rows
  };
}
