function transformCSVtoJSON(line) {
  var values = line.split(',');
  var properties = [
    'user_id',
    'search_keyword',
    'search_result_count',
    'created_at',
  ];
  var keyword = {};

  for (var count = 0; count < values.length; count++) {
    if (values[count] !== 'null') {
      keyword[properties[count]] = values[count];
    }
  }

  var jsonString = JSON.stringify(keyword);
  return jsonString;
}