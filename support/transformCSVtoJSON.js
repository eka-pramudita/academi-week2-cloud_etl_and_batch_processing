function transformCSVtoJSON(line) {
    var values = line.split(',');

    var keyword = {};
    var properties = [
      'user_id',
      'search_keyword',
      'search_result_count',
      'created_at',
    ];

    if(values.length > 4){
      keyword[properties[0]] = values[0];
      src_key = ""
      for(var count = 1; count < values.length - 2; count++){
        src_key += values[count]
      }
      keyword[properties[1]] = src_key;
      keyword[properties[2]] = values[count];
      keyword[properties[3]] = values[count+1];
    }else{
      for (var count = 0; count < values.length; count++) {
        if (values[count] !== 'null') {
          keyword[properties[count]] = values[count];
        }
      }
    }
    if(values[2] != "search_result_count"){
      var jsonString = JSON.stringify(keyword);
      return jsonString;
    }
}