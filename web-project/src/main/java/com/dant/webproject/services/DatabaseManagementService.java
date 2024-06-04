package com.dant.webproject.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.dant.webproject.dbcomponents.Column;
import com.dant.webproject.dbcomponents.DataType;

@Component
public class DatabaseManagementService implements IDatabaseManagementService {

  private Map<String, Map<String, Column>> database = new HashMap<>();

  public void createTableCol(String tableName, List<String> columns, List<DataType> type) {
    if (database.get(tableName) != null) {
      throw new IllegalArgumentException(
          "La table " + tableName + " existe deja dans la base de donnees");
    }

    Map<String, Column> table = new LinkedHashMap<>();

    for (int i = 0; i < columns.size(); i++)
      table.put(columns.get(i), new Column(columns.get(i), type.get(i)));

    database.put(tableName, table);
  }

  @SuppressWarnings("rawtypes")
  public ResponseEntity createTable(String tableName) {
    if (database.get(tableName) != null) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body("Cannot create table " + tableName + " because it already exists in the database.");
    }

    database.put(tableName, new HashMap<String, Column>());
    return ResponseEntity.status(HttpStatus.CREATED)
        .body("Table " + tableName + " is successfully created.");
  }

  public Map<String, Map<String, Column>> getDatabase() {
    return database;
  }

  public List<String> showTables() {
    List<String> keysList = new ArrayList<>(database.keySet());
    return keysList;
  }

  public List<Map<String, Object>> describeTable(String tableName) {
    List<Map<String, Object>> res = new ArrayList<>();
    Map<String, Column> table = database.get(tableName);
    for (Map.Entry<String, Column> entry : table.entrySet()) {
      Map<String, Object> val = new LinkedHashMap<>();
      val.put("Field", entry.getKey());
      val.put("Type", entry.getValue().getType());
      res.add(val);
    }
    return res;
  }

  public ResponseEntity<String> alterTable(String tableName, String columnName, DataType typeData) {
    if (database.get(tableName) == null) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body("Table " + tableName + " does not exist in the database.");
    }

    Object value = null;
    switch (typeData) {
      case INTEGER:
        value = Integer.MIN_VALUE;
        break;
      case STRING:
        value = "-";
        break;
      case DOUBLE:
        value = Double.NaN;
        break;
      case DATETIME_STRING:
        value = "-";
        break;
      default:
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
            .body("Non valid data type: " + typeData);
    }

    Column col = new Column(columnName, typeData);
    int size = 0;
    for (Map.Entry<String, Column> entry : database.get(tableName).entrySet()) {
      size = entry.getValue().getValues().size();
      break;
    }

    List<Object> listCol = col.getValues();
    for (int i = 0; i < size; i++) {
      listCol.add(value);
    }
    database.get(tableName).put(columnName, col);

    return ResponseEntity.status(HttpStatus.OK)
        .body("Table " + tableName + " altered successfully.");
  }

}
