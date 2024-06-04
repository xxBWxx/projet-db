package com.dant.webproject.services;

import java.util.*;

import com.dant.webproject.dbcomponents.Column;
import com.dant.webproject.dbcomponents.DataType;
import org.springframework.stereotype.Component;

import javax.xml.crypto.Data;

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

  public void createTable(String tableName) {
    if (database.get(tableName) != null) {
      throw new IllegalArgumentException(
          "La table " + tableName + " existe deja dans la base de donnees");
    }

    database.put(tableName, new HashMap<String, Column>());
  }

  public Map<String, Map<String, Column>> getDatabase() {
    return database;
  }

  public List<String> showTables() {
    List<String> keysList = new ArrayList<>(database.keySet());
    return keysList;
  }

  public List<Map<String, Object>> describeTable(String tableName){
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

  public void alterTable(String tableName, String columnName, DataType typeData){
    Column col = new Column(columnName, typeData);
    int size=0;
    for (Map.Entry<String, Column> entry : database.get(tableName).entrySet()) {
      size = entry.getValue().getValues().size();
      break;
    }

    Object value=null;
    switch (typeData) {
      case INTEGER:
        value=Integer.MIN_VALUE;
        break;
      case STRING:
        value="-";
        break;
      case DOUBLE:
        value=Double.NaN;
        break;
      case DATETIME_STRING:
        value="-";
        break;
      default:

    }

    List<Object> listCol = col.getValues();
    for(int i=0; i<size; i++){
      listCol.add(value);
    }
    database.get(tableName).put(columnName, col);
  }

}
