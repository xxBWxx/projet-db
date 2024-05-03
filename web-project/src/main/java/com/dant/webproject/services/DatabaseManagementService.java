package com.dant.webproject.services;

import java.util.*;

import com.dant.webproject.dbcomponents.Column;
import com.dant.webproject.dbcomponents.DataType;
import org.springframework.stereotype.Component;

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

}
