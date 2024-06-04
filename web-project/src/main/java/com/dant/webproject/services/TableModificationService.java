package com.dant.webproject.services;

import com.dant.webproject.dbcomponents.Column;
import com.dant.webproject.dbcomponents.DataType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class TableModificationService implements ISelectService {

    @Autowired
    private final DatabaseManagementService databaseManagementService;

    @Autowired
    public TableModificationService(DatabaseManagementService databaseManagementService) {
        this.databaseManagementService = databaseManagementService;
    }

    // Fonction intermediaire pour insert
    private void add(String tableName, String columnName, String value) {
        if (databaseManagementService.getDatabase().get(tableName) == null) {
            throw new IllegalArgumentException(
                    "Table not found : " + tableName);
        }

        Map<String, Column> table = databaseManagementService.getDatabase().get(tableName);

        if (table.get(columnName) == null) {
            throw new IllegalArgumentException(
                    "Column " + columnName + " not found in the table " + tableName);
        }

        Column column = table.get(columnName);

        column.addValue(value);
    }

    public void addColumn(String tableName, List<String> columns, List<DataType> type) {
        Map<String, Column> table = new LinkedHashMap<>();
        for (int i = 0; i < columns.size(); i++)
            table.put(columns.get(i), new Column(columns.get(i), type.get(i)));
        databaseManagementService.getDatabase().put(tableName, table);
    }

    public void insertMult(String table, List<String> col_name, List<List<String>> value) {
        for (int i = 0; i < value.size(); i++) {
            for (int j = 0; j < col_name.size(); j++) {
                add(table, col_name.get(j), value.get(i).get(j));
            }
        }

        Map<String, Column> mytable = databaseManagementService.getDatabase().get(table);

        for (String s : mytable.keySet()) {
            if (!col_name.contains(s))
                add(table, s, null);
        }
    }

    public void insert(String table, List<String> columns, List<String> values) {
        for (int i = 0; i < values.size(); i++) {
            add(table, columns.get(i), values.get(i));
        }
    }

    private int compareValues(Object value1, Object value2, DataType columnType) {
        if (columnType == DataType.INTEGER) {
            Integer intValue1 = Integer.parseInt(value1.toString());
            Integer intValue2 = Integer.parseInt(value2.toString());

            if (intValue1.intValue() == intValue2.intValue()) {
                return 0;
            }

            else if (intValue1.intValue() < intValue2.intValue()) {
                return -1;
            }

            else {
                return 1;
            }
        } else if (columnType == DataType.DOUBLE) {
            Double doubleValue1 = Double.parseDouble(value1.toString());
            Double doubleValue2 = Double.parseDouble(value2.toString());

            if (doubleValue1.doubleValue() == doubleValue2.doubleValue()) {
                return 0;
            }

            else if (doubleValue1.doubleValue() < doubleValue2.doubleValue()) {
                return -1;
            }

            else {
                return 1;
            }
        } else if (columnType == DataType.DATETIME_STRING) {
            return value1.toString().compareTo(value2.toString());
        } else {
            return ((String) value1).compareTo((String) value2);
        }
    }

    // mettre à jour une colonne donnée
    public ResponseEntity<String> updateColumn(String tableName, String columnName, String newData,
            String conditionColumn,
            Object conditionValue) {
        if (databaseManagementService.getDatabase().get(tableName) == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Table " + tableName + " does not exist in the database.");
        }

        Map<String, Column> table = databaseManagementService.getDatabase().get(tableName);
        if (!table.containsKey(columnName)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Column " + columnName + " does not exist.");
        }

        List<Object> columnData = table.get(columnName).getValues();
        DataType type = table.get(columnName).getType();
        List<Object> conditionColumnData = table.get(conditionColumn).getValues();

        // Parcourir les données de la colonne à mettre à jour
        for (int i = 0; i < columnData.size(); i++) {
            // Vérifier si la condition est satisfaite
            if (compareValues(conditionColumnData.get(i), conditionValue, table.get(conditionColumn).getType()) == 0) {
                Object castedData = castData(newData, type);
                columnData.set(i, castedData);
            }
        }

        return ResponseEntity.status(HttpStatus.OK)
                .body("Table " + tableName + " updated successfully.");
    }

    private Object castData(String newData, DataType type) {
        switch (type) {
            case STRING:
                return newData;
            case INTEGER:
                return Integer.parseInt(newData);
            case DOUBLE:
                return Double.parseDouble(newData);
            case DATETIME_STRING:
                return formatTimestamp(newData);
            // Ajoutez d'autres types de données si nécessaire
            default:
                throw new IllegalArgumentException("Type de donnée non supporté : " + type);
        }
    }

    private String formatTimestamp(String timestamp) {
        long microseconds = Long.parseLong(timestamp);
        long milliseconds = microseconds / 1000;

        Instant instant = Instant.ofEpochMilli(milliseconds);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        return formatter.format(instant.atZone(ZoneId.of("America/New_York")));
    }

    public ResponseEntity<String> deleteRow(String tableName, String conditionColumn, Object conditionValue) {
        if (databaseManagementService.getDatabase().get(tableName) == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Table " + tableName + " does not exist in the database.");
        }

        Map<String, Column> table = databaseManagementService.getDatabase().get(tableName);
        if (!table.containsKey(conditionColumn)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Column " + conditionColumn + " does not exist.");
        }

        List<Object> conditionColumnData = table.get(conditionColumn).getValues();
        DataType conditionColumnType = table.get(conditionColumn).getType();

        // Parcourir les données de la colonne à mettre à jour
        for (int i = conditionColumnData.size() - 1; i >= 0; i--) {
            // Vérifier si la condition est satisfaite
            if (compareValues(conditionColumnData.get(i), conditionValue, conditionColumnType) == 0) {
                // Supprimer la ligne i de chaque colonne
                for (Column column : table.values()) {
                    column.getValues().remove(i);
                }
            }
        }

        return ResponseEntity.status(HttpStatus.OK)
                .body("Deleted row successfully.");
    }

}
