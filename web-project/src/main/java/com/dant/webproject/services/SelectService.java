package com.dant.webproject.services;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dant.webproject.dbcomponents.DataType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


import com.dant.webproject.dbcomponents.Column;
import com.dant.webproject.dbcomponents.Operande;

@Component
public class SelectService implements ISelectService {

    @Autowired
    private final DatabaseManagementService databaseManagementService;

    @Autowired
    public SelectService(DatabaseManagementService databaseManagementService) {
        this.databaseManagementService = databaseManagementService;
    }

    public void validate_condition(String tableName, List<String> colNames, List<List<String>> conditions){
        Map<String, Map<String, Column>> database = databaseManagementService.getDatabase();
        Map<String, Column> table = database.get(tableName);

        if (table == null) {
            throw new IllegalArgumentException("La table " + tableName + " n'existe pas dans la base de données");
        }
        for (String columnName : colNames) {
            Column column = table.get(columnName);
            if (column == null) {
                throw new IllegalArgumentException("La colonne" + columnName + " n'existe pas dans la table");
            }
        }

        for (List<String> condition : conditions) {
            String columnName = condition.get(0);
            Column column = table.get(columnName);
            if (column == null) {
                throw new IllegalArgumentException("La colonne" + columnName + " n'existe pas dans la table");
            }

        }
    }

    public List<Map<String, Object>> select(String tableName, List<String> colNames, List<List<String>> conditions) {
        validate_condition(tableName,colNames,conditions);

        Map<String, Map<String, Column>> database = databaseManagementService.getDatabase();
        Map<String, Column> table = database.get(tableName);

        List<Map<String, Object>> rows = new ArrayList<>();

        Set<String> columnNames = table.keySet();
        int numRows = table.values().iterator().next().getValues().size();

        for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
            Map<String, Object> row = new LinkedHashMap<>();

            // Filtrer les colonnes si spécifiées
            if (colNames != null && !colNames.isEmpty()) {
                for (String columnName : colNames) {
                    Column column = table.get(columnName);
                    Object value = column.getValues().get(rowIndex);
                    row.put(columnName, value);
                }
            } else {
                // Sélectionner toutes les colonnes si aucune colonne spécifique n'est demandée
                for (String columnName : columnNames) {
                    Column column = table.get(columnName);
                    Object value = column.getValues().get(rowIndex);
                    row.put(columnName, value);
                }
            }

            // Vérifier les conditions si spécifiées
            if (conditions != null && !conditions.isEmpty()) {
                if (evaluateConditions(table,row, conditions)) {
                    rows.add(row);
                }
            } else {
                rows.add(row);
            }
        }

        return rows;
    }

    private boolean evaluateConditions(Map<String, Column> table,Map<String, Object> row, List<List<String>> conditions) {

        int compteur=0;
        for (List<String> condition : conditions) {
            String columnName = condition.get(0);
            String operator = condition.get(1);
            String operand = condition.get(2);

            Column column = table.get(columnName);
            Object value = row.get(columnName);

            switch (operator) {
                case "=":
                    if (compareValues(value,operand,column.getType()) == 0) {
                        compteur++;
                    }
                    break;
                case ">":
                    if (compareValues(value,operand,column.getType()) > 0) {
                        compteur++;
                    }
                    break;
                case "<":
                    if (compareValues(value,operand,column.getType()) < 0) {
                        compteur++;
                    }
                    break;
                // Ajoutez d'autres opérateurs au besoin
                default:
                    throw new IllegalArgumentException("Opérateur non pris en charge: " + operator);
            }
        }

        return compteur == conditions.size(); // Toutes les conditions sont satisfaites pour cette ligne
    }

    private int compareValues(Object value1, Object value2, DataType columnType) {
        if (columnType == DataType.INTEGER) {
            int intValue1 = Integer.parseInt(value1.toString());
            int intValue2 = Integer.parseInt(value2.toString());

            if (intValue1 == intValue2) {
                return 0;
            }

            return (intValue1 < intValue2) ? -1 : 1;
        } else if (columnType == DataType.DOUBLE) {
            double doubleValue1 = Double.parseDouble(value1.toString());
            double doubleValue2 = Double.parseDouble(value2.toString());

            if (doubleValue1 == doubleValue2) {
                return 0;
            }
            return (doubleValue1 < doubleValue2) ? -1 : 1;
        } else {
            return ((String) value1).compareTo((String) value2);
        }
    }

}
