package com.dant.webproject.services;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    /*public Map<String, List<Object>> selectAll(String tableName) {
        Map<String, List<Object>> res = new LinkedHashMap<>();
        for (Map.Entry<String, Column> entry : databaseManagementService.getDatabase().get(tableName).entrySet()) {
            res.put(entry.getKey(), entry.getValue().getValues());
        }
        return res;
    }*/
    public List<Map<String, Object>> selectAll(String tableName) {
        Map<String, Map<String, Column>> database = databaseManagementService.getDatabase();
        Map<String, Column> table = database.get(tableName);

        if (table == null) {
            throw new IllegalArgumentException("La table " + tableName + " n'existe pas dans la base de données");
        }

        List<Map<String, Object>> rows = new ArrayList<>();

        // Récupérer les noms de colonnes
        Set<String> columnNames = table.keySet();

        // Récupérer les valeurs pour chaque colonne (toutes les colonnes ont le même nombre de valeurs)
        int numRows = table.values().iterator().next().getValues().size();

        for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
            Map<String, Object> row = new LinkedHashMap<>();

            for (String columnName : columnNames) {
                Column column = table.get(columnName);
                Object value = column.getValues().get(rowIndex);
                row.put(columnName, value);
            }

            rows.add(row);
        }

        return rows;
    }








    public Map<String, List<Object>> select_cols(String tableName, List<String> col_names) {
        // Récupérer les données de la table
        Map<String, Column> tableData = databaseManagementService.getDatabase().get(tableName);
        // Vérifier si la table existe dans la base de données
        if (tableData == null) {
            throw new IllegalArgumentException(
                    "Table non trouvée dans la base de données");
        }

        for (String columnName : col_names) {
            // Vérifier si la colonne existe dans les données de la table
            if (!tableData.containsKey(columnName)) {
                throw new IllegalArgumentException(
                        "La colonne " + columnName + " n'existe pas dans la table");
            }
        }

        Map<String, List<Object>> results = new LinkedHashMap<>();

        for (String columnName : col_names) {
            results.put(columnName, tableData.get(columnName).getValues());
        }

        return results;

    }

    public Map<String, List<Object>> select_where(String table, List<Operande> listop) {
        Map<String, Column> tableData = databaseManagementService.getDatabase().get(table);
        if (tableData == null) {
            throw new IllegalArgumentException("Table non trouvée dans la base de données");
        }

        Map<String, List<Object>> filteredResult = new LinkedHashMap<>();

        List<List<Integer>> listind = new ArrayList<>();

        for (Operande op : listop) {
            List<Integer> l = new ArrayList<>();
            if (op.eval(tableData)) {
                l = op.getIndice();
            }
            listind.add(l);
        }

        // Initialisation avec les éléments de la première sous-liste
        Set<Integer> indices = new HashSet<>(listind.get(0));

        // Intersection avec les éléments des sous-listes suivantes
        for (List<Integer> liste : listind) {
            indices.retainAll(liste);
        }

        for (Map.Entry<String, Column> entry : tableData.entrySet()) {
            String currentColumn = entry.getKey();
            List<Object> columnValues = entry.getValue().getValues();
            List<Object> filteredColumnData = new ArrayList<>();

            for (Integer ind : indices) {
                filteredColumnData.add(columnValues.get(ind));
            }

            filteredResult.put(currentColumn, filteredColumnData);
        }

        return filteredResult;
    }


    public List<Map<String, Object>> select_opti(String tableName, List<String> colNames, List<List<String>> conditions) {
        Map<String, Map<String, Column>> database = databaseManagementService.getDatabase();
        Map<String, Column> table = database.get(tableName);

        if (table == null) {
            throw new IllegalArgumentException("La table " + tableName + " n'existe pas dans la base de données");
        }

        List<Map<String, Object>> rows = new ArrayList<>();

        Set<String> columnNames = table.keySet();
        int numRows = table.values().iterator().next().getValues().size();

        for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
            Map<String, Object> row = new LinkedHashMap<>();

            // Filtrer les colonnes si spécifiées
            if (colNames != null && !colNames.isEmpty()) {
                for (String columnName : colNames) {
                    Column column = table.get(columnName);
                    if (column != null) {
                        Object value = column.getValues().get(rowIndex);
                        row.put(columnName, value);
                    }
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

        for (List<String> condition : conditions) {
            String columnName = condition.get(0);
            String operator = condition.get(1);
            String operand = condition.get(2);

            Column column = table.get(columnName);
            if (column == null) {
                throw new IllegalArgumentException("La colonne " + columnName + " n'existe pas dans la table");
            }

            Object value = row.get(columnName);
            if (value == null) {
                return false; // Si la valeur de la colonne est nulle, ne satisfait pas la condition
            }

            switch (operator) {
                case "=":
                    if (!value.toString().equals(operand)) {
                        return false;
                    }
                    break;
                case ">":
                    if (!(Double.parseDouble(value.toString()) > Double.parseDouble(operand))) {
                        return false;
                    }
                    break;
                case "<":
                    if (!(Double.parseDouble(value.toString()) < Double.parseDouble(operand))) {
                        return false;
                    }
                    break;
                // Ajoutez d'autres opérateurs au besoin
                default:
                    throw new IllegalArgumentException("Opérateur non pris en charge: " + operator);
            }
        }

        return true; // Toutes les conditions sont satisfaites pour cette ligne
    }

}
