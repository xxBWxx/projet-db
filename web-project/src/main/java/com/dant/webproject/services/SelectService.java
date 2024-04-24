package com.dant.webproject.services;


import com.dant.webproject.dbcomponents.Column;
import com.dant.webproject.dbcomponents.Operande;
import com.dant.webproject.dbcomponents.Type;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class SelectService implements ISelectService{

    @Autowired
    private final DatabaseManagementService databaseManagementService;

    @Autowired
    public SelectService(DatabaseManagementService databaseManagementService){
        this.databaseManagementService=databaseManagementService;
    }

    public Map<String, List<Object>> selectAll(String tableName) {
        Map<String, List<Object>> res = new LinkedHashMap<>();
        for (Map.Entry<String, Column> entry : databaseManagementService.getDatabase().get(tableName).entrySet()) {
            res.put(entry.getKey(), entry.getValue().getValues());
        }
        return res;
    }

    public Map<String, List<Object>> select_cols(String tableName, List<String> col_names){
        // Récupérer les données de la table
        Map<String, Column> tableData = databaseManagementService.getDatabase().get(tableName);
        // Vérifier si la table existe dans la base de données
        if (tableData == null) {
            throw new IllegalArgumentException(
                    "Table non trouvée dans la base de données"
            );
        }

        for(String columnName : col_names){
            // Vérifier si la colonne existe dans les données de la table
            if (!tableData.containsKey(columnName)) {
                throw new IllegalArgumentException(
                        "La colonne " + columnName + " n'existe pas dans la table"
                );
            }
        }

        Map<String,List<Object>>results=new LinkedHashMap<>();

        for(String columnName : col_names){
            results.put(columnName,tableData.get(columnName).getValues());
        }

        return results;


    }

    public Map<String, List<Object>> selectWhere_eq(String table, String columnName, String value) {
        // Récupérer les données de la table
        Map<String, Column> tableData = databaseManagementService.getDatabase().get(table);
        // Vérifier si la table existe dans la base de données
        if (tableData == null) {
            throw new IllegalArgumentException(
                    "Table non trouvée dans la base de données"
            );
        }

        // Vérifier si la colonne existe dans les données de la table
        if (!tableData.containsKey(columnName)) {
            throw new IllegalArgumentException(
                    "La colonne " + columnName + " n'existe pas dans la table"
            );
        }

        Type columnType = tableData.get(columnName).getType();

        Object realValue;

        if (columnType == Type.INTEGER) realValue = Integer.parseInt(value);
        else realValue=value;

        // Filtrer les données en fonction de la condition
        List<Object> columnData = tableData.get(columnName).getValues();
        Map<String, List<Object>> filteredResult = new LinkedHashMap<>(); // Utilisation de LinkedHashMap pour préserver l'ordre des colonnes
        List<Integer> indices = new ArrayList<>();

        for (int i = 0; i < columnData.size(); i++) {
            Object cellValue = columnData.get(i);
            if (cellValue != null && cellValue.equals(realValue)) {
                indices.add(i);
            }
        }

        if(!indices.isEmpty()){
            // Ajout des colonnes filtrées dans le résultat
            for (Map.Entry<String, Column> entry : tableData.entrySet()) {
                String currentColumn = entry.getKey();
                List<Object> columnValues = entry.getValue().getValues();
                List<Object> filteredColumnData = new ArrayList<>();

                for (Integer ind : indices) {
                    filteredColumnData.add(columnValues.get(ind));
                }

                filteredResult.put(currentColumn, filteredColumnData);
            }
        }



        return filteredResult;
    }

    public Map<String, List<Object>> select_where(String table, List<Operande> listop) {
        Map<String, Column> tableData = databaseManagementService.getDatabase().get(table);
        if (tableData == null) {
            throw new IllegalArgumentException("Table non trouvée dans la base de données");
        }

        Map<String, List<Object>> filteredResult = new LinkedHashMap<>();


        List<List<Integer>> listind=new ArrayList<>();


        for (Operande op : listop) {
            List<Integer> l=new ArrayList<>();
            if (op.eval(tableData)) {
                l=op.getIndice();
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











}
