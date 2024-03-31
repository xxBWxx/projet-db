package com.dant.webproject.services;

import com.dant.webproject.dbcomponents.Column;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class TableModificationService implements ISelectService{

    @Autowired
    private final DatabaseManagementService databaseManagementService;

    @Autowired
    public TableModificationService(DatabaseManagementService databaseManagementService){
        this.databaseManagementService=databaseManagementService;
    }

    //Fonction intermediaire pour insert
     private void add(String tableName, String columnName, String data) {
        if (databaseManagementService.getDatabase().get(tableName) == null) {
            throw new IllegalArgumentException(
                    "La table " + tableName + " n'existe pas dans la base de donnees"
            );
        }

        Map<String, Column> table = databaseManagementService.getDatabase().get(tableName);

        if (table.get(columnName) == null) {
            throw new IllegalArgumentException(
                    "La colonne " + columnName + " n'existe pas dans la tbl"
            );
        }

        Column column = table.get(columnName);

        column.addValue(data);
    }


    public void insertMult(String table, List<String> col_name, List<List<String>> value) {
        for (int i = 0; i < value.size(); i++) {
            for (int j = 0; j < col_name.size(); j++) {
                add(table, col_name.get(j), value.get(i).get(j));
            }
        }

        Map<String, Column> mytable = databaseManagementService.getDatabase().get(table);

        for(String s: mytable.keySet()){
            if(!col_name.contains(s))add(table,s,null);
        }
    }

    public void insert(String table, List<String> col_name, List<String> value){
        for (int i = 0; i < value.size(); i++) {
            add(table, col_name.get(i), value.get(i));
        }

        Map<String, Column> mytable = databaseManagementService.getDatabase().get(table);

        for(String s: mytable.keySet()){
            if(!col_name.contains(s))add(table,s,null);
        }
    }

    //mettre à jour une colonne donnée
    public void updateColumn(String tableName, String columnName, List<String> newData) {
        if (databaseManagementService.getDatabase().get(tableName) == null) {
            throw new IllegalArgumentException(
                    "La table " + tableName + " n'existe pas dans la base de donnees"
            );
        }

        Map<String, Column> table = databaseManagementService.getDatabase().get(tableName);
        if (!table.containsKey(columnName)) {
            throw new IllegalArgumentException(
                    "La colonne " + columnName + " n'existe pas dans la table " + tableName
            );
        }

        Column new_col=new Column(columnName,table.get(columnName).getType());

        for(String s : newData)new_col.addValue(s);

        // Mettre à jour les données de la colonne
        table.put(columnName, new_col);
    }

}
