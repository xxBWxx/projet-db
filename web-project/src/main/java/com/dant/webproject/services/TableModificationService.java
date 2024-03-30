package com.dant.webproject.services;

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

        Map<String, List<String>> table = databaseManagementService.getDatabase().get(tableName);

        if (table.get(columnName) == null) {
            throw new IllegalArgumentException(
                    "La colonne " + columnName + " n'existe pas dans la tbl"
            );
        }

        List<String> column = table.get(columnName);

        column.add(data);
    }


    public void insert(String table, String[] col_name, String[] value) {
        for (int i = 0; i < col_name.length; i++) {
            add(table, col_name[i], value[i]);
        }
    }

}
