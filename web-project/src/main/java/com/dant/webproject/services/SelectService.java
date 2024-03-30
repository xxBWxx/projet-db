package com.dant.webproject.services;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class SelectService implements ISelectService{

    @Autowired
    private final DatabaseManagementService databaseManagementService;

    @Autowired
    public SelectService(DatabaseManagementService databaseManagementService){
        this.databaseManagementService=databaseManagementService;
    }

    public Map<String, List<String>> selectAll(String tableName) {
        return databaseManagementService.getDatabase().get(tableName);
    }

}
