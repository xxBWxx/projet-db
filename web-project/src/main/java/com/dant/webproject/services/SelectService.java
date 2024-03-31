package com.dant.webproject.services;


import com.dant.webproject.dbcomponents.Column;
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

    public Map<String, List<Object>> selectAll(String tableName) {
        Map<String, List<Object>> res = new HashMap<>();
        for (Map.Entry<String, Column> entry : databaseManagementService.getDatabase().get(tableName).entrySet()) {
            res.put(entry.getKey(), entry.getValue().getValues());
        }
        return res;
    }

}
