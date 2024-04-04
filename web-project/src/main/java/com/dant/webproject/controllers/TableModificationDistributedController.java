package com.dant.webproject.controllers;

import com.dant.webproject.services.DistributedService;
import com.dant.webproject.services.TableModificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/tablemodificationdistributed")
public class TableModificationDistributedController {

    @Autowired
    private DistributedService distributedService;

    private static final Logger LOGGER  = LoggerFactory.getLogger(TableModificationDistributedController.class);

    @PostMapping("/insert")
    public ResponseEntity<String> insert(@RequestParam String table, @RequestBody Map<String, Object> requestData) {
        List<String> col_name = (List<String>)(requestData.get("col_name"));
        List<List<String>> value = (List<List<String>>)(requestData.get("value"));
        LOGGER.info("Receiving request to insert values");
        distributedService.insertDistributed(table, col_name, value);
        return ResponseEntity.ok("Insertion realise !");
    }

    @PostMapping("/update_col")
    public void update_col(@RequestParam String tableName, @RequestParam String columnName, @RequestParam String newData, @RequestParam String conditionColumn, @RequestParam Object conditionValue) {
        distributedService.updateColumnDistributed(tableName,columnName,newData,conditionColumn,conditionValue);
    }

}