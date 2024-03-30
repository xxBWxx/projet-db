package com.dant.webproject.controllers;

import com.dant.webproject.services.DistributedService;
import com.dant.webproject.services.TableModificationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/tablemodificationdistributed")
public class TableModificationDistributedController {

    @Autowired
    private DistributedService distributedService;


    @PostMapping("/insert")
    public void insert(@RequestParam String table, @RequestBody Map<String, Object> requestData) {

        List<String> col_name = (List<String>)(requestData.get("col_name"));
        List<List<String>> value = (List<List<String>>)(requestData.get("value"));
        distributedService.insertDistributed(table, col_name, value);
    }

}