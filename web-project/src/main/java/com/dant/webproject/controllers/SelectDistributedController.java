package com.dant.webproject.controllers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.dant.webproject.dbcomponents.EqOperande;
import com.dant.webproject.dbcomponents.InfOperande;
import com.dant.webproject.dbcomponents.Operande;
import com.dant.webproject.dbcomponents.SupOperande;
import com.dant.webproject.services.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/selectDistributed")
public class SelectDistributedController {

    @Autowired
    private DistributedService distributedService;

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectDistributedController.class);

    @PostMapping("/select")
    public List<Map<String, Object>> select(@RequestParam String tableName, @RequestBody Map<String, Object> requestData) {
        List<String> colNames = (List<String>) (requestData.get("columns"));
        List<List<String>> conditions = (List<List<String>>) (requestData.get("where"));
        return distributedService.select(tableName, colNames, conditions);
    }

}