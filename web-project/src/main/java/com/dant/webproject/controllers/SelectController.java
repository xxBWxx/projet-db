package com.dant.webproject.controllers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dant.webproject.dbcomponents.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dant.webproject.services.SelectService;

@RestController
@RequestMapping("/select")
public class SelectController {

    @Autowired
    private SelectService selectService;



    @PostMapping("/select")
    public List<Map<String, Object>> select(@RequestParam String tableName, @RequestBody Map<String, Object> requestData) {
        List<String> colNames = (List<String>) (requestData.get("columns"));
        List<List<String>> conditions = (List<List<String>>) (requestData.get("where"));
        return selectService.select(tableName, colNames, conditions);
    }

}



