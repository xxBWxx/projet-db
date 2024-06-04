package com.dant.webproject.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dant.webproject.dbcomponents.AgregationType;
import com.dant.webproject.services.AgregationService;

@RestController
@RequestMapping("/agregation")
public class AgregationController {

    @Autowired
    private AgregationService agregationService;

    @GetMapping("/groupBy")
    public Object selectFrom(@RequestParam String tableName, @RequestParam String agregationType,
            @RequestParam String colName, @RequestParam String groupByValues) {
        return agregationService.agregation(AgregationType.valueOf(agregationType), tableName, colName, groupByValues);
    }
}
