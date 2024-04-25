package com.dant.webproject.controllers;

import com.dant.webproject.dbcomponents.AgregationType;
import com.dant.webproject.dbcomponents.DataType;
import com.dant.webproject.services.AgregationService;
import com.dant.webproject.services.SelectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/agregation")
public class AgregationController {

    @Autowired
    private AgregationService agregationService;

    @GetMapping("/selectFrom")
    public Object selectFrom(@RequestParam String tableName, @RequestParam String agregationType,
            @RequestParam String colName, @RequestParam String groupByValues) {
        return agregationService.agregation(AgregationType.valueOf(agregationType), tableName, colName, groupByValues);
    }
}
