package com.dant.webproject.controllers;

import com.dant.webproject.dbcomponents.AgregationType;
import com.dant.webproject.dbcomponents.DataType;
import com.dant.webproject.services.DistributedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/agregationDistributedController")

public class AgregationDistributedController {

        @Autowired
        private DistributedService distributedService;

        @GetMapping("/selectFrom")
        public Object selectFrom(@RequestParam String tableName, @RequestParam String agregationType, @RequestParam String colName, @RequestParam String groupByValues) {
            return distributedService.agregationDistributed(AgregationType.valueOf(agregationType), tableName, colName, groupByValues);
        }
}
