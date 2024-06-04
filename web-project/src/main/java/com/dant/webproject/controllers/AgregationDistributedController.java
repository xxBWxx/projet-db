package com.dant.webproject.controllers;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dant.webproject.dbcomponents.AgregationType;
import com.dant.webproject.services.DistributedService;

@RestController
@RequestMapping("/agregationDistributedController")

public class AgregationDistributedController {

        @Autowired
        private DistributedService distributedService;

        @SuppressWarnings("unchecked")
        @GetMapping("/groupBy")
        public Object selectFrom(@RequestParam String tableName, @RequestParam String agregationType,
                        @RequestParam String colName, @RequestParam String groupByValues) {

                LinkedList<LinkedHashMap<String, Object>> outputList = new LinkedList<>();
                String col = agregationType + "(" + colName + ")";

                Map<Object, Object> res = (Map<Object, Object>) distributedService.agregationDistributed(
                                AgregationType.valueOf(agregationType), tableName, colName, groupByValues);
                for (Map.Entry<Object, Object> entry : res.entrySet()) {
                        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
                        map.put(groupByValues, entry.getKey());
                        map.put(col, entry.getValue());
                        outputList.add(map);
                }
                return outputList;
        }
}
