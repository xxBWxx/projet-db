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

import java.util.*;

@RestController
@RequestMapping("/agregationDistributedController")

public class AgregationDistributedController {

        @Autowired
        private DistributedService distributedService;

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
