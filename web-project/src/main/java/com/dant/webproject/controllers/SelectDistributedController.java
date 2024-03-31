package com.dant.webproject.controllers;

import java.util.List;
import java.util.Map;

import com.dant.webproject.services.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/selectdistributed")
public class SelectDistributedController {

    @Autowired
    private DistributedService distributedService;

    private static final Logger LOGGER  = LoggerFactory.getLogger(SelectDistributedController.class);
    @GetMapping("/selectallfrom")
    public Map<String, List<String>> selectAll(@RequestParam String tableName) {
        LOGGER.info("Receiving select * from "+tableName+" request");
        return distributedService.selectAllDistributed(tableName);
    }

}