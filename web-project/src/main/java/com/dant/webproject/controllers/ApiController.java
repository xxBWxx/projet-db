package com.dant.webproject.controllers;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dant.webproject.services.DatabaseService;

@RestController
@RequestMapping("/api")
public class ApiController {

    @Autowired
    private DatabaseService databaseService;

    @GetMapping("/select")
    public ResponseEntity<?> selectData(@RequestParam("table") String table,
            @RequestParam("columns") List<String> columns /*,
            @RequestParam("conditions") Map<String, String> conditions*/) {
        try {
            List<Map<String, Object>> results = databaseService.select(table, columns);
            return ResponseEntity.ok(results);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Erreur lors de l'exécution de la commande SELECT.");
        }
    }
}
