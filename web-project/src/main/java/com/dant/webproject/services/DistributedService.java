package com.dant.webproject.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.util.*;

@Component
public class DistributedService {


    @Autowired
    private final SelectService selectService;
    @Autowired
    private final DatabaseManagementService databaseManagementService;
    @Autowired
    private final TableModificationService tableModificationService;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    public DistributedService(SelectService selectService, DatabaseManagementService databaseManagementService, TableModificationService tableModificationService){
        this.selectService=selectService;
        this.databaseManagementService=databaseManagementService;
        this.tableModificationService=tableModificationService;
    }


    public Map<String, List<String>> selectAllDistributed(String tableName) {

        Map<String, List<String>> value = new HashMap<>();
        selectService.selectAll(tableName).forEach((key, val) -> {
            if (!value.containsKey(key)) {
                value.put(key, new ArrayList<>());
            }
            value.get(key).addAll(val);
        });


        String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"} ;

        for (String serverUrl : serverUrls) {
            try {
                String url = serverUrl + "/select/selectallfrom?tableName=" + tableName;

                // Utilisation de ParameterizedTypeReference pour la désérialisation correcte
                Map<String, List<String>> result = restTemplate.exchange(url, HttpMethod.GET, null, new ParameterizedTypeReference<Map<String, List<String>>>() {}).getBody();
                result.forEach((key, val) -> value.get(key).addAll(val));

            } catch (Exception e) {
                e.printStackTrace(); // Handle exception or log it
            }
        }

        return value; // Return empty if not found anywhere
    }

    public void createTableDistributed(String tableName, List<String> columns){

        databaseManagementService.createTable(tableName, columns);
        String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"};

        // Headers pour la requête HTTP
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // Corps de la requête (les colonnes)
        HttpEntity<List<String>> requestEntity = new HttpEntity<>(columns, headers);

        // Appel du point de terminaison sur chaque serveur
        for (String serverUrl : serverUrls) {
            String createTableUrl = serverUrl + "/databasemanagement/create?tableName=" + tableName;
            restTemplate.exchange(createTableUrl, HttpMethod.POST, requestEntity, Void.class);
        }
    }


    public void insertDistributed(String table, String[] col_name, String[] value){
        String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"};

        if(col_name.length==1){
            tableModificationService.insert(table, col_name, value);
            return;
        }



    }



}
