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

    public void createTableColDistributed(String tableName, List<String> columns){

        databaseManagementService.createTableCol(tableName, columns);
        String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"};

        // Headers pour la requête HTTP
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // Corps de la requête (les colonnes)
        HttpEntity<List<String>> requestEntity = new HttpEntity<>(columns, headers);

        // Appel du point de terminaison sur chaque serveur
        for (String serverUrl : serverUrls) {
            String createTableUrl = serverUrl + "/databasemanagement/createTableCol?tableName=" + tableName;
            restTemplate.exchange(createTableUrl, HttpMethod.POST, requestEntity, Void.class);
        }
    }

    public void createTableDistributed(String tableName){
        databaseManagementService.createTable(tableName);
        String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"};


        // Appel du point de terminaison sur chaque serveur
        for (String serverUrl : serverUrls) {
            String createTableUrl = serverUrl + "/databasemanagement/createTable?tableName=" + tableName;
            restTemplate.exchange(createTableUrl, HttpMethod.POST, null, Void.class);
        }
    }


    public void insertDistributed(String table, List<String> col_name, List<List<String>> value){
        int cpt=0;
        String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"};

        if(value.size()==1){
            tableModificationService.insert(table, col_name, value.get(1));
            return;
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        for (List<String> item : value) {
            if(cpt==0){
                tableModificationService.insert(table, col_name, item);
                cpt++;
                continue;
            }
            // Construire le corps de la requête
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("col_name", col_name);
            requestBody.put("value", item);

            HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);

            // Construire l'URL pour le point de terminaison d'insertion
            String insertUrl = serverUrls[cpt-1] + "/tablemodification/insert?table=" + table;
            cpt=(cpt+1)%3;
            // Effectuer la requête HTTP POST
            restTemplate.exchange(insertUrl, HttpMethod.POST, requestEntity, Void.class);
        }
    }

    //Select col from table Where condition


}
