package com.dant.webproject.services;

import com.dant.webproject.dbcomponents.Type;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.client.RestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.util.*;
import java.util.stream.Collectors;

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


    public void createTableColDistributed(String tableName, List<String> columns, List<String> type){

        List<Type> typeList = type.stream().map(Type::valueOf).collect(Collectors.toList());

        databaseManagementService.createTableCol(tableName, columns, typeList);
        String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"};

        // Headers pour la requête HTTP
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // Corps de la requête (les colonnes)

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("col_name", columns);
        requestBody.put("type", type);
        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);

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

    public Map<String, List<Object>> selectAllDistributed(String tableName) {

        Map<String, List<Object>> value = new HashMap<>();
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
                Map<String, List<Object>> result = restTemplate.exchange(url, HttpMethod.GET, null, new ParameterizedTypeReference<Map<String, List<Object>>>() {}).getBody();
                result.forEach((key, val) -> value.get(key).addAll(val));

            } catch (Exception e) {
                e.printStackTrace(); // Handle exception or log it
            }
        }

        return value; // Return empty if not found anywhere
    }

    public Map<String, List<Object>> select_colsDistributed(String tableName, List<String> col_names){
        String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"} ;

        Map<String, List<Object>> value = new HashMap<>();
        selectService.select_cols(tableName, col_names).forEach((key, val) -> {
            if (!value.containsKey(key)) {
                value.put(key, new ArrayList<>());
            }
            value.get(key).addAll(val);
        });


        for (String serverUrl : serverUrls) {
            try {
                String url = serverUrl + "/select/selectcols?tableName=" + tableName;

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);

                HttpEntity<List<String>> requestEntity = new HttpEntity<>(col_names, headers);

                // Utilisation de ParameterizedTypeReference pour la désérialisation correcte
                Map<String, List<Object>> result = restTemplate.exchange(url, HttpMethod.POST, requestEntity, new ParameterizedTypeReference<Map<String, List<Object>>>() {}).getBody();
                result.forEach((key, val) -> value.get(key).addAll(val));

            } catch (Exception e) {
                e.printStackTrace(); // Handle exception or log it
            }
        }

        return value; // Return empty if not found anywhere
    }

    public Map<String, List<Object>> selectWhere_eqDistributed(String tableName, String colName, String val) {

        Map<String, List<Object>> value = new HashMap<>();
        selectService.selectWhere_eq(tableName, colName, val).forEach((key, v) -> {
            if (!value.containsKey(key)) {
                value.put(key, new ArrayList<>());
            }
            value.get(key).addAll(v);
        });


        String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"} ;

        for (String serverUrl : serverUrls) {
            try {
                String url = serverUrl + "/select/select_where_eq_from?tableName=" + tableName +"&colName="+colName+"&val="+val;

                // Utilisation de ParameterizedTypeReference pour la désérialisation correcte
                Map<String, List<Object>> result = restTemplate.exchange(url, HttpMethod.GET, null, new ParameterizedTypeReference<Map<String, List<Object>>>() {}).getBody();
                result.forEach((key, v) -> value.get(key).addAll(v));

            } catch (Exception e) {
                e.printStackTrace(); // Handle exception or log it
            }
        }

        return value; // Return empty if not found anywhere
    }

    public Map<String, List<Object>> selectWhere_supDistributed(@RequestParam String tableName, @RequestParam String colName,@RequestParam String val) {
        Map<String, List<Object>> value = new HashMap<>();
        selectService.selectWhere_sup(tableName, colName, val).forEach((key, v) -> {
            if (!value.containsKey(key)) {
                value.put(key, new ArrayList<>());
            }
            value.get(key).addAll(v);
        });


        String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"} ;

        for (String serverUrl : serverUrls) {
            try {
                String url = serverUrl + "/select/select_where_sup_from?tableName=" + tableName +"&colName="+colName+"&val="+val;

                // Utilisation de ParameterizedTypeReference pour la désérialisation correcte
                Map<String, List<Object>> result = restTemplate.exchange(url, HttpMethod.GET, null, new ParameterizedTypeReference<Map<String, List<Object>>>() {}).getBody();
                result.forEach((key, v) -> value.get(key).addAll(v));

            } catch (Exception e) {
                e.printStackTrace(); // Handle exception or log it
            }
        }

        return value; // Return empty if not found anywhere
    }

    public Map<String, List<Object>> selectWhere_infDistributed(@RequestParam String tableName, @RequestParam String colName,@RequestParam String val) {
        Map<String, List<Object>> value = new HashMap<>();
        selectService.selectWhere_inf(tableName, colName, val).forEach((key, v) -> {
            if (!value.containsKey(key)) {
                value.put(key, new ArrayList<>());
            }
            value.get(key).addAll(v);
        });


        String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"} ;

        for (String serverUrl : serverUrls) {
            try {
                String url = serverUrl + "/select/select_where_inf_from?tableName=" + tableName +"&colName="+colName+"&val="+val;

                // Utilisation de ParameterizedTypeReference pour la désérialisation correcte
                Map<String, List<Object>> result = restTemplate.exchange(url, HttpMethod.GET, null, new ParameterizedTypeReference<Map<String, List<Object>>>() {}).getBody();
                result.forEach((key, v) -> value.get(key).addAll(v));

            } catch (Exception e) {
                e.printStackTrace(); // Handle exception or log it
            }
        }

        return value; // Return empty if not found anywhere
    }

    public void updateColumnDistributed(String tableName, String columnName, String newData, String conditionColumn, Object conditionValue){
        String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"} ;
        tableModificationService.updateColumn(tableName, columnName, newData, conditionColumn, conditionValue);
        for (String serverUrl : serverUrls) {
            try {
                String url = serverUrl + "/tablemodification/update_col?tableName=" + tableName
                        +"&columnName="+columnName
                        +"&newData="+newData
                        +"&conditionColumn="+conditionColumn
                        +"&conditionValue="+conditionValue;


                restTemplate.exchange(url, HttpMethod.POST, null, Void.class);

            } catch (Exception e) {
                e.printStackTrace(); // Handle exception or log it
            }
        }
    }


}
