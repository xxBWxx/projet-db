package com.dant.webproject.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.dant.webproject.dbcomponents.AgregationType;
import com.dant.webproject.dbcomponents.DataType;

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
    public DistributedService(SelectService selectService, DatabaseManagementService databaseManagementService,
            TableModificationService tableModificationService) {
        this.selectService = selectService;
        this.databaseManagementService = databaseManagementService;
        this.tableModificationService = tableModificationService;
    }

    public void createTableColDistributed(String tableName, List<String> columns, List<DataType> types) {
        databaseManagementService.createTableCol(tableName, columns, types);
        // String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };
        String[] serverUrls = { "http://132.227.114.34:8081", "http://132.227.114.35:8082" };

        // Headers pour la requête HTTP
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // Corps de la requête (les colonnes)

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("col_name", columns);
        requestBody.put("type", types);
        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);

        // Appel du point de terminaison sur chaque serveur
        for (String serverUrl : serverUrls) {
            String createTableUrl = serverUrl + "/databaseManagement/createTableCol?tableName=" + tableName;
            restTemplate.exchange(createTableUrl, HttpMethod.POST, requestEntity, Void.class);
        }
    }

    public void addColumnColDistributed(String tableName, List<String> columns, List<DataType> types) {
        tableModificationService.addColumn(tableName, columns, types);
        // String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };
        String[] serverUrls = { "http://132.227.114.34:8081", "http://132.227.114.35:8082" };

        // Headers pour la requête HTTP
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // Corps de la requête (les colonnes)

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("col_name", columns);
        requestBody.put("type", types);
        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);

        // Appel du point de terminaison sur chaque serveur
        for (String serverUrl : serverUrls) {
            String createTableUrl = serverUrl + "/tableModification/addColumn?tableName=" + tableName;
            restTemplate.exchange(createTableUrl, HttpMethod.POST, requestEntity, Void.class);
        }
    }

    @SuppressWarnings("rawtypes")
    public ResponseEntity createTableDistributed(String tableName) {
        ResponseEntity response = databaseManagementService.createTable(tableName);
        // String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };
        String[] serverUrls = { "http://132.227.114.34:8081", "http://132.227.114.35:8082" };

        // Appel du point de terminaison sur chaque serveur
        if (response.getStatusCode() == HttpStatus.CREATED) {
            for (String serverUrl : serverUrls) {
                String createTableUrl = serverUrl + "/databaseManagement/createTable?tableName=" + tableName;
                restTemplate.exchange(createTableUrl, HttpMethod.POST, null, Void.class);
            }
        }

        return response;
    }

    public void insertDistributed(String tableName, List<String> columns, List<List<String>> valuesList) {
        int cpt = 0;
        // String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };
        String[] serverUrls = { "http://132.227.114.34:8081", "http://132.227.114.35:8082" };

        if (valuesList.size() == 1) {
            tableModificationService.insert(tableName, columns, valuesList.get(0));
            return;
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        for (List<String> value : valuesList) {
            if (cpt == 0) {
                tableModificationService.insert(tableName, columns, value);
                cpt++;

                continue;
            }

            // Construire le corps de la requête
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("col_name", columns);
            requestBody.put("value", value);

            HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);

            // Construire l'URL pour le point de terminaison d'insertion
            String insertUrl = serverUrls[cpt - 1] + "/tableModification/insert?tableName=" + tableName;
            cpt = (cpt + 1) % 3;

            // Effectuer la requête HTTP POST
            restTemplate.exchange(insertUrl, HttpMethod.POST, requestEntity, Void.class);
        }
    }

    public void insertRowDistributed(String tableName, List<String> columns, List<String> value, int modulo) {
        // String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };
        String[] serverUrls = { "http://132.227.114.34:8081", "http://132.227.114.35:8082" };

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        if (modulo == 0) {
            tableModificationService.insert(tableName, columns, value);

            return;
        }

        // Construire le corps de la requête
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("col_name", columns);
        requestBody.put("value", value);

        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);

        // Construire l'URL pour le point de terminaison d'insertion
        String insertUrl = serverUrls[modulo - 1] + "/tableModification/insert?tableName=" + tableName;

        // Effectuer la requête HTTP POST
        restTemplate.exchange(insertUrl, HttpMethod.POST, requestEntity, Void.class);
    }

    public List<Map<String, Object>> select(String tableName, List<String> colNames, List<List<String>> conditions) {

        List<Map<String, Object>> res = selectService.select(tableName, colNames, conditions);
        // String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };
        String[] serverUrls = { "http://132.227.114.34:8081", "http://132.227.114.35:8082" };

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        for (String serverUrl : serverUrls) {

            // Construire le corps de la requête
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("columns", colNames);
            requestBody.put("where", conditions);

            HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);

            // Construire l'URL pour le point de terminaison d'insertion
            String insertUrl = serverUrl + "/select/select?tableName=" + tableName;

            // Effectuer la requête HTTP POST
            List<Map<String, Object>> result = restTemplate.exchange(insertUrl, HttpMethod.POST, requestEntity,
                    new ParameterizedTypeReference<List<Map<String, Object>>>() {
                    }).getBody();
            res.addAll(result);
        }
        return res;
    }

    public void insertmult(String tableName, List<String> col, List<List<String>> value, int modulo) {
        tableModificationService.insertMult(tableName, col, value);
    }

    @SuppressWarnings("rawtypes")
    public ResponseEntity updateColumnDistributed(String tableName, String columnName, String newData,
            String conditionColumn,
            Object conditionValue) {
        // String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };
        String[] serverUrls = { "http://132.227.114.34:8081", "http://132.227.114.35:8082" };

        ResponseEntity response = tableModificationService.updateColumn(tableName, columnName, newData, conditionColumn,
                conditionValue);

        if (response.getStatusCode() == HttpStatus.OK) {
            for (String serverUrl : serverUrls) {
                try {
                    String url = serverUrl + "/tableModification/updateCol?tableName=" + tableName
                            + "&columnName=" + columnName
                            + "&newData=" + newData
                            + "&conditionColumn=" + conditionColumn
                            + "&conditionValue=" + conditionValue;

                    restTemplate.exchange(url, HttpMethod.POST, null, Void.class);

                } catch (Exception e) {
                    e.printStackTrace(); // Handle exception or log it
                }
            }
        }

        return response;
    }

    @SuppressWarnings("rawtypes")
    public ResponseEntity deleteRowDistributed(String tableName, String conditionColumn,
            Object conditionValue) {
        // String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };
        String[] serverUrls = { "http://132.227.114.34:8081", "http://132.227.114.35:8082" };

        ResponseEntity response = tableModificationService.deleteRow(tableName, conditionColumn, conditionValue);

        if (response.getStatusCode() == HttpStatus.OK) {
            for (String serverUrl : serverUrls) {
                try {
                    String url = serverUrl + "/tableModification/deleteRow?tableName=" + tableName
                            + "&conditionColumn=" + conditionColumn
                            + "&conditionValue=" + conditionValue;

                    restTemplate.exchange(url, HttpMethod.POST, null, Void.class);

                } catch (Exception e) {
                    e.printStackTrace(); // Handle exception or log it
                }
            }
        }

        return response;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Object agregationDistributed(AgregationType type, String nametable, String namecolumn, String groupByCol) {
        // String[] serverUrls = { "http://localhost:8080", "http://localhost:8081",
        // "http://localhost:8082" };
        String[] serverUrls = { "http://132.227.114.33:8080", "http://132.227.114.34:8081",
                "http://132.227.114.35:8082" };

        // L'agrégation initiale est faite localement.
        // Map<Object, Object> aggregatedResults = (Map<Object, Object>)
        // agregationService.agregation(type, nametable, namecolumn, groupByCol);
        Map<Object, Object> aggregatedResults = new HashMap<>();

        for (String serverUrl : serverUrls) {
            try {
                // Corrigez l'URL pour utiliser correctement les & pour séparer les paramètres
                String url = serverUrl + "/agregation/groupBy?agregationType=" + type + "&tableName=" + nametable
                        + "&colName=" + namecolumn + "&groupByValues=" + groupByCol;

                RestTemplate restTemplate = new RestTemplate();
                ResponseEntity<Map<Object, Object>> response = restTemplate.exchange(url, HttpMethod.GET, null,
                        new ParameterizedTypeReference<Map<Object, Object>>() {
                        });

                Map<Object, Object> result = response.getBody();

                // Fusion des résultats selon le type d'agrégation
                if (result != null) {
                    result.forEach((key, value) -> {
                        Object existingValue = aggregatedResults.get(key);
                        if (existingValue == null) {
                            System.out.println(key);
                            aggregatedResults.put(key, value);
                        } else {
                            switch (type) {
                                case SUM:
                                    aggregatedResults.put(key, (Integer) existingValue + (Integer) value);
                                    break;
                                case COUNT:
                                    aggregatedResults.put(key, (Integer) existingValue + (Integer) value);
                                    break;
                                case MAX:
                                    if (((Comparable) existingValue).compareTo(value) < 0) {
                                        aggregatedResults.put(key, value);
                                    }
                                    break;
                                case MIN:
                                    if (((Comparable) existingValue).compareTo(value) > 0) {
                                        aggregatedResults.put(key, value);
                                    }
                                    break;
                                case AVG:
                                    aggregatedResults.put(key, ((Double) existingValue + (Double) value) / 2);
                            }
                        }
                    });
                }

            } catch (Exception e) {
                e.printStackTrace(); // Gestion des exceptions
            }
        }

        return aggregatedResults;
    }

    @SuppressWarnings("rawtypes")
    public ResponseEntity alterTableDistributed(String tableName, String columnName, String typeData) {
        // String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };
        String[] serverUrls = { "http://132.227.114.34:8081", "http://132.227.114.35:8082" };

        DataType type = DataType.valueOf(typeData);
        ResponseEntity response = databaseManagementService.alterTable(tableName, columnName, type);

        if (response.getStatusCode() == HttpStatus.OK) {
            for (String serverUrl : serverUrls) {
                try {
                    String url = serverUrl + "/databaseManagement/alterTable?tableName=" + tableName
                            + "&columnName=" + columnName
                            + "&typeData=" + typeData;

                    restTemplate.exchange(url, HttpMethod.POST, null, Void.class);

                } catch (Exception e) {
                    e.printStackTrace(); // Handle exception or log it
                }
            }
        }

        return response;
    }

}
