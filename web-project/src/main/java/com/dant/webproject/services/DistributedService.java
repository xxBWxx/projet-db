package com.dant.webproject.services;

import com.dant.webproject.dbcomponents.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.client.RestTemplate;
import org.springframework.core.ParameterizedTypeReference;

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
    private final AgregationService agregationService;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    public DistributedService(SelectService selectService, DatabaseManagementService databaseManagementService,
            TableModificationService tableModificationService, AgregationService agregationService) {
        this.selectService = selectService;
        this.databaseManagementService = databaseManagementService;
        this.tableModificationService = tableModificationService;
        this.agregationService = agregationService;
    }

    public void createTableColDistributed(String tableName, List<String> columns, List<DataType> types) {
        // List<DataType> typeList =
        // types.stream().map(DataType::valueOf).collect(Collectors.toList());

        databaseManagementService.createTableCol(tableName, columns, types);
        String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };

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

    public void createTableDistributed(String tableName) {
        databaseManagementService.createTable(tableName);
        String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };

        // Appel du point de terminaison sur chaque serveur
        for (String serverUrl : serverUrls) {
            String createTableUrl = serverUrl + "/databaseManagement/createTable?tableName=" + tableName;
            restTemplate.exchange(createTableUrl, HttpMethod.POST, null, Void.class);
        }
    }

    public void insertDistributed(String tableName, List<String> columns, List<List<String>> valuesList) {
        int cpt = 0;
        String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };

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
        String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };

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

    public void insertmult(String tableName, List<String> col, List<List<String>> value, int modulo) {
        tableModificationService.insertMult(tableName,col,value);
    }

    public Map<String, List<Object>> selectAllDistributed(String tableName) {

        Map<String, List<Object>> value = new HashMap<>();
        selectService.selectAll(tableName).forEach((key, val) -> {
            if (!value.containsKey(key)) {
                value.put(key, new ArrayList<>());
            }
            value.get(key).addAll(val);
        });

        String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };

        for (String serverUrl : serverUrls) {
            try {
                String url = serverUrl + "/select/selectallfrom?tableName=" + tableName;

                // Utilisation de ParameterizedTypeReference pour la désérialisation correcte
                Map<String, List<Object>> result = restTemplate.exchange(url, HttpMethod.GET, null,
                        new ParameterizedTypeReference<Map<String, List<Object>>>() {
                        }).getBody();
                result.forEach((key, val) -> value.get(key).addAll(val));

            } catch (Exception e) {
                e.printStackTrace(); // Handle exception or log it
            }
        }

        return value; // Return empty if not found anywhere
    }

    public Map<String, List<Object>> select_colsDistributed(String tableName, List<String> col_names) {
        String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };

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
                Map<String, List<Object>> result = restTemplate.exchange(url, HttpMethod.POST, requestEntity,
                        new ParameterizedTypeReference<Map<String, List<Object>>>() {
                        }).getBody();
                result.forEach((key, val) -> value.get(key).addAll(val));

            } catch (Exception e) {
                e.printStackTrace(); // Handle exception or log it
            }
        }

        return value; // Return empty if not found anywhere
    }

    /*
     * public Map<String, List<Object>> selectWhere_eqDistributed(String tableName,
     * String colName, String val) {
     *
     * Map<String, List<Object>> value = new HashMap<>();
     * selectService.selectWhere_eq(tableName, colName, val).forEach((key, v) -> {
     * if (!value.containsKey(key)) {
     * value.put(key, new ArrayList<>());
     * }
     * value.get(key).addAll(v);
     * });
     *
     *
     * String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"} ;
     *
     * for (String serverUrl : serverUrls) {
     * try {
     * String url = serverUrl + "/select/select_where_eq_from?tableName=" +
     * tableName +"&colName="+colName+"&val="+val;
     *
     * // Utilisation de ParameterizedTypeReference pour la désérialisation correcte
     * Map<String, List<Object>> result = restTemplate.exchange(url, HttpMethod.GET,
     * null, new ParameterizedTypeReference<Map<String, List<Object>>>()
     * {}).getBody();
     * result.forEach((key, v) -> value.get(key).addAll(v));
     *
     * } catch (Exception e) {
     * e.printStackTrace(); // Handle exception or log it
     * }
     * }
     *
     * return value; // Return empty if not found anywhere
     * }
     *
     * public Map<String, List<Object>> selectWhere_supDistributed(@RequestParam
     * String tableName, @RequestParam String colName,@RequestParam String val) {
     * Map<String, List<Object>> value = new HashMap<>();
     * selectService.selectWhere_sup(tableName, colName, val).forEach((key, v) -> {
     * if (!value.containsKey(key)) {
     * value.put(key, new ArrayList<>());
     * }
     * value.get(key).addAll(v);
     * });
     *
     *
     * String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"} ;
     *
     * for (String serverUrl : serverUrls) {
     * try {
     * String url = serverUrl + "/select/select_where_sup_from?tableName=" +
     * tableName +"&colName="+colName+"&val="+val;
     *
     * // Utilisation de ParameterizedTypeReference pour la désérialisation correcte
     * Map<String, List<Object>> result = restTemplate.exchange(url, HttpMethod.GET,
     * null, new ParameterizedTypeReference<Map<String, List<Object>>>()
     * {}).getBody();
     * result.forEach((key, v) -> value.get(key).addAll(v));
     *
     * } catch (Exception e) {
     * e.printStackTrace(); // Handle exception or log it
     * }
     * }
     *
     * return value; // Return empty if not found anywhere
     * }
     *
     * public Map<String, List<Object>> selectWhere_infDistributed(@RequestParam
     * String tableName, @RequestParam String colName,@RequestParam String val) {
     * Map<String, List<Object>> value = new HashMap<>();
     * selectService.selectWhere_inf(tableName, colName, val).forEach((key, v) -> {
     * if (!value.containsKey(key)) {
     * value.put(key, new ArrayList<>());
     * }
     * value.get(key).addAll(v);
     * });
     *
     *
     * String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"} ;
     *
     * for (String serverUrl : serverUrls) {
     * try {
     * String url = serverUrl + "/select/select_where_inf_from?tableName=" +
     * tableName +"&colName="+colName+"&val="+val;
     *
     * // Utilisation de ParameterizedTypeReference pour la désérialisation correcte
     * Map<String, List<Object>> result = restTemplate.exchange(url, HttpMethod.GET,
     * null, new ParameterizedTypeReference<Map<String, List<Object>>>()
     * {}).getBody();
     * result.forEach((key, v) -> value.get(key).addAll(v));
     *
     * } catch (Exception e) {
     * e.printStackTrace(); // Handle exception or log it
     * }
     * }
     *
     * return value; // Return empty if not found anywhere
     * }
     */

    public void updateColumnDistributed(String tableName, String columnName, String newData, String conditionColumn,
            Object conditionValue) {
        String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };
        tableModificationService.updateColumn(tableName, columnName, newData, conditionColumn, conditionValue);
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

    public Object agregationDistributed(AgregationType type, String nametable, String namecolumn, String groupByCol){
        String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };

        // L'agrégation initiale est faite localement.
        Map<Object, Object> aggregatedResults = (Map<Object, Object>) agregationService.agregation(type, nametable, namecolumn, groupByCol);

        for (String serverUrl : serverUrls) {
            try {
                // Corrigez l'URL pour utiliser correctement les & pour séparer les paramètres
                String url = serverUrl + "/agregation/selectFrom?type=" + type + "&nametable=" + nametable + "&namecolumn=" + namecolumn + "&groupByCol=" + groupByCol;

                RestTemplate restTemplate = new RestTemplate();
                ResponseEntity<Map<Object, Object>> response = restTemplate.exchange(url, HttpMethod.GET, null, new ParameterizedTypeReference<Map<Object, Object>>() {});

                Map<Object, Object> result = response.getBody();

                // Fusion des résultats selon le type d'agrégation
                if (result != null) {
                    result.forEach((key, value) -> {
                        Object existingValue = aggregatedResults.get(key);
                        if (existingValue == null) {
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

    public Map<String, List<Object>> selectWhereDistributed(String tableName, List<List<String>> op) {
        Map<String, List<Object>> value = new HashMap<>();

        List<Operande> listop = new ArrayList<>();

        for (List<String> l : op) {
            if (l.get(1).equals("=")) {
                EqOperande newop = new EqOperande(l.get(0), l.get(2));
                listop.add(newop);
            }
            if (l.get(1).equals(">")) {
                SupOperande newop = new SupOperande(l.get(0), l.get(2));
                listop.add(newop);
            }
            if (l.get(1).equals("<")) {
                InfOperande newop = new InfOperande(l.get(0), l.get(2));
                listop.add(newop);
            }
        }

        selectService.select_where(tableName, listop).forEach((key, v) -> {
            if (!value.containsKey(key)) {
                value.put(key, new ArrayList<>());
            }
            value.get(key).addAll(v);
        });

        String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"} ;
        for (String serverUrl : serverUrls) {
            try {
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);

                    HttpEntity<List<List<String>>> requestEntity = new HttpEntity<>(op);
                    String url = serverUrl + "/select/select_where?tableName=" + tableName;

                    // Utilisation de ParameterizedTypeReference pour la désérialisation correcte
                    Map<String, List<Object>> result = restTemplate.exchange(url, HttpMethod.POST, requestEntity, new ParameterizedTypeReference<Map<String, List<Object>>>() {}).getBody();
                    result.forEach((key, v) -> value.get(key).addAll(v));

            } catch (Exception e) {
                e.printStackTrace(); // Handle exception or log it
            }
      }

     return value; // Return empty if not found anywhere
    }

}
