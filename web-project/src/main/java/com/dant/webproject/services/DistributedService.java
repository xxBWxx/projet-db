package com.dant.webproject.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;

import java.util.*;

@Component
public class DistributedService {


    @Autowired
    private final SelectService selectService;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    public DistributedService(SelectService selectService){
        this.selectService=selectService;
    }


    public List<Map<String, List<String>>> selectAllDistributed(String tableName) {
        List<Map<String, List<String>>> value = new ArrayList<>();

        value.add(selectService.selectAll(tableName));

        String[] serverUrls = {"http://localhost:8081", "http://localhost:8082"} ;

        for (String serverUrl : serverUrls) {
            try {
                String url = serverUrl + "/select/selectallfrom?tableName=" + tableName;

                // Utilisation de ParameterizedTypeReference pour la désérialisation correcte
                Map<String, List<String>> result = restTemplate.exchange(url, HttpMethod.GET, null, new ParameterizedTypeReference<Map<String, List<String>>>() {}).getBody();
                value.add(result);

            } catch (Exception e) {
                e.printStackTrace(); // Handle exception or log it
            }
        }

        return value; // Return empty if not found anywhere
    }


}
