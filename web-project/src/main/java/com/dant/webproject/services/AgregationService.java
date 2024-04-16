package com.dant.webproject.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AgregationService {

    @Autowired
    private final DatabaseManagementService databaseManagementService;

    @Autowired
    public AgregationService(DatabaseManagementService databaseManagementService){
        this.databaseManagementService=databaseManagementService;
    }

    //Object agregation(AgregationType, nom colonne)

    //SUM(Colonne)
    //MAX(Colonne)
    //MIN(Colonne)
    //COUNT(Colonne)
    //Creer un record (agragationtype, nom col)
}
