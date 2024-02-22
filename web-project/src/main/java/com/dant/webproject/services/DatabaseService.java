package com.dant.webproject.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

@Component
public class DatabaseService {

    // Structure de données pour stocker les tables et les données
    //private Map<String, Map<String, Object>> database = new ConcurrentHashMap<>();
    private Map<String, Map<String, Object>> database = new HashMap<>();

    // Méthode pour effectuer une opération de sélection (SELECT)
    public List<Map<String, Object>> select(String table, List<String> columns) {
        List<Map<String, Object>> results = new ArrayList<>();

        // Vérifier si la table existe dans la base de données
        if (!database.containsKey(table)) {
            throw new IllegalArgumentException("Table non trouvée dans db");
        }

        // Récupérer les données de la table
        Map<String, Object> tableData = database.get(table);

        // Filtrer les données en fonction des conditions
        //On neglige pour l'instant les conditions
        //On renvoie donc toutes les lignes pour les colonnes indiquées
        for (Map.Entry<String, Object> entry : tableData.entrySet()) {
            Map<String, Object> row = new HashMap<>();
            for (String column : columns) {
                // Vérifier si la colonne est présente dans les données de la table
                if (!entry.getKey().equals(column)) {
                    throw new IllegalArgumentException("La colonne " + column + " n'existe pas dans la tbl");
                }

                // Ajouter la colonne et sa valeur à la ligne
                row.put(column, entry.getValue());
            }
        }
        return results;
    }
}
