package com.dant.database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

@Component
public class DatabaseService {

    // Structure de données pour stocker les tables et les données
    private Map<String, Map<String, Object>> database = new ConcurrentHashMap<>();

    // Méthode pour effectuer une opération de sélection (SELECT)
    public List<Map<String, Object>> select(String table, List<String> columns, Map<String, String> conditions) {
        List<Map<String, Object>> results = new ArrayList<>();

        // Vérifier si la table existe dans la base de données
        if (!database.containsKey(table)) {
            throw new IllegalArgumentException("Table non trouvée dans db");
        }

        // Récupérer les données de la table
        Map<String, Object> tableData = database.get(table);

        // Filtrer les données en fonction des conditions
        for (Map.Entry<String, Object> entry : tableData.entrySet()) {
            Map<String, Object> row = new HashMap<>();
            boolean matchConditions = true;
            for (String column : columns) {
                // Vérifier si la colonne est présente dans les données de la table
                if (!tableData.containsKey(column)) {
                    throw new IllegalArgumentException("La colonne " + column + " n'existe pas dans la tbl");
                }
                // Vérifier si la ligne satisfait toutes les conditions
                if (conditions.containsKey(column) && !conditions.get(column).equals(entry.getValue().toString())) {
                    matchConditions = false;
                    break;
                }
                // Ajouter la colonne et sa valeur à la ligne
                row.put(column, entry.getValue());
            }
            // Si la ligne satisfait toutes les conditions, l'ajouter aux résultats
            if (matchConditions) {
                results.add(row);
            }
        }

        return results;
    }
}
