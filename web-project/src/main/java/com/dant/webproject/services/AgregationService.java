package com.dant.webproject.services;

import com.dant.webproject.dbcomponents.AgregationType;
import com.dant.webproject.dbcomponents.Column;
import com.dant.webproject.dbcomponents.Type;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class AgregationService {

    @Autowired
    private final DatabaseManagementService databaseManagementService;

    @Autowired
    public AgregationService(DatabaseManagementService databaseManagementService) {
        this.databaseManagementService = databaseManagementService;
    }

    public Object agregation(AgregationType type, String nametable, String namecolumn) {

        Map<String, Column> table = databaseManagementService.getDatabase().get(nametable);
        Column column = table.get(namecolumn);
        switch (type) {
            case SUM:
                return sum(column);
            case COUNT:
                return count(column);
            case MAX:
                return max(column);
            case MIN:
                return min(column);
            default:
                throw new IllegalArgumentException("Le type d'aggregation est invalide");
        }
    }

    public int sum(Column c) {
        if (c.getType() == Type.STRING) {
            throw new IllegalArgumentException("Le type de la colonne n'est pas INTEGER");
        }
        List<Object> values = c.getValues();
        int total = 0;
        for (Object value : values) {
            total += (Integer) value;
        }
        return total;
    }

    public Object max(Column c) {
        List<Object> values = c.getValues();
        if (values.isEmpty()) {
            return null; // Retourne null si la liste est vide
        }

        Object max = values.get(0);
        for (Object value : values) {
            if (max.getClass() != value.getClass() || !(value instanceof Comparable)) {
                throw new IllegalArgumentException("All elements must be of the same type and comparable");
            }
            Comparable comparableValue = (Comparable) value;
            if (comparableValue.compareTo(max) > 0) {
                max = value;
            }
        }

        return max;
    }

    public Object min(Column c) {
        List<Object> values = c.getValues();
        if (values.isEmpty()) {
            return null; // Retourne null si la liste est vide
        }

        Object min = values.get(0);
        for (Object value : values) {
            if (min.getClass() != value.getClass() || !(value instanceof Comparable)) {
                throw new IllegalArgumentException("All elements must be of the same type and comparable");
            }
            Comparable comparableValue = (Comparable) value;
            if (comparableValue.compareTo(min) < 0) {
                min = value;
            }
        }

        return min;
    }

    public int count(Column c) {
        List<Object> values = c.getValues();
        return values.size(); // Retourne le nombre d'éléments dans la liste
    }

}
