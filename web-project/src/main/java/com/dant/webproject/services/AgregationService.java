package com.dant.webproject.services;

import com.dant.webproject.dbcomponents.AgregationType;
import com.dant.webproject.dbcomponents.Column;
import com.dant.webproject.dbcomponents.DataType;
import jakarta.persistence.criteria.CriteriaBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
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

    // Rajouter le group by en creant un dico
    public Object agregation(AgregationType type, String nametable, String namecolumn, String groupByCol) {
        Map<String, Column> table = databaseManagementService.getDatabase().get(nametable);
        Column column = table.get(namecolumn);
        Column groupBy = table.get(groupByCol);
        switch (type) {
            case SUM:
                return sum(column, groupBy);
            case COUNT:
                return count(column, groupBy);
            case MAX:
                return max(column, groupBy);
            case MIN:
                return min(column, groupBy);
            default:
                throw new IllegalArgumentException("Le type d'aggregation est invalide");
        }
    }

    public Map<Object, Integer> sum(Column c, Column groupBy) {

        if (c.getType() == DataType.STRING) {
            throw new IllegalArgumentException("Le type de la colonne n'est pas INTEGER");
        }

        List<Object> values = c.getValues();
        List<Object> valuesGroupBy = groupBy.getValues();
        Map<Object, Integer> res = new HashMap<>();

        for (int i = 0; i < values.size(); i++) {
            if (!(res.containsKey(valuesGroupBy.get(i)))) {
                res.put(valuesGroupBy.get(i), 0);
            }
            res.put(valuesGroupBy.get(i), (Integer) (values.get(i)) + res.get(valuesGroupBy.get(i)));
        }
        return res;
    }

    public Map<Object, Object> max(Column c, Column groupBy) {

        List<Object> values = c.getValues();
        List<Object> valuesGroupBy = groupBy.getValues();
        Map<Object, Object> res = new HashMap<>();

        for (int i = 0; i < values.size(); i++) {
            Object groupByKey = valuesGroupBy.get(i);
            Object currentValue = values.get(i);

            if (!res.containsKey(groupByKey)) {
                // Si la clé n'existe pas, initialisez avec la valeur actuelle
                res.put(groupByKey, currentValue);
            } else {
                // Si la clé existe, comparez et gardez le maximum
                Comparable currentMax = (Comparable) res.get(groupByKey);
                if (currentMax.compareTo(currentValue) < 0) {
                    res.put(groupByKey, currentValue);
                }
            }
        }
        return res;
    }

    public Map<Object, Object> min(Column c, Column groupBy) {

        List<Object> values = c.getValues();
        List<Object> valuesGroupBy = groupBy.getValues();
        Map<Object, Object> res = new HashMap<>();

        for (int i = 0; i < values.size(); i++) {
            Object groupByKey = valuesGroupBy.get(i);
            Object currentValue = values.get(i);

            if (!res.containsKey(groupByKey)) {
                // Si la clé n'existe pas, initialisez avec la valeur actuelle
                res.put(groupByKey, currentValue);
            } else {
                // Si la clé existe, comparez et gardez le minimum
                Comparable currentMin = (Comparable) res.get(groupByKey);
                if (currentMin.compareTo(currentValue) > 0) {
                    res.put(groupByKey, currentValue);
                }
            }
        }
        return res;
    }

    public Map<Object, Integer> count(Column c, Column groupBy) {
        List<Object> values = c.getValues();
        List<Object> valuesGroupBy = groupBy.getValues();

        Map<Object, Integer> res = new HashMap<>();

        for (int i = 0; i < values.size(); i++) {
            if (!(res.containsKey(valuesGroupBy.get(i)))) {
                res.put(valuesGroupBy.get(i), 0);
            }
            res.put(valuesGroupBy.get(i), (Integer) (1 + res.get(valuesGroupBy.get(i))));
        }
        return res;
    }

}
