package com.dant.webproject.dbcomponents;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Column {
    private String name;
    private DataType type;
    private List<Object> values;

    public Column(String name, DataType type) {
        this.name = name;
        this.type = type;
        switch (type) {
            case INTEGER:
                this.values = new ArrayList<>();
                break;

            case STRING:
                this.values = new ArrayList<>();
                break;

            case DOUBLE:
                this.values = new ArrayList<>();
                break;

            case DATETIME_STRING:
                this.values = new ArrayList<>();
                break;

            default:
                throw new IllegalArgumentException("Unsupported column type: " + type);
        }
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    public void addValue(String value) {
        switch (type) {
            case INTEGER:
                try {
                    values.add(Integer.parseInt(value));
                } catch (Exception e) {
                    values.add(Integer.MIN_VALUE);
                }
                break;

            case STRING:
                values.add(value);
                break;

            case DOUBLE:
                try {
                    values.add(Double.parseDouble(value));
                } catch (Exception e) {
                    values.add(Double.NaN);
                }
                break;

            case DATETIME_STRING:
                values.add(formatTimestamp(value));
                break;

            default:
                throw new IllegalArgumentException("Unsupported column type: " + type);
        }
    }

    public List<Object> getValues() {
        return values;
    }

    private String formatTimestamp(String timestamp) {
        long microseconds = Long.parseLong(timestamp);
        long milliseconds = microseconds / 1000;

        Instant instant = Instant.ofEpochMilli(milliseconds);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        return formatter.format(instant.atZone(ZoneId.of("America/New_York")));
    }

} // Vous pouvez ajouter d'autres méthodes si nécessaire
