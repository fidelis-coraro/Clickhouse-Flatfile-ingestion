package com.example.Clickhouse_Flatfile_Ingestion.model;

import lombok.Data;



import java.util.List;

@Data

public class IngestionRequest {
    private String sourceType; // "clickhouse" or "flatfile"
    private String targetType;
    private ClickHouseConfig clickHouseConfig;
    private FlatFileConfig flatFileConfig;
    private String table;
    private List<String> selectedColumns;
    private List<String> joinTables;
    private String joinCondition;

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getTargetType() {
        return targetType;
    }

    public void setTargetType(String targetType) {
        this.targetType = targetType;
    }

    public ClickHouseConfig getClickHouseConfig() {
        return clickHouseConfig;
    }

    public void setClickHouseConfig(ClickHouseConfig clickHouseConfig) {
        this.clickHouseConfig = clickHouseConfig;
    }

    public FlatFileConfig getFlatFileConfig() {
        return flatFileConfig;
    }

    public void setFlatFileConfig(FlatFileConfig flatFileConfig) {
        this.flatFileConfig = flatFileConfig;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getSelectedColumns() {
        return selectedColumns;
    }

    public void setSelectedColumns(List<String> selectedColumns) {
        this.selectedColumns = selectedColumns;
    }

    public List<String> getJoinTables() {
        return joinTables;
    }

    public void setJoinTables(List<String> joinTables) {
        this.joinTables = joinTables;
    }

    public String getJoinCondition() {
        return joinCondition;
    }

    public void setJoinCondition(String joinCondition) {
        this.joinCondition = joinCondition;
    }
}
