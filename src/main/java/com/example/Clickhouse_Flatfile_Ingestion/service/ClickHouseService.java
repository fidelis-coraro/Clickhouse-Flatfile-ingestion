package com.example.Clickhouse_Flatfile_Ingestion.service;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.example.Clickhouse_Flatfile_Ingestion.controller.IngestionController;
import com.example.Clickhouse_Flatfile_Ingestion.model.ClickHouseConfig;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Service
public class ClickHouseService {
    private static final Logger log = LoggerFactory.getLogger(IngestionController.class);

    public Connection connect(ClickHouseConfig config) throws SQLException {

        String url = String.format("jdbc:ch://%s:%d/%s", config.getHost(), config.getPort(), config.getDatabase());
        String user = config.getUser();
        String password = config.getPassword() != null ? config.getPassword() : "";
        log.info("Connecting to ClickHouse at {}, user: {}", url, user);
        return DriverManager.getConnection(url, user, password);
    }

    public List<String> getTables(Connection conn) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (ResultSet rs = conn.getMetaData().getTables(null, null, "%", new String[]{"TABLE"})) {
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        }
        return tables;
    }

    public List<String> getColumns(Connection conn, String table) throws SQLException {
        List<String> columns = new ArrayList<>();
        String query = "SELECT name FROM system.columns WHERE table = ? AND database = ?";
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, table);
            stmt.setString(2, conn.getCatalog());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString("name"));
                }
            }
        }
        return columns;
    }

    public ResultSet queryData(Connection conn, String table, List<String> columns, List<String> joinTables, String joinCondition, int limit) throws SQLException {
        String columnList = String.join(",", columns);
        String query;
        if (joinTables == null || joinTables.isEmpty()) {
            query = String.format("SELECT %s FROM %s", columnList, table);
        } else {
            StringBuilder joinClause = new StringBuilder();
            joinClause.append(table).append(" t1");
            for (int i = 0; i < joinTables.size(); i++) {
                joinClause.append(" JOIN ").append(joinTables.get(i)).append(" t").append(i + 2)
                        .append(" ON ").append(joinCondition);
            }
            query = String.format("SELECT %s FROM %s", columnList, joinClause);
        }
        if (limit > 0) {
            query += " LIMIT " + limit;
        }
        return conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).executeQuery(query);
    }

    public long insertData(Connection conn, String table, List<String> columns, List<List<String>> data) throws SQLException {
        String columnList = String.join(",", columns);
        String placeholders = String.join(",", columns.stream().map(c -> "?").toList());
        String query = String.format("INSERT INTO %s (%s) VALUES (%s)", table, columnList, placeholders);
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            for (List<String> row : data) {
                for (int i = 0; i < row.size(); i++) {
                    stmt.setString(i + 1, row.get(i));
                }
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
        return data.size();
    }

    public void createTable(Connection conn, String table, List<String> columns) throws SQLException {
        String columnDefs = String.join(",", columns.stream().map(c -> c + " String").toList());
        String query = String.format("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE=Log", table, columnDefs);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(query);
        }
    }
    public byte[] exportTableToCsv(Connection conn, String tableName, List<String> columns) throws SQLException, IOException {
        String columnList = String.join(",", columns);
        String query = String.format("SELECT %s FROM %s FORMAT CSVWithNames", columnList, tableName);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            while (rs.next()) {
                baos.write(rs.getString(1).getBytes());
                baos.write("\n".getBytes());
            }
            return baos.toByteArray();
        }
    }
}
