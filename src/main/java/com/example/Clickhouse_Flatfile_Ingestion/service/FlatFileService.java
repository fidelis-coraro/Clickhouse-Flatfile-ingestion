package com.example.Clickhouse_Flatfile_Ingestion.service;

import com.example.Clickhouse_Flatfile_Ingestion.model.FlatFileConfig;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
@Slf4j
public class FlatFileService {
    public List<String> getColumns(FlatFileConfig config) throws IOException {
        try (CSVReader reader = new CSVReaderBuilder(
                new InputStreamReader(config.getFile().getInputStream()))
                .withCSVParser(new com.opencsv.CSVParserBuilder()
                        .withSeparator(config.getDelimiter().charAt(0)).build())
                .build()) {
            String[] headers = reader.readNext();
            return config.isHasHeader() && headers != null ? Arrays.asList(headers) : new ArrayList<>();
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }

    public long writeToCsv(ResultSet rs, String outputPath, String delimiter, List<String> columns) throws SQLException, IOException {
        try (CSVWriter writer = new CSVWriter(new FileWriter(outputPath), delimiter.charAt(0),
                CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END)) {
            writer.writeNext(columns.toArray(new String[0]));
            long count = 0;
            while (rs.next()) {
                String[] row = new String[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    row[i] = rs.getString(columns.get(i));
                }
                writer.writeNext(row);
                count++;
                if (count % 1000 == 0) {
                    writer.flush();
                }
            }
            return count;
        }
    }

    public List<List<String>> readCsv(FlatFileConfig config, List<String> selectedColumns) throws IOException {
        List<List<String>> data = new ArrayList<>();
        try (CSVReader reader = new CSVReaderBuilder(
                new InputStreamReader(config.getFile().getInputStream()))
                .withCSVParser(new com.opencsv.CSVParserBuilder()
                        .withSeparator(config.getDelimiter().charAt(0)).build())
                .build()) {
            String[] headers = reader.readNext();
            if (config.isHasHeader() && headers == null) {
                return data;
            }
            List<Integer> columnIndices = new ArrayList<>();
            if (config.isHasHeader()) {
                for (String col : selectedColumns) {
                    for (int i = 0; i < headers.length; i++) {
                        if (col.equals(headers[i])) {
                            columnIndices.add(i);
                            break;
                        }
                    }
                }
            } else {
                for (int i = 0; i < selectedColumns.size(); i++) {
                    columnIndices.add(i);
                }
            }
            String[] row;
            while ((row = reader.readNext()) != null) {
                List<String> selectedRow = new ArrayList<>();
                for (int idx : columnIndices) {
                    selectedRow.add(idx < row.length ? row[idx] : "");
                }
                data.add(selectedRow);
            }
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
        return data;
    }

    public void ingestCsvToTable(Connection conn, FlatFileConfig config, String tableName, List<String> columns) throws IOException, SQLException {
        String columnList = String.join(",", columns);
        String createTable = String.format("CREATE TABLE IF NOT EXISTS default.%s (%s) ENGINE = MergeTree() ORDER BY tuple()", tableName,
                columns.stream().map(c -> c + " String").reduce((a, b) -> a + "," + b).get());
        try (PreparedStatement createStmt = conn.prepareStatement(createTable)) {
            createStmt.execute();
        }

        String insertQuery = String.format("INSERT INTO default.%s (%s) FORMAT CSV", tableName, columnList);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(config.getFile().getInputStream()));
             PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {
            reader.lines().skip(config.isHasHeader() ? 1 : 0).forEach(line -> {
                try {
                    insertStmt.setString(1, line);
                    insertStmt.execute();
                } catch (SQLException e) {
                    log.error("Failed to insert line: {}", line, e);
                }
            });
        }
    }
}
