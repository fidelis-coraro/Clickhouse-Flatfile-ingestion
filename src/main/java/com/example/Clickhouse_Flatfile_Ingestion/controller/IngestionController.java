package com.example.Clickhouse_Flatfile_Ingestion.controller;


import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.example.Clickhouse_Flatfile_Ingestion.model.ClickHouseConfig;
import com.example.Clickhouse_Flatfile_Ingestion.model.FlatFileConfig;
import com.example.Clickhouse_Flatfile_Ingestion.model.IngestionRequest;
import com.example.Clickhouse_Flatfile_Ingestion.service.ClickHouseService;
import com.example.Clickhouse_Flatfile_Ingestion.service.FlatFileService;
import jakarta.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Controller
public class IngestionController {
    private static final Logger log = LoggerFactory.getLogger(IngestionController.class);
    @Autowired
    private ClickHouseService clickHouseService;
    @Autowired
    private FlatFileService flatFileService;

    @GetMapping("/")
    public String index(Model model) {
        model.addAttribute("clickHouseConfig", new ClickHouseConfig());
        model.addAttribute("flatFileConfig", new FlatFileConfig());
        model.addAttribute("ingestionRequest", new IngestionRequest());
        return "index";
    }

    @PostMapping("/connect/clickhouse")
    public String connectClickHouse(@Valid @ModelAttribute ClickHouseConfig clickHouseConfig,
                                    BindingResult result,@RequestParam(value = "sourceType", required = false) String sourceType, Model model) {

        log.info("Received ClickHouseConfig: host={}, port={}, database={}, user={}, sourceType={}",
                clickHouseConfig.getHost(), clickHouseConfig.getPort(), clickHouseConfig.getDatabase(),
                clickHouseConfig.getUser(),sourceType);

        // Update ingestionRequest with sourceType
        IngestionRequest ingestionRequest = new IngestionRequest();
        ingestionRequest.setSourceType(sourceType);
        model.addAttribute("ingestionRequest", ingestionRequest);

        // Check binding errors
        if (result.hasErrors()) {
            log.error("Form binding errors: {}", result.getAllErrors());
            model.addAttribute("error", "Invalid configuration: please check all fields");
            model.addAttribute("clickHouseConfig", clickHouseConfig);
            model.addAttribute("flatFileConfig", new FlatFileConfig());
            return "index";
        }

        try {
            try (Connection conn = clickHouseService.connect(clickHouseConfig)) {
                model.addAttribute("tables", clickHouseService.getTables(conn));
                model.addAttribute("status", "Connected to ClickHouse");
            }
        } catch (Exception e) {
            log.error("Failed to connect to ClickHouse", e);
            model.addAttribute("error", "Connection failed: " + e.getMessage());
        }
        model.addAttribute("clickHouseConfig", clickHouseConfig);
        model.addAttribute("flatFileConfig", new FlatFileConfig());
        return "index";
    }

    @PostMapping("/connect/flatfile")
    public String connectFlatFile(@ModelAttribute FlatFileConfig flatFileConfig, Model model) {
        try {
            List<String> columns = flatFileService.getColumns(flatFileConfig);
            model.addAttribute("columns", columns);
            model.addAttribute("flatFileConfig", flatFileConfig);
            model.addAttribute("status", "Flat File loaded");
        } catch (IOException e) {
            model.addAttribute("error", "File processing failed: " + e.getMessage());
        }
        model.addAttribute("clickHouseConfig", new ClickHouseConfig());
        model.addAttribute("ingestionRequest", new IngestionRequest());
        return "index";
    }

    @PostMapping("/columns/clickhouse")
    public String getClickHouseColumns(@RequestParam String table, @ModelAttribute ClickHouseConfig clickHouseConfig, Model model) {
        try (Connection conn = clickHouseService.connect(clickHouseConfig)) {
            List<String> columns = clickHouseService.getColumns(conn, table);
            model.addAttribute("columns", columns);
            model.addAttribute("tables", clickHouseService.getTables(conn));
            model.addAttribute("clickHouseConfig", clickHouseConfig);
            model.addAttribute("selectedTable", table);
            model.addAttribute("status", "Columns loaded for table: " + table);
        } catch (SQLException e) {
            model.addAttribute("error", "Failed to load columns: " + e.getMessage());
        }
        model.addAttribute("flatFileConfig", new FlatFileConfig());
        model.addAttribute("ingestionRequest", new IngestionRequest());
        return "index";
    }

    @PostMapping(value = "/ingest", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public String ingest(@ModelAttribute IngestionRequest ingestionRequest, Model model) {
        try {
            long count;
            String outputPath = null;
            if ("clickhouse".equals(ingestionRequest.getSourceType()) && "flatfile".equals(ingestionRequest.getTargetType())) {
                try (Connection conn = clickHouseService.connect(ingestionRequest.getClickHouseConfig())) {
                    ResultSet rs = clickHouseService.queryData(conn, ingestionRequest.getTable(),
                            ingestionRequest.getSelectedColumns(), ingestionRequest.getJoinTables(),
                            ingestionRequest.getJoinCondition(), 0);
                    outputPath = "output-" + UUID.randomUUID() + ".csv";
                    count = flatFileService.writeToCsv(rs, outputPath, ingestionRequest.getFlatFileConfig().getDelimiter(),
                            ingestionRequest.getSelectedColumns());
                }
                model.addAttribute("downloadPath", outputPath);
            } else if ("flatfile".equals(ingestionRequest.getSourceType()) && "clickhouse".equals(ingestionRequest.getTargetType())) {
                List<List<String>> data = flatFileService.readCsv(ingestionRequest.getFlatFileConfig(),
                        ingestionRequest.getSelectedColumns());
                try (Connection conn = clickHouseService.connect(ingestionRequest.getClickHouseConfig())) {
                    String targetTable = ingestionRequest.getTable();
                    clickHouseService.createTable(conn, targetTable, ingestionRequest.getSelectedColumns());
                    count = clickHouseService.insertData(conn, targetTable, ingestionRequest.getSelectedColumns(), data);
                }
            } else {
                throw new IllegalArgumentException("Invalid source/target combination");
            }
            model.addAttribute("result", "Ingestion completed. Records processed: " + count);
        } catch (Exception e) {
            model.addAttribute("error", "Ingestion failed: " + e.getMessage());
        }
        model.addAttribute("clickHouseConfig", ingestionRequest.getClickHouseConfig());
        model.addAttribute("flatFileConfig", ingestionRequest.getFlatFileConfig());
        model.addAttribute("ingestionRequest", ingestionRequest);
        return "index";
    }

    @PostMapping("/preview")
    public String preview(@ModelAttribute IngestionRequest ingestionRequest, Model model) {
        try {
            List<List<String>> previewData = new ArrayList<>();
            List<String> columns = ingestionRequest.getSelectedColumns();
            if ("clickhouse".equals(ingestionRequest.getSourceType())) {
                try (Connection conn = clickHouseService.connect(ingestionRequest.getClickHouseConfig());
                     ResultSet rs = clickHouseService.queryData(conn, ingestionRequest.getTable(), columns,
                             ingestionRequest.getJoinTables(), ingestionRequest.getJoinCondition(), 100)) {
                    while (rs.next()) {
                        List<String> row = new ArrayList<>();
                        for (String col : columns) {
                            row.add(rs.getString(col));
                        }
                        previewData.add(row);
                    }
                }
            } else {
                previewData = flatFileService.readCsv(ingestionRequest.getFlatFileConfig(), columns);
                previewData = previewData.size() > 100 ? previewData.subList(0, 100) : previewData;
            }
            model.addAttribute("previewData", previewData);
            model.addAttribute("previewColumns", columns);
            model.addAttribute("status", "Preview loaded");
        } catch (Exception e) {
            model.addAttribute("error", "Preview failed: " + e.getMessage());
        }
        model.addAttribute("clickHouseConfig", ingestionRequest.getClickHouseConfig());
        model.addAttribute("flatFileConfig", ingestionRequest.getFlatFileConfig());
        model.addAttribute("ingestionRequest", ingestionRequest);
        return "index";
    }

    @GetMapping("/download/{filename:.+}")
    public ResponseEntity<FileSystemResource> downloadFile(@PathVariable String filename) {
        File file = new File(filename);
        if (!file.exists()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok()
                .header("Content-Disposition", "attachment; filename=" + file.getName())
                .body(new FileSystemResource(file));
    }
}
