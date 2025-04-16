package com.example.Clickhouse_Flatfile_Ingestion;

import com.example.Clickhouse_Flatfile_Ingestion.model.ClickHouseConfig;
import com.example.Clickhouse_Flatfile_Ingestion.model.FlatFileConfig;
import com.example.Clickhouse_Flatfile_Ingestion.service.ClickHouseService;
import com.example.Clickhouse_Flatfile_Ingestion.service.FlatFileService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hibernate.validator.internal.util.Contracts.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.client.ExpectedCount.times;


public class IngestionServiceTest {
    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private ResultSet resultSet;

    @InjectMocks
    private ClickHouseService clickHouseService;

    @InjectMocks
    private FlatFileService flatFileService;

    private ClickHouseConfig clickHouseConfig;
    private FlatFileConfig flatFileConfig;

    @BeforeEach
    void setUp() throws SQLException {
        clickHouseConfig = new ClickHouseConfig();
        clickHouseConfig.setHost("localhost");
        clickHouseConfig.setPort(8123);
        clickHouseConfig.setDatabase("default");
        clickHouseConfig.setUser("default");
        clickHouseConfig.setPassword("");

        flatFileConfig = new FlatFileConfig();
        flatFileConfig.setDelimiter(",");
        flatFileConfig.setHasHeader(true);

        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    }

    @Test
    void testExportTableToCsv() throws SQLException, IOException {
        String tableName = "uk_price_paid";
        List<String> columns = Arrays.asList("price", "date", "postcode");
        String csvData = "\"250000\",\"2023-01-15\",\"SW1A 1AA\"\n\"300000\",\"2023-02-20\",\"EC1A 1BB\"\n";

        when(statement.executeQuery("SELECT price,date,postcode FROM uk_price_paid FORMAT CSVWithNames")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString(1)).thenReturn("\"250000\",\"2023-01-15\",\"SW1A 1AA\"", "\"300000\",\"2023-02-20\",\"EC1A 1BB\"");

        byte[] result = clickHouseService.exportTableToCsv(connection, tableName, columns);

        assertNotNull(result);
        assertEquals(csvData, new String(result));
        verify(statement).executeQuery("SELECT price,date,postcode FROM uk_price_paid FORMAT CSVWithNames");
    }

    @Test
    void testIngestCsvToTable() throws IOException, SQLException {
        String csvContent = "price,date,postcode\n250000,2023-01-15,SW1A 1AA\n300000,2023-02-20,EC1A 1BB\n";
        MultipartFile file = new MockMultipartFile("file", "test.csv", "text/csv", csvContent.getBytes());
        flatFileConfig.setFile(file);
        String tableName = "test_table";
        List<String> columns = Arrays.asList("price", "date", "postcode");

        when(preparedStatement.execute()).thenReturn(true);

        flatFileService.ingestCsvToTable(connection, flatFileConfig, tableName, columns);

        verify(connection).prepareStatement("CREATE TABLE IF NOT EXISTS default.test_table (price String,date String,postcode String) ENGINE = MergeTree() ORDER BY tuple()");
        verify(connection).prepareStatement("INSERT INTO default.test_table (price,date,postcode) FORMAT CSV");
        verify(preparedStatement, times(2)).execute();
    }

    @Test
    void testExportJoinedTablesToCsv() throws SQLException, IOException {
        String tableName = "uk_price_paid JOIN other_table ON uk_price_paid.postcode = other_table.postcode";
        List<String> columns = Arrays.asList("uk_price_paid.price", "uk_price_paid.date", "other_table.data");
        String csvData = "\"250000\",\"2023-01-15\",\"extra\"\n";

        when(statement.executeQuery("SELECT uk_price_paid.price,uk_price_paid.date,other_table.data FROM uk_price_paid JOIN other_table ON uk_price_paid.postcode = other_table.postcode FORMAT CSVWithNames")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString(1)).thenReturn("\"250000\",\"2023-01-15\",\"extra\"");

        byte[] result = clickHouseService.exportTableToCsv(connection, tableName, columns);

        assertNotNull(result);
        assertEquals(csvData, new String(result));
    }

    @Test
    void testConnectionFailure() {
        clickHouseConfig.setUser("invalid_user");

        SQLException exception = new SQLException("Code: 194. DB::Exception: invalid_user: Authentication failed");
        try {
            when(DriverManager.getConnection(anyString(), eq("invalid_user"), anyString())).thenThrow(exception);
            clickHouseService.connect(clickHouseConfig);
            fail("Expected SQLException");
        } catch (SQLException e) {
            assertEquals("Code: 194. DB::Exception: invalid_user: Authentication failed", e.getMessage());
        }
    }

    @Test
    void testDataPreview() throws SQLException {
        String tableName = "uk_price_paid";
        List<String> columns = Arrays.asList("price", "date");
        String query = "SELECT price,date FROM uk_price_paid LIMIT 2";

        when(statement.executeQuery(query)).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("price")).thenReturn("250000", "300000");
        when(resultSet.getString("date")).thenReturn("2023-01-15", "2023-02-20");

        ResultSet rs = statement.executeQuery(query);
        List<List<String>> preview = new ArrayList<>();
        while (rs.next()) {
            List<String> row = Arrays.asList(rs.getString("price"), rs.getString("date"));
            preview.add(row);
        }

        assertEquals(2, preview.size());
        assertEquals(Arrays.asList("250000", "2023-01-15"), preview.get(0));
        assertEquals(Arrays.asList("300000", "2023-02-20"), preview.get(1));
    }
}
