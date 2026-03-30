package com.cassandratool.web.api;

import com.cassandratool.service.CassandraBulkLoadService;
import com.cassandratool.service.CassandraSchemaService;
import com.cassandratool.service.CsvParseService;
import com.cassandratool.service.CsvTableData;
import com.cassandratool.web.AsyncConfig;
import com.cassandratool.web.LoadJobRegistry;
import com.cassandratool.web.LoadJobState;
import com.cassandratool.web.WebCassandraSupport;
import com.cassandratool.web.api.dto.LoadJobResponse;
import com.cassandratool.web.api.dto.PreviewResponse;
import com.datastax.oss.driver.api.core.CqlSession;
import com.opencsv.exceptions.CsvException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.springframework.http.HttpStatus.UNAUTHORIZED;

@RestController
@RequestMapping("/api/csv")
public class CsvApiController {

    private static final int PREVIEW_MAX = 200;
    private static final DateTimeFormatter LOG_TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final CsvParseService csvParseService;
    private final CassandraBulkLoadService bulkLoadService;
    private final LoadJobRegistry loadJobRegistry;
    private final Executor loadExecutor;

    public CsvApiController(
            CsvParseService csvParseService,
            CassandraBulkLoadService bulkLoadService,
            LoadJobRegistry loadJobRegistry,
            @Qualifier(AsyncConfig.LOAD_EXECUTOR) Executor loadExecutor) {
        this.csvParseService = csvParseService;
        this.bulkLoadService = bulkLoadService;
        this.loadJobRegistry = loadJobRegistry;
        this.loadExecutor = loadExecutor;
    }

    @PostMapping(value = "/preview", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public PreviewResponse preview(
            HttpSession http,
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "firstRowHeader", defaultValue = "true") boolean firstRowHeader,
            @RequestParam("keyspace") String keyspace,
            @RequestParam("table") String table) throws IOException, CsvException {

        CqlSession cql = require(http);
        List<String> schemaCols = CassandraSchemaService.getColumnNamesOrdered(cql, keyspace, table)
                .orElseThrow(() -> new ResponseStatusException(
                        org.springframework.http.HttpStatus.NOT_FOUND, "Таблица не найдена"));

        Path temp = Files.createTempFile("csv-preview-", ".csv");
        try {
            file.transferTo(temp.toFile());
            CsvTableData raw = csvParseService.readAll(temp, StandardCharsets.UTF_8, firstRowHeader);
            CsvTableData data = alignCsvRowsToSchema(schemaCols, raw);
            List<List<String>> rows = data.getRows();
            int n = Math.min(PREVIEW_MAX, rows.size());
            List<List<String>> sub = rows.subList(0, n);
            List<List<String>> copy = new ArrayList<>(sub);
            return new PreviewResponse(data.getColumnNames(), copy);
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    @PostMapping(value = "/load", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Map<String, String> startLoad(
            HttpSession http,
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "firstRowHeader", defaultValue = "true") boolean firstRowHeader,
            @RequestParam("keyspace") String keyspace,
            @RequestParam("table") String table,
            @RequestParam("maxRows") int maxRows) throws IOException {

        CqlSession cql = require(http);
        if (maxRows < 1) {
            throw new ResponseStatusException(org.springframework.http.HttpStatus.BAD_REQUEST, "maxRows >= 1");
        }

        Path temp = Files.createTempFile("csv-load-", ".csv");
        file.transferTo(temp.toFile());

        String jobId = UUID.randomUUID().toString();
        LoadJobState state = new LoadJobState();
        loadJobRegistry.put(jobId, state);

        loadExecutor.execute(() -> runLoadJob(cql, temp, firstRowHeader, keyspace, table, maxRows, state));

        return Map.of("jobId", jobId);
    }

    @GetMapping("/load/{jobId}")
    public LoadJobResponse loadStatus(@PathVariable String jobId) {
        LoadJobState st = loadJobRegistry.get(jobId);
        if (st == null) {
            throw new ResponseStatusException(org.springframework.http.HttpStatus.NOT_FOUND, "Задача не найдена");
        }
        return new LoadJobResponse(
                st.getLogLines(),
                st.isDone(),
                st.getInserted(),
                st.getFailed(),
                st.getError());
    }

    private void runLoadJob(
            CqlSession cql,
            Path tempFile,
            boolean firstRowHeader,
            String keyspace,
            String table,
            int maxRows,
            LoadJobState state) {

        try {
            List<String> schemaCols = CassandraSchemaService.getColumnNamesOrdered(cql, keyspace, table)
                    .orElseThrow(() -> new IllegalArgumentException("Таблица не найдена"));
            CsvTableData raw = csvParseService.readAll(tempFile, StandardCharsets.UTF_8, firstRowHeader);
            CsvTableData data = alignCsvRowsToSchema(schemaCols, raw);

            appendLog(state, "— Начало загрузки —");
            appendLog(state, String.format(
                    "Keyspace.table: %s.%s, колонок: %d, строк в CSV: %d, maxRows: %d",
                    keyspace, table, schemaCols.size(), data.rowCount(), maxRows));

            CassandraBulkLoadService.LoadResult result = bulkLoadService.insertRows(
                    cql,
                    keyspace,
                    table,
                    data,
                    schemaCols,
                    maxRows,
                    msg -> appendLog(state, msg),
                    (successful, processed, total) -> { });

            state.finish(result.inserted(), result.failed());
            appendLog(state, String.format("Готово. Успешно: %d, ошибок: %d", result.inserted(), result.failed()));
        } catch (Exception e) {
            appendLog(state, "Ошибка: " + (e.getMessage() != null ? e.getMessage() : e.toString()));
            state.fail(e.getMessage());
        } finally {
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException ignored) {
                // ignore
            }
        }
    }

    private static void appendLog(LoadJobState state, String message) {
        state.appendLog(LocalTime.now().format(LOG_TIME) + " " + message);
    }

    private static CsvTableData alignCsvRowsToSchema(List<String> schemaColumns, CsvTableData raw) {
        int n = schemaColumns.size();
        List<List<String>> alignedRows = new ArrayList<>();
        for (List<String> row : raw.getRows()) {
            List<String> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                out.add(i < row.size() && row.get(i) != null ? row.get(i) : "");
            }
            alignedRows.add(out);
        }
        return new CsvTableData(schemaColumns, alignedRows);
    }

    private static CqlSession require(HttpSession http) {
        CqlSession s = WebCassandraSupport.get(http);
        if (s == null) {
            throw new ResponseStatusException(UNAUTHORIZED, "Не подключено");
        }
        return s;
    }
}
