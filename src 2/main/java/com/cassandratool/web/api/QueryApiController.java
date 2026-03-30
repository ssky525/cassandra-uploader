package com.cassandratool.web.api;

import com.cassandratool.service.CassandraSelectService;
import com.cassandratool.web.WebCassandraSupport;
import com.cassandratool.web.api.dto.PreviewResponse;
import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import javax.servlet.http.HttpSession;

import static org.springframework.http.HttpStatus.UNAUTHORIZED;

@RestController
@RequestMapping("/api/query")
public class QueryApiController {

    private final CassandraSelectService selectService;

    public QueryApiController(CassandraSelectService selectService) {
        this.selectService = selectService;
    }

    /**
     * Данные таблицы: SELECT * … LIMIT (по умолчанию 10, макс. 1000).
     */
    @GetMapping("/table-data")
    public PreviewResponse tableData(
            HttpSession http,
            @RequestParam("keyspace") String keyspace,
            @RequestParam("table") String table,
            @RequestParam(value = "limit", defaultValue = "10") int limit) {
        CqlSession s = WebCassandraSupport.get(http);
        if (s == null) {
            throw new ResponseStatusException(UNAUTHORIZED, "Не подключено");
        }
        int lim = limit;
        if (lim < 1) {
            lim = 1;
        }
        if (lim > 1000) {
            lim = 1000;
        }
        CassandraSelectService.TablePreview p = selectService.selectStarLimit(s, keyspace, table, lim);
        return new PreviewResponse(p.getColumnNames(), p.getRows());
    }
}
