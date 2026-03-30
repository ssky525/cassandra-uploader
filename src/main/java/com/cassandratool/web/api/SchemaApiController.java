package com.cassandratool.web.api;

import com.cassandratool.service.CassandraSchemaService;
import com.cassandratool.web.WebCassandraSupport;
import com.cassandratool.web.api.dto.KeyspaceTreeDto;
import com.cassandratool.web.api.dto.MetadataDescriptionDto;
import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import javax.servlet.http.HttpSession;
import java.util.ArrayList;
import java.util.List;

import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.UNAUTHORIZED;

@RestController
@RequestMapping("/api/schema")
public class SchemaApiController {

    @GetMapping("/tree")
    public List<KeyspaceTreeDto> tree(HttpSession http) {
        CqlSession s = require(http);
        List<KeyspaceTreeDto> out = new ArrayList<>();
        for (String name : CassandraSchemaService.listKeyspaces(s)) {
            out.add(new KeyspaceTreeDto(name, CassandraSchemaService.listTables(s, name)));
        }
        return out;
    }

    @GetMapping("/keyspace/metadata")
    public MetadataDescriptionDto keyspaceMetadata(HttpSession http, @RequestParam("name") String name) {
        String text = CassandraSchemaService.describeKeyspaceForUi(require(http), name)
                .orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "Keyspace не найден"));
        return new MetadataDescriptionDto(text);
    }

    @GetMapping("/table/metadata")
    public MetadataDescriptionDto tableMetadata(
            HttpSession http,
            @RequestParam("keyspace") String keyspace,
            @RequestParam("table") String table) {
        String text = CassandraSchemaService.describeTableForUi(require(http), keyspace, table)
                .orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "Таблица не найдена"));
        return new MetadataDescriptionDto(text);
    }

    @GetMapping("/keyspaces")
    public List<String> keyspaces(HttpSession http) {
        return CassandraSchemaService.listKeyspaces(require(http));
    }

    @GetMapping("/tables")
    public List<String> tables(HttpSession http, @RequestParam("keyspace") String keyspace) {
        return CassandraSchemaService.listTables(require(http), keyspace);
    }

    @GetMapping("/columns")
    public List<String> columns(
            HttpSession http,
            @RequestParam("keyspace") String keyspace,
            @RequestParam("table") String table) {
        return CassandraSchemaService.getColumnNamesOrdered(require(http), keyspace, table)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Таблица не найдена"));
    }

    private static CqlSession require(HttpSession http) {
        CqlSession s = WebCassandraSupport.get(http);
        if (s == null) {
            throw new ResponseStatusException(UNAUTHORIZED, "Не подключено");
        }
        return s;
    }
}
