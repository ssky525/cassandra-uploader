package com.cassandratool.web.api;

import com.cassandratool.model.ConnectionProfile;
import com.cassandratool.service.CassandraClusterDiagnostics;
import com.cassandratool.service.CassandraSessionFactory;
import com.cassandratool.web.WebCassandraSupport;
import com.cassandratool.web.api.dto.TopologyResponse;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import javax.servlet.http.HttpSession;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/session")
public class SessionApiController {

    @PostMapping("/connect")
    public ResponseEntity<Map<String, Object>> connect(HttpSession http, @RequestBody ConnectionProfile profile) {
        List<String> missing = CassandraSessionFactory.validateProfile(profile);
        if (!missing.isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("ok", false, "error", "Заполните: " + String.join(", ", missing)));
        }
        try {
            CqlSession session = CassandraSessionFactory.connect(profile);
            WebCassandraSupport.set(http, session);
            List<String> hints = new ArrayList<>();
            CassandraClusterDiagnostics.warnIfLocalDcMismatch(session, profile.getLocalDatacenter(), hints::add);
            Map<String, Object> body = new HashMap<>();
            body.put("ok", true);
            body.put("hints", hints);
            return ResponseEntity.ok(body);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                    .body(Map.of("ok", false, "error", e.getMessage() != null ? e.getMessage() : "ошибка подключения"));
        }
    }

    @PostMapping("/disconnect")
    public ResponseEntity<Void> disconnect(HttpSession http) {
        WebCassandraSupport.clear(http);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/status")
    public Map<String, Object> status(HttpSession http) {
        boolean connected = WebCassandraSupport.isConnected(http);
        Map<String, Object> m = new HashMap<>();
        m.put("connected", connected);
        return m;
    }

    @GetMapping("/topology")
    public TopologyResponse topology(HttpSession http) {
        CqlSession s = requireCql(http);
        String cluster = s.getMetadata().getClusterName().orElse("cluster");
        List<TopologyResponse.NodeRow> rows = new ArrayList<>();
        for (Node n : s.getMetadata().getNodes().values()) {
            rows.add(new TopologyResponse.NodeRow(
                    n.getEndPoint().toString(),
                    n.getDatacenter(),
                    n.getRack(),
                    n.getState().name()));
        }
        return new TopologyResponse(cluster, rows);
    }

    private static CqlSession requireCql(HttpSession http) {
        CqlSession s = WebCassandraSupport.get(http);
        if (s == null) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Сначала подключитесь к Cassandra");
        }
        return s;
    }
}
