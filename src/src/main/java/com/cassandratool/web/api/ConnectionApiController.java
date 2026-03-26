package com.cassandratool.web.api;

import com.cassandratool.model.ConnectionProfile;
import com.cassandratool.service.ConnectionStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/connections")
public class ConnectionApiController {

    private final ConnectionStore connectionStore;

    public ConnectionApiController(ConnectionStore connectionStore) {
        this.connectionStore = connectionStore;
    }

    @GetMapping
    public List<ConnectionProfile> list() throws IOException {
        return connectionStore.loadAll();
    }

    @PostMapping
    public ResponseEntity<Void> save(@RequestBody ConnectionProfile profile) throws IOException {
        if (profile.getDisplayName() == null || profile.getDisplayName().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        List<String> missing = com.cassandratool.service.CassandraSessionFactory.validateProfile(profile);
        if (!missing.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        if (profile.getId() == null || profile.getId().isBlank()) {
            profile.setId(UUID.randomUUID().toString());
        }
        List<ConnectionProfile> all = new ArrayList<>(connectionStore.loadAll());
        boolean updated = false;
        for (int i = 0; i < all.size(); i++) {
            if (all.get(i).getId().equals(profile.getId())) {
                all.set(i, profile);
                updated = true;
                break;
            }
        }
        if (!updated) {
            all.add(profile);
        }
        connectionStore.saveAll(all);
        return ResponseEntity.ok().build();
    }
}
