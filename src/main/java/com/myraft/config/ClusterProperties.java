package com.myraft.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@ConfigurationProperties(prefix = "cluster")
public class ClusterProperties {
    private List<String> peers = new ArrayList<>();

    public List<String> getPeers() {
        return peers;
    }

    public void setPeers(List<String> peers) {
        // Пустые/blank значения (например -Dcluster.peers=) не должны становиться пирами:
        // иначе кворум считается по несуществующему узлу.
        this.peers = peers == null
                ? new ArrayList<>()
                : peers.stream()
                        .filter(peer -> peer != null && !peer.isBlank())
                        .collect(Collectors.toCollection(ArrayList::new));
    }
}
