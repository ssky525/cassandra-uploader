package com.cassandratool.web.api.dto;

import java.util.List;

public class TopologyResponse {

    private String clusterName;
    private List<NodeRow> nodes;

    public TopologyResponse() {}

    public TopologyResponse(String clusterName, List<NodeRow> nodes) {
        this.clusterName = clusterName;
        this.nodes = nodes;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public List<NodeRow> getNodes() {
        return nodes;
    }

    public void setNodes(List<NodeRow> nodes) {
        this.nodes = nodes;
    }

    public static class NodeRow {
        private String endpoint;
        private String datacenter;
        private String rack;
        private String state;

        public NodeRow() {}

        public NodeRow(String endpoint, String datacenter, String rack, String state) {
            this.endpoint = endpoint;
            this.datacenter = datacenter;
            this.rack = rack;
            this.state = state;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        public String getDatacenter() {
            return datacenter;
        }

        public void setDatacenter(String datacenter) {
            this.datacenter = datacenter;
        }

        public String getRack() {
            return rack;
        }

        public void setRack(String rack) {
            this.rack = rack;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }
    }
}
