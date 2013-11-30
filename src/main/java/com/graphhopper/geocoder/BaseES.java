package com.graphhopper.geocoder;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Peter Karich
 */
public abstract class BaseES {

    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected final Configuration config;
    protected final String osmType = "osmobject";
    protected final String osmIndex = "osm";
    protected Client client;

    public BaseES(Configuration config, Client client) {
        this.config = config;
        if (client == null)
            throw new IllegalArgumentException("client cannot be null");

        this.client = client;
    }

    public static Client createClient(String cluster, String url, int port) {
        Settings s = ImmutableSettings.settingsBuilder().put("cluster.name", cluster).build();
        TransportClient tmp = new TransportClient(s);
        tmp.addTransportAddress(new InetSocketTransportAddress(url, port));
        return tmp;
    }

    public static Client createClient(Configuration config) {
        // "failed to get node info for..." -> wrong elasticsearch version for client vs. server
        String cluster = config.getElasticSearchCluster();
        String host = config.getElasticSearchHost();
        int port = config.getElasticSearchPort();
        return createClient(cluster, host, port);
    }
}
