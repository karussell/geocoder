package com.graphhopper.geocoder.http;

import com.google.inject.AbstractModule;
import com.graphhopper.geocoder.Configuration;
import com.graphhopper.geocoder.JsonFeeder;
import com.graphhopper.geocoder.QueryHandler;
import javax.inject.Singleton;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Peter Karich
 */
public class DefaultModule extends AbstractModule {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Configuration config;

    @Override
    protected void configure() {
        configureConfiguration();
        configureClient();
        configureQueryHandler();
    }

    private void configureConfiguration() {
        config = new Configuration();
        config.startReloadThread();
        bind(Configuration.class).toInstance(config);
    }

    private void configureClient() {
        Client client = JsonFeeder.createClient(config.getElasticSearchCluster(), config.getElasticSearchHost(), config.getElasticSearchPort());        
        bind(Client.class).toInstance(client);
    }

    private void configureQueryHandler() {
        bind(QueryHandler.class).in(Singleton.class);
    }
}
