package com.graphhopper.geocoder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Peter Karich
 */
public class Configuration {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, String> props = new ConcurrentHashMap<String, String>();
    private volatile boolean autoReload = false;
    private static final String appName = "geocoder";

    public String getElasticSearchHost() {
        String value = get(appName + ".elasticsearch.url");
        if (value == null)
            return "localhost";
        return value;
    }

    public String getElasticSearchCluster() {
        String value = get(appName + ".elasticsearch.cluster");
        if (value == null)
            return "elasticsearch";
        return value;
    }

    public int getElasticSearchPort() {
        String value = get(appName + ".elasticsearch.port");
        if (value == null)
            return 9300;
        return Integer.parseInt(value);
    }

    public File getBaseFolder() {
        String value = get(appName + ".basefolder");
        if (value == null)
            return new File(System.getProperty("user.dir"));
        File folder = new File(value);
        if (folder.exists() && folder.isDirectory() && folder.canRead())
            return folder;
        return new File(System.getProperty("user.dir"));
    }

    public String get(String key) {
        // system values are more important!
        String val = System.getProperty(key);
        if (val == null)
            val = props.get(key);
        return val;
    }

    public void set(String key, String val) {
        props.put(key, val);
    }

    public Configuration stopReloadThread() {
        autoReload = false;
        return this;
    }

    public void startReloadThread() {
        final long reloadInterval = 10000;
        new Thread("configuration-reloader") {
            @Override
            public void run() {
                while (autoReload && !isInterrupted()) {
                    reload();
                    try {
                        Thread.sleep(reloadInterval);
                    } catch (InterruptedException ex) {
                        logger.info("Interrupted " + getName());
                        break;
                    }
                }
            }
        }.start();
        reload();
        logger.info("reloading config file every " + reloadInterval + "ms. properties:" + props.size());
    }

    public Configuration reload() {
        File file = new File(getBaseFolder(), "config.properties");
        try {
            load(new InputStreamReader(new FileInputStream(file)));
        } catch (FileNotFoundException ex) {
            logger.info("no configuration file found at " + file.getAbsolutePath() + " using defaults. " + ex.getMessage());
        }
        return this;
    }

    public void load(Reader reader) {
        try {
            props.clear();
            Helper.loadProperties(props, reader);
        } catch (IOException ex) {
            logger.error("Problem while reading configuration", ex);
        }
    }
}
