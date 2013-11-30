package com.graphhopper.geocoder.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Provides;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Singleton;

/**
 * @author Peter Karich
 */
public class MyServletModule extends JerseyServletModule {

    @Override
    protected void configureServlets() {
        Map<String, String> params = new HashMap<String, String>();
        params.put("mimeTypes", "text/html,"
                + "text/plain,"
                + "text/xml,"
                + "application/xhtml+xml,"
                + "text/css,"
                + "application/json,"
                + "application/javascript,"
                + "image/svg+xml");

        filter("/*").through(MyGZIPHook.class, params);
        bind(MyGZIPHook.class).in(Singleton.class);
        
        filter("/*").through(CORSFilter.class);
        bind(CORSFilter.class).in(Singleton.class);

        bind(GeocoderResource.class).in(Singleton.class);

        // hook Jersey into Guice Servlet
        bind(GuiceContainer.class);

        // hook Jackson into Jersey as the POJO <-> JSON mapper        
        // bind(JacksonJsonProvider.class).in(Singleton.class); -> use jacksonJsonProvider
        
        // writing DateTime via MyDateTimeSerializer
        // reading DateTime via:
        // bind(DateTimeDeserializer.class).in(Singleton.class);
        
        Map<String, String> guiceContainerConfig = new HashMap<String, String>();
        // make jersey read through the package to e.g. find DateTimeDeserializer as @Provider
        // guiceContainerConfig.put(PackagesResourceConfig.PROPERTY_PACKAGES, "com.graphhopper.geocoder.http");
        serve("/*").with(GuiceContainer.class, guiceContainerConfig);
    }

    // use custom objectmapper for jackson!
    @Provides @Singleton
    ObjectMapper objectMapper() {
        final ObjectMapper mapper = new ObjectMapper();        
        // Print out DateTime is a defined format.
//        mapper.registerModule(new SimpleModule() {
//            {                
//                addSerializer(new MyDateTimeSerializer());
//            }
//        });

        // pretty print json for development!
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        // mapper.writerWithDefaultPrettyPrinter()
        return mapper;
    }

    @Provides @Singleton
    JacksonJsonProvider jacksonJsonProvider(ObjectMapper mapper) {
        return new JacksonJsonProvider(mapper);
    }
}
