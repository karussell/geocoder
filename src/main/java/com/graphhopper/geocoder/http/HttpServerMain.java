package com.graphhopper.geocoder.http;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.EnumSet;
import javax.servlet.DispatcherType;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.LoggerFactory;

public class HttpServerMain {

    public static void main(String[] args) throws Exception {
        // see http://blog.palominolabs.com/2011/08/15/a-simple-java-web-stack-with-guice-jetty-jersey-and-jackson/
        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                binder().requireExplicitBindings();

                install(new DefaultModule());
                install(new MyServletModule());

                bind(GuiceFilter.class);
            }
        });

        Server server = new Server();
        ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/");

        handler.addServlet(new ServletHolder(new InvalidRequestServlet()), "/*");

        FilterHolder guiceFilter = new FilterHolder(injector.getInstance(GuiceFilter.class));
        handler.addFilter(guiceFilter, "/*", EnumSet.allOf(DispatcherType.class));                

        // SPDY works over TLS
        // create keystore via
        // keytool -genkey -keyalg RSA -alias selfsigned -keystore spdy.keystore -validity 360 -keysize 2048
        // It is important to match the correct npn version for your OpenJDK version
        // see http://www.eclipse.org/jetty/documentation/current/npn-chapter.html#npn-versions
//        SslContextFactory sslFactory = new SslContextFactory();
//        sslFactory.setKeyStorePath("src/main/resources/spdy.keystore");
//        sslFactory.setKeyStorePassword("whatacomplexthingtodo");
        int httpPort = 8999, sslPort = 8442, spdyPort = 9442;
        SelectChannelConnector connector0 = new SelectChannelConnector();
        connector0.setPort(httpPort);
        server.addConnector(connector0);

//        SslSelectChannelConnector connector1 = new SslSelectChannelConnector(sslFactory);
//        connector1.setPort(sslPort);
//        server.addConnector(connector1);
//        HTTPSPDYServerConnector connector2 = new HTTPSPDYServerConnector(sslFactory);
//        connector2.setPort(spdyPort);
//        server.addConnector(connector2);
        server.setHandler(handler);
        server.start();
        
        LoggerFactory.getLogger(HttpServerMain.class).info("Started server at HTTP " + httpPort
                + ", HTTPS " + sslPort + " and SPDY " + spdyPort);
    }
}
