package com.graphhopper.geocoder.http;

import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import org.eclipse.jetty.servlets.GzipFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Just to check if response is really gzipped
 * <p/>
 * @author Peter Karich
 */
public class MyGZIPHook extends GzipFilter
{
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void doFilter( ServletRequest req, ServletResponse res, FilterChain chain ) throws IOException, ServletException
    {
        // logger.info("NOW " + req.getParameterMap().toString() + ", filter:" + chain);
        // if response contains "Content-Encoding" => do not filter
        super.doFilter(req, res, chain);
    }
}
