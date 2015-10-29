// @(#)$Id: SomeClass.java,v 1.1 2007/04/04 00:02:36 morten Exp $
package org.apache.catalina.cassandra.cache;

import java.io.IOException;

import javax.servlet.ServletException;

import org.apache.catalina.valves.ValveBase;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;

import org.apache.log4j.Logger;

/**
 * Base class for all valves that handle caching.
 * 
 * (C)opyright OpenJaw Technologies 2014
 * <ul>
 *   <li><b>Project:</b> TomcatCassandraApache</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.cache.CacheValve</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 13 Jan 2014 15:11:10</li>
 *   <li><b>Description:</b>
 *     Base class for all valves that handle caching.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
public abstract class CacheValve extends ValveBase {

    /** Log4J logger instance for class CacheValve. */
    private final static Logger cat = Logger.getLogger(CacheValve.class);
    /** Log4J debug setting for class CacheValve. */
    private final static boolean debug = cat.isDebugEnabled();

    protected CacheValve() {
        
    }
    
    protected abstract Cache getCache(Request request);

    /**
     * This method allows us to intercept requests before they are forwarded
     * to the target servlet/JSP/whatever. We use this to attach the cache
     * request to a thread-local variable.
     * @param request The servlet request to be processed
     * @param response The servlet response to be created
     */
    public void invoke(Request request, Response response) throws IOException, ServletException {

        try {
            // Create the cache.
            final Cache cache = getCache(request);
            cache.attach();
            
            // Process the next valve in the chain
            this.getNext().invoke(request, response);
            
            // Time the write of dirty session objects.
            final long start = System.currentTimeMillis();
            cache.detach();
            final long stop = System.currentTimeMillis();
            
            request.setAttribute(Cache.FLUSH_TIME_ATTRIBUTE, new Long(stop-start));
    
            cache.cleanup();
        }
        catch (Throwable e) {
            cat.error("Error in cache valve: "+e.getMessage(), e);
        }
    }
    
}
