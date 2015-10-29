// @(#)$Id: RequestHolderValve.java,v 1.2 2012/04/13 16:07:47 morten Exp $
package org.apache.catalina.cassandra.utils;

import java.io.IOException;

import org.apache.catalina.valves.ValveBase;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;

import javax.servlet.ServletException;

import org.apache.log4j.Logger;

/**
 * <ul>
 *   <li><b>Project:</b> TomcatCassandra</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.utils.RequestHolderValve</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 8 Mar 2012 14:19:51</li>
 *   <li><b>Description:</b>
 *     Filter that stores a reference to the current request in a thread-local
 *     variable so that it can be accessed by the SessionMap and SessionSet
 *     classes.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
public class RequestHolderValve extends ValveBase {

    /** Log4J logger instance for class WriteBackCache. */
    private final static Logger cat = Logger.getLogger(RequestHolderValve.class);
    /** Log4J debug setting for class WriteBackCache. */
    private final static boolean debug = cat.isDebugEnabled();

    /** Stores the current servlet request for each thread. */
    private static ThreadLocal<Request> request = new ThreadLocal<Request>();
    
    /**
     * Constructor.
     */
    public RequestHolderValve() {
        
    }

    /**
     * This method allows us to intercept requests before they are forwarded
     * to the target servlet/JSP/whatever. We use this to attach the current
     * request to a thread-local variable, and our write-through/write-back
     * cache is attached to this request. 
     * @param request The servlet request to be processed
     * @param response The servlet response to be created
     */
    public void invoke(Request request, Response response) throws IOException, ServletException { 
        // Attach the request
        RequestHolderValve.request.set(request);

        // Process the next valve in the chain.
        getNext().invoke(request, response);
        
        // Remove the reference to the request (to prevent memory leaks).
        // We use remove() rather than set(null) to prevent leaks.
        RequestHolderValve.request.remove();
    }

    /**
     * Returns the current servlet request as an HTTP servlet request.
     * @return the current servlet request as an HTTP servlet request.
     */
    public static javax.servlet.http.HttpServletRequest getCurrentRequest() {
        return (javax.servlet.http.HttpServletRequest)request.get();
    }

}
