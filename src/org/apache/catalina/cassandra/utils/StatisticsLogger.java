// @(#)$Id: SomeClass.java,v 1.1 2007/04/04 00:02:36 morten Exp $
package org.apache.catalina.cassandra.utils;

import java.io.IOException;

import javax.servlet.ServletException;

import org.apache.catalina.cassandra.CassandraStatistics;
import org.apache.catalina.cassandra.cache.Cache;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;
import org.apache.log4j.Logger;

/**
 * Valve that logs Cassandra I/O statistics.
 * 
 * (C)opyright OpenJaw Technologies 2014
 * <ul>
 *   <li><b>Project:</b> TomcatCassandraApache</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.utils.StatisticsLogger</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 13 Jan 2014 10:14:11</li>
 *   <li><b>Description:</b>
 *     Valve that logs Cassandra I/O statistics.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
public class StatisticsLogger extends ValveBase {

    /** Log4J logger instance for class StatisticsLogger. */
    private final static Logger cat = Logger.getLogger(StatisticsLogger.class);
    /** Log4J debug setting for class StatisticsLogger. */
    private final static boolean debug = cat.isDebugEnabled();

    public StatisticsLogger() {
        
    }
    
    /**
     * This method allows us to intercept requests before they are forwarded
     * to the target servlet/JSP/whatever.
     * @param request The servlet request to be processed
     * @param response The servlet response to be created
     */
    public void invoke(Request request, Response response) throws IOException, ServletException {

        // Attach statistics container if in debug mode.
        CassandraStatistics.attach(request.getRequestURI());
        
        // Process the next valve in the chain
        this.getNext().invoke(request, response);

        // Get the statistics counters for the current request.
        final CassandraStatistics stats = CassandraStatistics.detach(); 
        
        final Long flushTime = (Long)request.getAttribute(Cache.FLUSH_TIME_ATTRIBUTE);
        if (flushTime != null) stats.setFlushTime(flushTime.longValue());

        if (debug && request.getSession() != null) {
            final StringBuffer buf = new StringBuffer();
            buf.append(request.getRequestURL());
            buf.append(" [");
            buf.append(request.getSession().getId());
            buf.append("] ");
            final String prefix = buf.toString();
    
            cat.debug(prefix+stats.getCacheStatistics());
            cat.debug(prefix+stats.getReadStatistics());
            cat.debug(prefix+stats.getWriteStatistics());
            cat.debug(prefix+stats.getOverheadStatistics());

            // Output Excel-friendly CSV version of the statistics
            cat.debug(stats.toString());
        }
        
        stats.dump();
    }    

}
