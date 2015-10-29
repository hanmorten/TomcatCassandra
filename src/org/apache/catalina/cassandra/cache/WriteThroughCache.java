// @(#)$Id: WriteThroughCache.java,v 1.2 2012/04/13 16:07:47 morten Exp $
package org.apache.catalina.cassandra.cache;

import org.apache.catalina.cassandra.CassandraSession;
import org.apache.catalina.cassandra.utils.SessionObjectSerialiser;
import org.apache.catalina.connector.Request;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;

/**
 * <ul>
 *   <li><b>Project:</b> TomcatCassandra</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.cache.WriteThroughCache</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 8 Mar 2012 13:42:25</li>
 *   <li><b>Description:</b>
 *     Implements a request-scope write-through cache for the Cassandra
 *     session manager for Tomcat. Data is instantly written to Cassandra for
 *     every cache update.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
public class WriteThroughCache extends CacheValve {

    /** Log4J logger instance for class WriteThroughCache. */
    private final static Logger cat = Logger.getLogger(WriteThroughCache.class);
    /** Log4J debug setting for class WriteThroughCache. */
    private final static boolean debug = cat.isDebugEnabled();
    
    /**
     * Constructor.
     */
    public WriteThroughCache() {
        
    }
    
    /**
     * Implementation of write-through cache.
     */
    private class Cache extends org.apache.catalina.cassandra.cache.Cache {
        
        /**
         * Extends Cache superclass.
         * @param request Request this cache has scope for.
         */
        public Cache(HttpServletRequest request) {
            super(request);
        }
        
        /*
         * (non-Javadoc)
         * @see org.apache.cassandra.cache.Cache#write(java.lang.String, java.lang.Object)
         * 
         * This method is overridden to ensure that any data is written to
         * Cassandra once it is written into the cache.
         */
        public void write(String name, Object object, SessionObjectSerialiser serialiser) {
            // Write the session object to the cache.
            super.write(name, object, serialiser);
            // Write the session object through to Cassandra.
            final CassandraSession cassandra = this.getCassandraSession();
            if (cassandra != null) {
                if (object != null)
                    cassandra.setAttribute(name, object, serialiser);
                else
                    cassandra.removeAttribute(name);
            }
        }        

    }

    protected org.apache.catalina.cassandra.cache.Cache getCache(Request request) {
        return new Cache(request);
    }

}
