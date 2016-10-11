// @(#)$Id: WriteBackCache.java,v 1.2 2012/04/13 16:07:47 morten Exp $
package org.apache.catalina.cassandra.cache;

import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;

import org.apache.catalina.cassandra.CassandraSession;
import org.apache.catalina.cassandra.utils.SessionObjectSerialiser;
import org.apache.catalina.connector.Request;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;

/**
 * <ul>
 *   <li><b>Project:</b> TomcatCassandra</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.cache.AgressiveWriteBackCache</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 8 Mar 2012 14:19:51</li>
 *   <li><b>Description:</b>
 *     Implements a request-scope write-back cache for the Cassandra 
 *     session manager for Tomcat. All data that is stored in the
 *     session is cached in this object, and the data is written back to
 *     Cassandra only when all request processing has completed.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
public class AggressiveWriteBackCache extends CacheValve {

    /** Log4J logger instance for class WriteBackCache. */
    private final static Logger cat = Logger.getLogger(AggressiveWriteBackCache.class);
    /** Log4J debug setting for class WriteBackCache. */
    private final static boolean debug = cat.isDebugEnabled();
    
    /**
     * Constructor.
     */
    public AggressiveWriteBackCache() {
        
    }

    /**
     * Implementation of write-back cache.
     */
    private class Cache extends org.apache.catalina.cassandra.cache.Cache {

        /**
         * Dirty session objects that have been updated by the web application
         * (assumed after setAttribute() has been called), but that we have
         * not yet written back to Cassandra.
         */
        private Set<String> dirty = new HashSet<String>();
        
        /**
         * Extends Cache superclass.
         * @param request Request this cache has scope for.
         */
        public Cache(HttpServletRequest request) {
            super(request);
        }

        public void write(String name, Object object, SessionObjectSerialiser serialiser) {
            super.write(name, object, serialiser);
            dirty.add(name);
        }
        
        /*
         * (non-Javadoc)
         * @see org.apache.cassandra.cache.Cache#detach()
         * 
         * This method is extended to ensure that all data is written back to
         * Cassandra once all request processing has completed.
         */
        public void detach() {
            // Detach this cache from the current thread.
            super.detach();

            if (debug) cat.debug("Flushing cache "+this.getClass().getName());
            
            // Write all cached entries to cassandra.
            if (dirty.size() == 0) return;
            
            //final CassandraSession cassandra = this.getCassandraSession();
            final CassandraSession cassandra = this.getUpdatedCassandraSession();

            if (cassandra == null) return;
            
            if (this.serialiser == null) {
                cat.error("Unable to flush write-back cache as no Cassandra read/write operations have taken place within the scope of this request.");
                return;
            }

            // Get the names of the dirty session objects
            final Iterator<String> keys = dirty.iterator();
            while (keys.hasNext()) {
                // Get the next dirty session object.
                final String key = (String)keys.next();
                final Object object = super.data.get(key);
                if (object != null && object != NULL)
                    cassandra.setAttribute(key, object, this.serialiser);
                else
                    cassandra.removeAttribute(key);
            }
        }
    }

    protected org.apache.catalina.cassandra.cache.Cache getCache(Request request) {
        return new Cache(request);
    }

}
