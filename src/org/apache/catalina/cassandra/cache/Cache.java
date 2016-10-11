// @(#)$Id: Cache.java,v 1.2 2012/04/13 16:07:47 morten Exp $
package org.apache.catalina.cassandra.cache;

import java.util.*;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.catalina.cassandra.CassandraSession;
import org.apache.catalina.cassandra.CassandraStatistics;
import org.apache.catalina.cassandra.TomcatSession;
import org.apache.catalina.cassandra.utils.SessionObjectSerialiser;

import org.apache.log4j.Logger;



/**
 * <ul>
 *   <li><b>Project:</b> TomcatCassandra</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.cache.Cache</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 8 Mar 2012 14:02:39</li>
 *   <li><b>Description:</b>
 *     Base class for all implemented caching strategies.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
public abstract class Cache {

    /** Log4J logger instance for class CacheBase. */
    private final static Logger cat = Logger.getLogger(Cache.class);
    /** Log4J debug setting for class CacheBase. */
    private final static boolean debug = cat.isDebugEnabled();

    /** Request attribute used to store time it takes to flush/persist the cache. */
    public final static String FLUSH_TIME_ATTRIBUTE = "##CASSANDRA##FLUSH##TIME##";
    
    /** This binds a cache instance to the current thread. */
    private static ThreadLocal<Cache> cache = new ThreadLocal<Cache>();

    /** Container for the actual cached data. */
    protected Map<String,Object> data = new HashMap<String,Object>();

    /**
     * Reference to cassandra access layer for the current session.
     * We create this reference on demand only, thus preventing us from
     * accessing the session for servlets/JSPs that do not use sessions.
     */
    private CassandraSession _cassandra = null;
    
    /** Lazy-initialised session object serialiser. */
    protected SessionObjectSerialiser serialiser = null;
    
    /** Reference to servlet request. */
    private HttpServletRequest _request = null;
    
    /**
     * Creates a new cache instance.
     * @param request Current servlet request.
     */
    protected Cache(ServletRequest request) {
        this._request = (HttpServletRequest)request;
    }
    
    /**
     * Lazy creation of CassandraSession instance.
     * @return CassadraSession instance for managing session objects
     *    for the current request.
     */
    protected CassandraSession getCassandraSession() {
    	if (this._cassandra == null) {
	        final HttpSession httpsession = _request.getSession();
	        if (httpsession instanceof TomcatSession) {
	            final TomcatSession session = (TomcatSession)httpsession;
	            this._cassandra = session.getCassandraSession();
	        }
	        
	        if (this._cassandra == null) {
	            cat.error("Unable to use cache as Cassandra Session Manager is not installed!");
	        }
    	}
    	
	    return this._cassandra;    
    }

    protected CassandraSession getUpdatedCassandraSession() {
        final HttpSession httpsession = _request.getSession();
        if (httpsession instanceof TomcatSession) {
            final TomcatSession session = (TomcatSession)httpsession;
            this._cassandra = session.getCassandraSession();
        }

        if (this._cassandra == null) {
            cat.error("Unable to use cache as Cassandra Session Manager is not installed!");
        }

        return this._cassandra;
    }

    /**
     * This method tells you if the user has accessed the session within the
     * current request/cache scope.
     * @return true if user has accessed session.
     */
    public boolean hasCassandraSession() {
        return (this._cassandra != null);
    }
    
    /**
     * Writes an entry to the cache.
     * @param name Cache entry name.
     * @param object Cache entry data.
     */
    public void write(String name, Object object, SessionObjectSerialiser serialiser) {
        // Store reference for later use by detach() method.
        this.serialiser = serialiser;

        if (debug) cat.debug("Cache store of object "+name);
        if (object == null) {
            object = NULL;
        }
        else if (!(object instanceof java.io.Serializable)) {
            // It is good to get a warning (and stack trace) here, as the
            // extending cache may not trigger serialization right here and
            // now. To debug the calling code, the developer needs to be
            // able to trace back to the method/code that tries to store
            // a non-serializable object in the session.
            cat.warn("Session object "+name+" is of non-serializable class "+object.getClass().getName(), new Exception());
        }
        
        this.data.put(name, object);

        // Get the statistics counters for the current request.
        final CassandraStatistics stats = CassandraStatistics.getInstance(); 
        if (stats != null) stats.cacheWrite();
    }
    
    protected final static Object NULL = new Object();
    
    /**
     * Obtains an entry from the cache.
     * @param name Cache entry name.
     * @param serialiser Serialiser for session objects.
     * @return Cache entry data, or null if not in cache.
     */
    public Object read(String name, SessionObjectSerialiser serialiser) {
        // Store reference for later use by detach() method.
        this.serialiser = serialiser;

        // Attempt to get the object from the in-memory cache.
        Object object = this.data.get(name);
        
        // Get the statistics counters for the current request.
        final CassandraStatistics stats = CassandraStatistics.getInstance(); 
        
        // Cache miss.
        if (object == null) {
            if (stats != null) stats.cacheMiss();
            if (debug) cat.debug("Cache miss for object "+name+" (cassandra queried).");
            // See if cassandra has the object
            final CassandraSession cassandra = this.getCassandraSession();
            if (cassandra != null) {
                object = cassandra.getAttribute(name, serialiser);
                if (object != null)
                    this.data.put(name, object);
                else
                    this.data.put(name, NULL);
            }
        }
        else if (object == NULL) {
            if (stats != null) stats.cacheMiss();
            if (debug) cat.debug("Cache miss for object "+name+" (no cassandra query).");
            object = null;
        }
        // Cache hit - return the cached object.
        else {
            if (stats != null) stats.cacheHit();
            if (debug) cat.debug("Cache hit for object "+name);
        }

        return object;
    }

    /**
     * Obtains the list of entries in the cache and the Cassandra session
     * combined.
     * @return List of entries.
     */
    public List<String> getEntries() {
        // This will hold the complete set of session attributes
        final Set<String> unique = new HashSet<String>();

        // Get session attributes that exist in cassandra.
        final CassandraSession cassandra = this.getCassandraSession();
        if (cassandra != null) {
        	unique.addAll(cassandra.getAttributes());
        }

        // Add all non-NULL objects from this cache
        final Iterator<String> keys = this.data.keySet().iterator();
        while (keys.hasNext()) {
            final String key = keys.next();
            final Object object = this.data.get(key);
            if (object != null && object != NULL) unique.add(key);
        }
        
        final List<String> result = new ArrayList<String>();
        result.addAll(unique);
        return result;
    }
    
    /**
     * Removes an entry from this cache and from Cassandra.
     * @param name Name of entry to remove.
     */
    public void remove(String name) {
        this.data.put(name, NULL);
        final CassandraSession cassandra = this.getCassandraSession();
        if (cassandra != null) cassandra.removeAttribute(name);
    }
    
    /**
     * Attaches this object to the current thread/request.
     */
    public void attach() {
        Cache.cache.set(this);
    }
    
    /**
     * Detaches this object from the current thread/request.
     */
    public void detach() {
        Cache.cache.remove();
    }

    /**
     * Obtains the cache instance for the current thread/request.
     * @return the cache instance for the current thread/request.
     */
    public static Cache getInstance() {
        return (Cache)Cache.cache.get();
    }

    /**
     * Clears some references to prevent stale data from hanging around.
     */
    public void cleanup() {
    	this._request = null;
    	this._cassandra = null;
    }

}
