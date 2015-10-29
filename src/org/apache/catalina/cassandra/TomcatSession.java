// @(#)$Id: TomcatSession.java,v 1.10 2012/04/13 16:07:47 morten Exp $
package org.apache.catalina.cassandra;

import java.util.*;

import java.security.Principal;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;

import org.apache.catalina.Context;
import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.catalina.SessionListener;
import org.apache.catalina.cassandra.cache.Cache;
import org.apache.catalina.cassandra.utils.IteratorEnumeration;

import org.apache.log4j.Logger;

import org.apache.catalina.cassandra.utils.SessionObjectSerialiser;

/**
 * <ul>
 *   <li><b>Project:</b> TomcatCassandra</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.TomcatSession</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 15 Aug 2011 11:16:46</li>
 *   <li><b>Description:</b>
 *     Cassandra session container for Tomcat.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
@SuppressWarnings("deprecation")
public class TomcatSession implements Session, HttpSession {

    /** Log4J logger instance for class TomcatSession. */
    private final static Logger cat = Logger.getLogger(TomcatSession.class);
    /** Log4J debug setting for class TomcatSession. */
    private final static boolean debug = cat.isDebugEnabled();

    /** Tomcat manager that manages this session (and other sessions). */
    private TomcatManager manager;
    
    /** Container for the session data in cassandra. */
    private CassandraSession cassandra;
    
    /** Our session ID. */
    private String id;
    
    /**
     * Session notes are set for each request for authenticated sessions.
     * We don't want to store these unnecessarily in Cassandra, so we store
     * them in this (transient) map, making them available to servlets.
     */
    private Map<String,Object> notes = new HashMap<String,Object>();
    
    /**
     * Creates a new Tomcat session container.
     * @param manager Owning tomcat manager.
     * @param cassandra container for the session data in cassandra.
     */
    TomcatSession(TomcatManager manager, CassandraSession cassandra) {
        this.manager = manager;
        this.cassandra = cassandra;
        this.id = this.cassandra.getId();
    }

    /**
     * Obtains the class-loader for the web-application/context that this
     * session belongs to. This class-loader is used to re-build serialised
     * objects in this session.
     * @return Class-loader for this session's web application/context.
     */
    ClassLoader getClassLoader() {
        return this.manager.getClassLoader();
    }

    SessionObjectSerialiser getSerialiser() {
        return this.manager.getSerialiser();
    }
    
    /**
     * Obtains the cassandra layer for this session.
     * @return the cassandra layer for this session.
     */
    public CassandraSession getCassandraSession() {
        return this.cassandra;
    }
    
    /** ================================================================== **/
    /** Tomcat Session implementation.                                     **/
    /** ================================================================== **/

    /**
     * Sets the session ID.
     * @param id Session ID.
     */
    public void setId(String id) {
        if (debug) cat.debug("["+id+"] Session ID set to "+id);
        this.id = id;
        cassandra.setId(id);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#addSessionListener(org.apache.catalina.SessionListener)
     */
    public void addSessionListener(SessionListener sessionListener) {
        cat.error("["+id+"] Session listeners are not supported!");
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#removeSessionListener(org.apache.catalina.SessionListener)
     */
    public void removeSessionListener(SessionListener sessionListener) {
        cat.error("["+id+"] Session listeners are not supported!");
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#setAuthType(java.lang.String)
     */
    public void setAuthType(String authType) {
        cassandra.setAuthType(authType);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#getAuthType()
     */
    public String getAuthType() {
        return cassandra.getAuthType();
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#setPrincipal(java.security.Principal)
     */
    public void setPrincipal(Principal principal) {
    	if (principal instanceof java.io.Serializable) {
    		cassandra.setPrincipal(principal, this.getSerialiser());
    	}
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#getPrincipal()
     */
    public Principal getPrincipal() {
        return cassandra.getPrincipal(this.getSerialiser());
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#getInfo()
     */
    public String getInfo() {
        return "TomcatCassandraSession/1.4";
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#setNote(java.lang.String, java.lang.Object)
     */
    public void setNote(String name, Object value) {
    	this.notes.put(name, value);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#removeNote(java.lang.String)
     */
    public void removeNote(String name) {
    	this.notes.remove(name);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#getNote(java.lang.String)
     */
    public Object getNote(String name) {
    	return this.notes.get(name);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#getNoteNames()
     */
    public Iterator<String> getNoteNames() {
    	return this.notes.keySet().iterator();
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#setValid(boolean)
     */
    public void setValid(boolean valid) {
        cassandra.setValid(valid);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#isValid()
     */
    public boolean isValid() {
        return cassandra.isValid();
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#expire()
     */
    public void expire() {
        cassandra.expire();
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#recycle()
     */
    public void recycle() {
        cassandra.recycle();
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#setManager(org.apache.catalina.Manager)
     */
    public void setManager(Manager manager) {
        this.manager = (TomcatManager)manager;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#getManager()
     */
    public Manager getManager() {
        return this.manager;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#getSession()
     */
    public HttpSession getSession() {
        return this;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#getIdInternal()
     */
    public String getIdInternal() {
        return cassandra.getId();
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#access()
     */
    public void access() {
        cassandra.access();
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Session#endAccess()
     */
    public void endAccess() {
        cassandra.endAccess();
    }

    /** ================================================================== **/
    /** HTTP HttpServlet implementation.                                   **/
    /** ================================================================== **/

    public void setAttribute(String name, Object value) {
        if (debug) {
            if (value == null)
                cat.debug("["+id+"] Setting attribute "+name+" = "+value);
            else
                cat.debug("["+id+"] Setting attribute "+name+" = "+value.getClass().getName());
        }
        final Cache cache = Cache.getInstance();
        final SessionObjectSerialiser serialiser = this.manager.getSerialiser();
        if (cache != null) {
            cache.write(name, value, serialiser);
        }
        else {
            cassandra.setAttribute(name, value, serialiser);
        }
    }
    public Object getAttribute(String name) {

        Object value = null;
        final Cache cache = Cache.getInstance();
        final SessionObjectSerialiser serialiser = this.manager.getSerialiser();
        if (cache != null) {
            value = cache.read(name, serialiser);
        }
        else {
            value = cassandra.getAttribute(name, serialiser);
        }
        if (debug) {
            if (value == null)
                cat.debug("["+id+"] Reading attribute "+name+" = "+value);
            else
                cat.debug("["+id+"] Reading attribute "+name+" = "+value.getClass().getName());
        }
        return value;
    }

    public void removeAttribute(String name) {
        if (debug) cat.debug("["+id+"] Removing attribute "+name);
        final Cache cache = Cache.getInstance();
        if (cache != null) {
            cache.remove(name);
        }
        else {
            cassandra.removeAttribute(name);
        }
    }
    
    public Enumeration getAttributeNames() {
        if (debug) cat.debug("["+id+"] Reading attribute names");
        List<String> objects = null;
        final Cache cache = Cache.getInstance();
        if (cache != null) {
            objects = cache.getEntries();
        }
        else {
            objects =  cassandra.getAttributes();
        }
        return new IteratorEnumeration(objects.iterator());
    }

    public void putValue(String name, Object value) {
        this.setAttribute(name, value);
    }

    public Object getValue(String name) {
        return this.getAttribute(name);
    }

    public void removeValue(String name) {
        this.removeAttribute(name);
    }

    public String[] getValueNames() {
        if (debug) cat.debug("["+id+"] Reading value names");
        List<String> objects = null;
        final Cache cache = Cache.getInstance();
        if (cache != null) {
            objects = cache.getEntries();
        }
        else {
            objects =  cassandra.getAttributes();
        }
        return objects.toArray(new String[objects.size()]);
    }

    public void setCreationTime(long creationTime) {
        cassandra.setCreationTime(creationTime);
    }

    public long getCreationTime() {
        return cassandra.getCreationTime();
    }

    public String getId() {
        return this.id;
    }

    public long getLastAccessedTime() {
        return cassandra.getLastAccessedTime();
    }

    public void setMaxInactiveInterval(int interval) {
        cassandra.setMaxInactiveInterval(interval);
    }

    public int getMaxInactiveInterval() {
        return cassandra.getMaxInactiveInterval();
    }

    public ServletContext getServletContext() {
        if (this.manager == null) return null;
        final Context context =  (Context)this.manager.getContainer();
        if (context == null) return null;
        return context.getServletContext();
    }

    public javax.servlet.http.HttpSessionContext getSessionContext() {
        cat.error("["+id+"] Call to deprecated method HttpSession.getSessionContext() !!!");
        return null;
    }

    public void invalidate() {
        cassandra.invalidate();
    }

    public void setNew(boolean isnew) {
        cassandra.setNew(isnew);
    }
    
    public boolean isNew() {
        return cassandra.isNew();
    }

    public long getLastAccessedTimeInternal() {
        return cassandra.getLastAccessedTimeInternal();
    }

    public long getSize() {
    	return cassandra.getSize();
    }

}
