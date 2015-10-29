// @(#)$Id: TomcatManager.java,v 1.9 2012/04/13 16:07:47 morten Exp $
package org.apache.catalina.cassandra;

import java.util.*;

import java.beans.PropertyChangeListener;
import java.io.IOException;

import org.w3c.dom.*;

import org.apache.catalina.Container;
import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.log4j.Logger;

import org.apache.catalina.cassandra.utils.SessionObjectSerialiser;

/**
 * <ul>
 *   <li><b>Project:</b> TomcatCassandra</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.TomcatManager</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 15 Aug 2011 11:32:39</li>
 *   <li><b>Description:</b>
 *     Cassandra session manager for Tomcat.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
@SuppressWarnings("deprecation")
public class TomcatManager implements Manager {
    
    /** Log4J logger instance for class TomcatManager. */
    private final static Logger cat = Logger.getLogger(TomcatManager.class);
    /** Log4J debug setting for class TomcatManager. */
    private final static boolean debug = cat.isDebugEnabled();

    private int maxActive;
    
    private int sessionIdLength = 32;
    
    private int sessionCount;
    
    private int expiredSessions;
    
    private int rejectedSessions;
    
    private int inactiveInterval;
    
    private int sessionAverageAliveTime;
    
    private int sessionMaxAliveTime;
    
    /** Timestamp for when we last tried to purge expired sessions. */
    private long lastPurged = 0L;
    
    /** Web application container. */
    private Container container;
    
    /**
     * Cassandra web application manager. This object is created on-demand
     * through the getCassandraManager() method, hence is should only be
     * accessed through that accessor method.
     * @see #getCassandraManager()
     */
    private CassandraManager manager;
    
    /**
     * Cassandra configuration container. This object is populated by the
     * bean-ish setter methods at the end of this class.
     * @see #setCluster(String)
     * @see #setHosts(String)
     * @see #setKeySpace(String)
     * @see #setUsername(String)
     * @see #setPassword(String)
     */
    private CassandraConfig config;
    
    /** Serialiser for session objects. */
    private SessionObjectSerialiser serialiser;
    
    /**
     * Creates a new Tomcat manager.
     */
    public TomcatManager() {
        this.config = new CassandraConfig();
        this.serialiser = new SessionObjectSerialiser(this); 
        CassandraStatistics.register(this);
    }
    
    /**
     * Returns the class loader for this web application.
     * @return Class loader for this web application.
     */
    public ClassLoader getClassLoader() {
        if (this.container == null) return null;
        if (this.container.getLoader() == null) return null;
        return this.container.getLoader().getClassLoader();
    }

    /**
     * Returns the session object serialiser instance.
     * @return the session object serialiser instance.
     */
    public SessionObjectSerialiser getSerialiser() {
        return this.serialiser;
    }

    /**
     * Returns the configuration container.
     * @return the configuration container.
     */
    public CassandraConfig getConfig() {
        return this.config;
    }
    
    /**
     * Obtains the Cassandra accesssor for this Tomcat web application manager.
     * @return the Cassandra accesssor for this Tomcat web application manager.
     */
    private CassandraManager getCassandraManager() {
        if (this.manager == null) {
            synchronized (this) {
                if (this.manager == null) {
                    this.manager = new CassandraManager(this);
                }
            }
        }
        return this.manager;
    }

    /* =============== SESSION MANAGEMENT METHODS ======================== */
    
    /**
     * Adds a new session.
     * @param session Session to be added.
     */
    public void add(Session session) {
        if (debug) cat.debug("Adding session "+session.getId());
        getCassandraManager().addSession(session.getId());
    }

    /**
     * Removes an existing session.
     * @param session Session to remove.
     */
    public void remove(Session session) {
        if (debug) cat.debug("Removing session "+session.getId());
        getCassandraManager().removeSession(session.getId());
    }

    /**
     * Creates a new Tomcat session.
     */
    public Session createEmptySession() {
    	final CassandraManager manager = this.getCassandraManager();
        final String id = CassandraManager.generateUniqueID();
        if (debug) cat.debug("Creating new empty session "+id);
        final CassandraSession cassandra = manager.addSession(id);
        return new TomcatSession(this, cassandra);
    }

    /**
     * Creates a new Tomcat session.
     */
    public Session createSession() {
    	final CassandraManager manager = this.getCassandraManager();
        final String id = CassandraManager.generateUniqueID();
        if (debug) cat.info("Creating new session "+id);
        final CassandraSession cassandra = manager.addSession(id);
        return new TomcatSession(this, cassandra);
    }

    /**
     * Creates a new Tomcat session.
     * @param id Session ID to use (or null).
     */
    public Session createSession(String id) {
    	final CassandraManager manager = this.getCassandraManager();
        if (id == null) id = CassandraManager.generateUniqueID();
        if (debug) cat.debug("Creating new session "+id);
        final CassandraSession cassandra = manager.addSession(id);
        return new TomcatSession(this, cassandra);
    }
    
    /**
     * Obtains an existing Tomcat session.
     * @param id Session ID.
     * @return Tomcat session.
     */
    public Session findSession(String id) throws IOException {
        final CassandraSession cassandra = this.getCassandraManager().getSession(id);
        return new TomcatSession(this, cassandra);
    }

    /**
     * Provides a list of all existing sessions. Hopefully Tomcat won't call
     * this method often, as the overhead against Cassandra is massive.
     * @return List of open sessions.
     */
    public Session[] findSessions() {
        if (debug) cat.debug("Obtaining ALL session IDs from Cassandra.");

        final List<Session> sessions = new ArrayList<Session>();

        final List<String> ids = getCassandraManager().getSessions();
        for (int i=0; i<ids.size(); i++) {
            final String id = ids.get(i);
            final CassandraSession cassandra = this.getCassandraManager().getSession(id);
            sessions.add(new TomcatSession(this, cassandra));
        }
        return sessions.toArray(new Session[sessions.size()]);
    }

    /**
     * Returns the number of active sessions.
     * @return the number of active sessions.
     */
    public int getActiveSessions() {
        return getCassandraManager().getSessionCount();
    }

    /* =============== TOMCAT MANAGER IMPLEMENTATION ===================== */
    
    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#setSessionIdLength(int)
     */
    public void setSessionIdLength(int length) {
        this.sessionIdLength = length;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#getSessionIdLength()
     */
    public int getSessionIdLength() {
        if (this.sessionIdLength == 0) return 32;
        return this.sessionIdLength;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#setSessionCounter(int)
     */
    public void setSessionCounter(int count) {
        this.sessionCount = count;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#getSessionCounter()
     */
    public int getSessionCounter() {
        return this.sessionCount;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#setExpiredSessions(int)
     */
    public void setExpiredSessions(int count) {
        this.expiredSessions = count;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#getExpiredSessions()
     */
    public int getExpiredSessions() {
        return this.expiredSessions;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#setRejectedSessions(int)
     */
    public void setRejectedSessions(int count) {
        this.rejectedSessions = count;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#getRejectedSessions()
     */
    public int getRejectedSessions() {
        return this.rejectedSessions;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#setMaxActive(int)
     */
    public void setMaxActive(int max) {
        this.maxActive = max;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#getMaxActive()
     */
    public int getMaxActive() {
        return this.maxActive;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#setMaxInactiveInterval(int)
     */
    public void setMaxInactiveInterval(int interval) {
        this.inactiveInterval = interval;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#getMaxInactiveInterval()
     */
    public int getMaxInactiveInterval() {

        return this.inactiveInterval;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#setSessionAverageAliveTime(int)
     */
    public void setSessionAverageAliveTime(int time) {
        this.sessionAverageAliveTime = time;
    }

    /*ession max inactive interval.
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#getSessionAverageAliveTime()
     */
    public int getSessionAverageAliveTime() {
        return this.sessionAverageAliveTime;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#setSessionMaxAliveTime(int)
     */
    public void setSessionMaxAliveTime(int time) {
        this.sessionMaxAliveTime = time;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#getSessionMaxAliveTime()
     */
    public int getSessionMaxAliveTime() {
        return this.sessionMaxAliveTime;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#setDistributable(boolean)
     */
    public void setDistributable(boolean isDistributable) {
        
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#getDistributable()
     */
    public boolean getDistributable() {
        return false;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#setContainer(org.apache.catalina.Container)
     */
    public void setContainer(Container container) {
        this.container = container;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#getContainer()
     */
    public Container getContainer() {
        return this.container;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#getInfo()
     */
    public String getInfo() {
        return "Tomcat Cassandra Manager/1.1";
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#load()
     */
    public void load() throws ClassNotFoundException, IOException {
        cat.info("Cassandra session manager loaded.");
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#unload()
     */
    public void unload() throws IOException {
        cat.info("Cassandra session manager unloaded");
        this.getCassandraManager().unload();
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#removePropertyChangeListener(java.beans.PropertyChangeListener)
     */
    public void removePropertyChangeListener(PropertyChangeListener listener) {
        cat.error("Property change listeners are not supported!");
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#addPropertyChangeListener(java.beans.PropertyChangeListener)
     */
    public void addPropertyChangeListener(PropertyChangeListener listener) {
        cat.error("Property change listeners are not supported!");
    }

    /*
     * (non-Javadoc)
     * @see org.apache.catalina.Manager#backgroundProcess()
     */
    public void backgroundProcess() {
    	final long now = System.currentTimeMillis();
    	if ((lastPurged + (this.config.getPurgeInterval() * 1000L)) < now) {
    		lastPurged = System.currentTimeMillis();
    		this.closeExpiredSessions();
        }
    }
    
    /**
     * Purges all expired sessions from Cassandra.
     */
    private void closeExpiredSessions() {
    	if (debug) cat.debug("Searching for expired sessions...");
    	try {
    		final List<String> sessions = getCassandraManager().getExpiredSessions();
    		if (debug) cat.debug("Closing expired sessions "+sessions);
    		for (String sid : sessions) {
    			try {
    				getCassandraManager().removeSession(sid);
    			}
    			catch (Throwable e) {
    				cat.error("Error purging expired session "+sid+": "+e.getMessage(), e);
    			}
    		}
		}
		catch (Throwable e) {
			cat.error("Unable to find expired sessions: "+e.getMessage(), e);
		}
    }
    
    
    public void changeSessionId(Session session) {
    	/*
        final CassandraManager manager = this.getCassandraManager();
        final String id = CassandraManager.generateUniqueID();
        session.setId(id);
        */
    }

    /* =============== CONFIGURATION PARAMETER SETTERS =================== */

    /**
     * Sets the host-list configuration parameter.
     * @param hosts List of host/port pairs.
     */
    public void setHosts(String hosts) {
        this.config.setHosts(hosts);
    }

    /**
     * Sets the name of the Cassandra cluster to connect to.
     * @param cluster the name of the Cassandra cluster to connect to.
     */
    public void setCluster(String cluster) {
        this.config.setCluster(cluster);
    }

    /**
     * Sets the name of the Cassandra keyspace to use.
     * @param keyspace the Cassandra keyspace to use.
     */
    public void setKeySpace(String keyspace) {
        this.config.setKeySpace(keyspace);
    }

    /**
     * Sets the username required to authenticate to Cassandra.
     * @param username the username required to authenticate to Cassandra.
     */
    public void setUsername(String username) {
        this.config.setUsername(username);
    }
    
    /**
     * Sets the password required to authenticate to Cassandra.
     * @param password the password required to authenticate to Cassandra.
     */
    public void setPassword(String password) {
        this.config.setPassword(password);
    }
 
    /**
     * Sets the consistency level to use for read operations.
     * @param level the consistency level to use for read operations.
     */
    public void setReadConsistency(String level) {
        this.config.setReadConsistencyLevel(level);
    }
    
    /**
     * Sets the consistency level to use for write operations.
     * @param level the consistency level to use for write operations.
     */
    public void setWriteConsistency(String level) {
        this.config.setWriteConsistencyLevel(level);
    }
    
    /**
     * Sets the compression level for persisted objects.
     * @param level Compression level for persisted objects ("BEST" gives best
     *    compression, "SPEED" gives best performance, "DEFAULT" provides
     *    balanced compression/speed, and "NONE" gives no compression).
     */
    public void setCompressionLevel(String level) {
        this.config.setCompressionLevel(level);
    }

    /**
     * Sets the session timeout in seconds.
     * @param timeout The session timeout in seconds.
     */
    public void setTimeout(long timeout) {
    	this.config.setTimeout(timeout);
    }

    /**
     * Sets the database purge interval in seconds.
     * @param interval Purge interval in seconds.
     */
    public void setPurgeInterval(long interval) {
        this.config.setPurgeInterval(interval);
    }
    
    /**
     * Generates an XML element that contains the list of active sessions,
     * their session IDs and size. The session size is the number of bytes as
     * stored in Cassandra, and does <u>NOT</u> reflect the session size in
     * memory.
     * @param doc Document used to create XML elements.
     * @return session ID/size element.
     */
    public Element getSessionSizes(Document doc) {
        final Element _sessions = doc.createElement("Sessions");
        
        final List<String> ids = getCassandraManager().getSessions();
        for (int i=0; i<ids.size(); i++) {
            final String id = ids.get(i);
            final CassandraSession session = this.getCassandraManager().getSession(id);
            final long size = session.getSize();
            final Element _session = doc.createElement("Session");
            _session.setAttribute("ID", id);
            _session.setAttribute("Size", Long.toString(size));
            _sessions.appendChild(_session);
        }
        
        return _sessions;
    }
    
}
