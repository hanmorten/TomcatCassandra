// @(#)$Id: CassandraManager.java,v 1.6 2012/04/13 16:07:47 morten Exp $
package org.apache.catalina.cassandra;

import java.util.*;

import org.apache.log4j.Logger;

import org.apache.catalina.Container;
import org.apache.catalina.Session;

/**
 * <ul>
 *   <li><b>Project:</b> TomcatCassandra</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.CassandraSession</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 15 Aug 2011 14:28:10</li>
 *   <li><b>Description:</b>
 *     Data access layer for cassandra sessions, providing access to session
 *     data stored in cassandra. The TomcatManager maps the Tomcat "Manager"
 *     interface to this class.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
public class CassandraManager {

    /** Log4J logger instance for class CassandraSession. */
    private final static Logger cat = Logger.getLogger(CassandraManager.class);
    /** Log4J debug setting for class CassandraSession. */
    private final static boolean debug = cat.isDebugEnabled();

    /** Cassandra accessor provider. */
    private CassandraClient client;
    /** Tomcat session manager. */
    private TomcatManager tomcat;
    
    /**
     * Creates a new cassandra manager.
     */
    public CassandraManager(TomcatManager tomcat) {
        this.tomcat = tomcat;
        this.client = new CassandraClient(tomcat.getConfig());
    }
    
    /**
     * Generates a new unique session ID.
     * @param length session ID length (in characters).
     * @return New unique session id.
     */
    static String generateUniqueID() {
        final UUID uuid = UUID.randomUUID();
        return "OJ" + uuid.toString();
    }
    
    /**
     * Adds a new session.
     * @param sid Session ID.
     * @return Session instance.
     */
    CassandraSession addSession(String sid) {
        client.addSession(sid);
        return new CassandraSession(client, sid);
    }

    /**
     * Adds a new session.
     * @return Session instance.
     */
   public CassandraSession addSession() {
        final String sid = CassandraManager.generateUniqueID();
        client.addSession(sid);
        return new CassandraSession(client, sid);
    }

    /**
     * Removes an existing session.
     * @param sid Session ID.
     */
    public void removeSession(String sid) {
        client.removeSession(sid);
    }

    /**
     * Obtains an existing session.
     * @param sid Session ID.
     * @return Session instance.
     */
    public CassandraSession getSession(String sid) {
        return new CassandraSession(client, sid);
    }
    
    /**
     * Provides a list of all existing sessions.
     * This is a heavy operation and should not be used!
     * @return list of session IDs.
     */
    List<String> getSessions() {
        return client.getSessions();
    }

    /**
     * Provides a list of expird sessions.
     * @return list of IDs of expired sessions.
     */
    List<String> getExpiredSessions() {
        return client.getExpiredSessions();
    }

    /**
     * Returns the number of sessions.
     * @return the number of sessions.
     */
    public int getSessionCount() {
        return client.getSessionCount();
    }

    /**
     * Shuts down this cassandra manager.
     */
    public void unload() {
        this.client.shutdown();
    }
    
}
