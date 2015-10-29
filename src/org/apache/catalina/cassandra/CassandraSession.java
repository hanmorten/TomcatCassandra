// @(#)$Id: CassandraSession.java,v 1.8 2012/04/13 16:07:47 morten Exp $
package org.apache.catalina.cassandra;

import java.util.*;

import java.security.Principal;

import org.apache.catalina.cassandra.utils.*;
import org.apache.log4j.Logger;


/**
 * <ul>
 *   <li><b>Project:</b> TomcatCassandra</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.CassandraSession</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 15 Aug 2011 14:28:10</li>
 *   <li><b>Description:</b>
 *     Data access layer for a single Cassandra session.
 *     This sits on top of the basic Cassandra client.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
public class CassandraSession {

    /** Log4J logger instance for class CassandraSession. */
    private final static Logger cat = Logger.getLogger(CassandraSession.class);
    /** Log4J debug setting for class CassandraSession. */
    private final static boolean debug = cat.isDebugEnabled();

    /** Cassandra client API wrapper. */
    private CassandraClient client;
    
    /** Session ID. */
    private String sid;
    
    /**
     * Creates a new cassandra session container. The underlying session may
     * or may not exist.
     * @param client Cassandra client API wrapper.
     * @param sid Session ID..
     */
    CassandraSession(CassandraClient client, String sid) {
        this.client = client;
        this.sid = sid;
    }
    
    /** ================================================================== **/
    /** Tomcat Session accessor methods.                                   **/
    /** ================================================================== **/

    void setId(String sid) {
        if (debug) cat.debug("["+sid+"] Session ID set to "+sid);
        this.sid = sid;
    }

    /**
     * Returns the ID for this session.
     * @return Session ID.
     */
    public String getId() {
        return this.sid;
    }

    /**
     * Sets the session authentication type.
     * @param authType session authentication type.
     */
    void setAuthType(String authType) {
        this.client.setSessionValue(sid, "AuthType", authType);
    }

    /**
     * Obtains the session authentication type.
     * @return session authentication type.
     */
    String getAuthType() {
        return this.client.getSessionValueAsString(sid, "AuthType");
    }

    /**
     * Sets the pricipal (authenticated client identification).
     * @param principal the pricipal (authenticated client identification).
     */
    void setPrincipal(Principal principal, SessionObjectSerialiser serialiser) {
        this.client.addSessionObject(sid, "Principal", principal, serialiser);
    }

    /**
     * Obtains the pricipal (authenticated client identification).
     * @param classLoader Session manager's class loader.
     * @return the pricipal (authenticated client identification).
     */
    Principal getPrincipal(SessionObjectSerialiser serialiser) {
        return (Principal)this.client.getSessionObject(sid, "Principal", serialiser);
    }

    /**
     * Sets the session validity flag.
     * @param valid Session validity flag.
     */
    void setValid(boolean valid) {
        this.client.setSessionValue(sid, "Valid", Boolean.toString(valid));
    }

    /**
     * Returns the session validity flag.
     * @return the session validity flag.
     */
    boolean isValid() {
        final String valid = this.client.getSessionValueAsString(sid, "Valid");
        if (valid == null) return false;
        return Boolean.parseBoolean(valid);
    }

    /**
     * Expires the session.
     */
    void expire() {
        this.client.removeSession(sid);
    }

    /**
     * Recycles the session.
     */
    void recycle() {
        // TODO: Clear session state.
    }

    /**
     * Marks the session as accessed.
     */
    void access() {
        this.client.access(sid);
    }

    /**
     * Flags end of session access.
     */
    void endAccess() {
        this.client.endAccess(sid);
    }

    
    /** ================================================================== **/
    /** HTTP HttpServlet accessor methods.                                 **/
    /** ================================================================== **/

    /**
     * Sets/updates a session object.
     * @param name Session object name.
     * @param value Session object value.
     * @param serialiser Serialiser for session objects.
     */
    public void setAttribute(String name, Object value, SessionObjectSerialiser serialiser) {
        if (value == null) {
            this.removeAttribute(name);
        }
        else {
            if (debug) cat.debug("["+sid+"] Storing session object "+name+" / "+value.getClass().getName());
            this.client.addSessionObject(sid, name, value, serialiser);
        }
    }

    /**
     * Reads a session object.
     * @param name Session object name.
     * @param serialiser Serialiser for session objects.
     */
    public Object getAttribute(String name, SessionObjectSerialiser serialiser) {
        return this.client.getSessionObject(sid, name, serialiser);
    }

    /**
     * Removes a session name.
     * @param name Session object name.
     */
    public void removeAttribute(String name) {
        if (debug) cat.debug("["+sid+"] Removing session attribute "+name);
        this.client.removeSessionObject(sid, name);
    }

    /**
     * Obtains a list of all session object names.
     * @return List of session object names.
     */
    public List<String> getAttributes() {
        if (debug) cat.debug("Call to getAttributes() (heavy operation)", new Exception());
        return this.client.getSessionObjects(sid);
    }

    /**
     * Sets the session creation timestamp.
     * @param creationTime Session creation timestamp.
     */
    void setCreationTime(long creationTime) {
        this.client.setSessionValue(sid, "Created", creationTime);
    }

    /**
     * Returns the session creation timestamp.
     * @return the session creation timestamp.
     */
    long getCreationTime() {
        return this.client.getSessionValueAsLong(sid, "Created");
    }

    /**
     * Returns the session last accessed timestamp.
     * @return the session last accessed timestamp.
     */
    long getLastAccessedTime() {
        return this.client.getSessionValueAsLong(sid, "LastAccessed");
    }

    /**
     * Sets the session max inactive interval.
     * @param interval Session max inactive interval.
     */
    void setMaxInactiveInterval(int interval) {
        this.client.setSessionValue(sid, "MaxInactiveInterval", (long)interval);
    }

    /**
     * Returns the session max inactive interval.
     * @return the session max inactive interval.
     */
    int getMaxInactiveInterval() {
        return (int)this.client.getSessionValueAsLong(sid, "MaxInactiveInterval");
    }

    /**
     * Invalidates the session.
     */
    void invalidate() {
        this.setValid(false);
    }

    /**
     * Marks the session as new.
     * @param isnew Session "is new" flag.
     */
    void setNew(boolean isnew) {
        this.client.setSessionValue(sid, "New", Boolean.toString(isnew));
    }

    /**
     * Tells you if the session is new.
     * @return true if session is new.
     */
    boolean isNew() {
        final String isnew = this.client.getSessionValueAsString(sid, "New");
        if (isnew == null) return true;
        return Boolean.parseBoolean(isnew);
    }

    /**
     * Returns the last accessed time interval.
     * @return the last accessed time interval.
     */
    long getLastAccessedTimeInternal() {
        return this.client.getSessionValueAsLong(sid, "LastAccessInterval");
    }

    /** ================================================================== **/
    /** Extensions to our tomcat session manager.                          **/ 
    /** ================================================================== **/

    /**
     * Returns the size of the session data in bytes.
     * @return the size of the session data in bytes.
     */
    long getSize() {
    	return this.client.getSessionSize(sid);
    }

}
