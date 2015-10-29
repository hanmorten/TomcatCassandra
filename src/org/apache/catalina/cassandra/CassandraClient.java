// @(#)$Id: CassandraClient.java,v 1.7 2012/04/13 16:07:47 morten Exp $
package org.apache.catalina.cassandra;

import java.util.*;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.*;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;

import org.apache.catalina.cassandra.utils.SessionObjectSerialiser;

import org.apache.log4j.Logger;

/**
 * <ul>
 *   <li><b>Project:</b> TomcatCassandra</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.CassandraClient</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 15 Aug 2011 15:16:29</li>
 *   <li><b>Description:</b>
 *     Provdes accessor methods for data that is contained in cassandra.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
class CassandraClient {

    /** Log4J logger instance for class CassandraClient. */
    private final static Logger cat = Logger.getLogger(CassandraClient.class);
    /** Log4J debug setting for class CassandraClient. */
    private final static boolean debug = cat.isDebugEnabled();

    /** Cassandra configuration container. */
    protected CassandraConfig config;
    
    /** Cassandra cluster reference. */
    protected Cluster cluster;

    /** Cassandra keyspace reference. */
    private Keyspace keyspace = null;
    
    /** Name of key used to store sessions. */
    private static final String SESSIONS = "sessions";
    /** Name of key used to store session objects. */
    private static final String SESSIONOBJECTS = "sessionobjects";
    
    /**
     * Creates a new cassandra accessor object.
     * @param config Cassandra configuration container.
     */
    public CassandraClient(CassandraConfig config) {
        this.config = config;

        cat.info("Cassandra session manager connecting to cluster \""+config.getCluster()+"\" via "+config.getHosts());
        final CassandraHostConfigurator hosts = new CassandraHostConfigurator();
        hosts.setAutoDiscoverHosts(true);
        hosts.setAutoDiscoveryDelayInSeconds(60);
        hosts.setHosts(config.getHosts());
        this.cluster = HFactory.getOrCreateCluster(config.getCluster(), hosts);
    }

    /**
     * Returns the configuration container for the Cassandra session manager.
     * @return the configuration container for the Cassandra session manager.
     */
    public CassandraConfig getConfig() {
        return this.config;
    }
    
    /**
     * Shuts down the cassandra client.
     */
    public void shutdown() {
        this.cluster.getConnectionManager().shutdown();
    }

    /**
     * Creates a reference to the Cassandra keyspace. The keyspace, and the
     * necessary credentials to authenticate to this keyspace are all
     * configured in the CassandraConfig class passed to the constructor of
     * this object. A username/password can (should) be configured for
     * authentication against the Cassandra keyspace.
     * @return reference to the Cassandra keyspace.
     */
    private Keyspace createKeyspace() {

        // Get the consistency level from the configuration, allowing the
        // administrator to set the consistency levels for read and write
        // operations independently of each other.
        final ConfigurableConsistencyLevel consistency  = new ConfigurableConsistencyLevel();
        // Default is LOCAL_QUORUM for read operations.
        consistency.setDefaultReadConsistencyLevel(config.getReadConsistencyLevel());
        // Default is EACH_QUORUM for read operations.
        consistency.setDefaultWriteConsistencyLevel(config.getWriteConsistencyLevel());
        
        // Failover policy is to attempt the write up to 3 times, waiting
        // 250ms between each retry.
        final FailoverPolicy failover = new FailoverPolicy(3, 250);

        // Attempt to connect to the keyspace anonymously.
        if (config.getUsername() == null || config.getPassword() == null) {
        	cat.info("Cassandra username/password is not set. Attempting to connect to cluster anonymously. Consider setting the @username and @password attributes in the web application descriptor.");
        	return HFactory.createKeyspace(config.getKeySpace(), cluster, consistency, failover);
        }
        // Use username/password for keyspace authentication (if provided).
        else {
        	final Map<String,String> credentials = new HashMap<String,String>();
        	credentials.put("user", config.getUsername());
        	credentials.put("username", config.getUsername());
        	credentials.put("password", config.getPassword());
        	return HFactory.createKeyspace(config.getKeySpace(), cluster, consistency, failover, credentials);
        }
    }

    private Keyspace getKeyspace() {
        if (this.keyspace != null) return keyspace;
        synchronized (this) {
            if (this.keyspace != null) return keyspace;
            this.keyspace = this.createKeyspace();
            return this.keyspace;
        }
    }
    
    /**
     * Creates a mutator for a cluster and the configured keyspace.
     * @param cluster Cassandra cluster.
     * @return Mutator.
     */
    private Mutator<String> getMutator(Cluster cluster) {
        final Keyspace keyspace = getKeyspace();
        final StringSerializer str = StringSerializer.get();
        return HFactory.createMutator(keyspace, str);
    }

    /**
     * Executes/commits a mutator.
     * @param mutator Mutator to commit.
     */
    private void commitMutator(Mutator<String> mutator) {
        mutator.execute();
    }

    /**
     * Inserts a string name/value pair.
     * @param mutator Mutator to add insert to.
     * @param colfamily Column family.
     * @param key Parent key.
     * @param name Name.
     * @param value Value as a string.
     */
    private void insert(Mutator<String> mutator, String colfamily, String key, String name, String value) {
        final StringSerializer str = StringSerializer.get();
        final HColumn<String, String> column = HFactory.createColumn(name, value, str, str);
        //column.setTtl((int)this.ttl);
        mutator.addInsertion(key, colfamily, column);
    }

    /**
     * Inserts an Object name/value pair.
     * @param mutator Mutator to add insert to.
     * @param colfamily Column family.
     * @param key "Row" key (session ID).
     * @param name Name.
     * @param value Value as an object.
     * @param serialiser Object serialiser to use.
     */
    private void insert(Mutator<String> mutator, String colfamily, String key, String name, Object value, SessionObjectSerialiser serialiser) {
        final StringSerializer str = StringSerializer.get();
        final HColumn<String,Object> column = HFactory.createColumn(name, value, str, serialiser);
        mutator.addInsertion(key, colfamily, column);
    }

    /**
     * Inserts a long name/value pair.
     * @param mutator Mutator to add insert to.
     * @param colfamily Column family.
     * @param key "Row" key (session ID).
     * @param name Name.
     * @param value Value as a long.
     */
    private void insert(Mutator<String> mutator, String colfamily, String key, String name, long value) {
        final StringSerializer str = StringSerializer.get();
        final LongSerializer lng = LongSerializer.get();
        final HColumn<String, Long> column = HFactory.createColumn(name, value, str, lng);
        //column.setTtl((int)this.ttl);
        mutator.addInsertion(key, colfamily, column);
    }

    /**
     * Removes a key (session ID).
     * @param mutator Mutator to add delete to.
     * @param colfamily Column family.
     * @param key "Row" key (session ID).
     */
    private void remove(Mutator<String> mutator, String colfamily, String key) {
        mutator.addDeletion(key, colfamily);
    }

    /**
     * Adds a new session to the cassandra cache.
     * @param sid Session ID.
     */
    public void addSession(String sid) {
        final long now = System.currentTimeMillis();
        final Mutator<String> mutator = this.getMutator(cluster);
        this.insert(mutator, SESSIONS, sid, "ID", sid);
        this.insert(mutator, SESSIONS, sid, "New", "true");
        this.insert(mutator, SESSIONS, sid, "Valid", "true");
        this.insert(mutator, SESSIONS, sid, "Created", now);
        this.insert(mutator, SESSIONS, sid, "LastAccessed", now);
        this.insert(mutator, SESSIONS, sid, "LastEndAccessed", now);
        this.insert(mutator, SESSIONS, sid, "LastAccessInterval", 0L);
        this.insert(mutator, SESSIONS, sid, "MaxInactiveInterval", 0L);
        this.insert(mutator, SESSIONS, sid, "Sentinel", 0L);
        this.commitMutator(mutator);
    }
    
    /**
     * Updates session data (not attributes) to mark access to the session.
     * @param sid Session ID.
     */
    public void access(String sid) {
        final long last = this.getSessionValueAsLong(sid, "LastEndAccessed");
        final long now = System.currentTimeMillis();
        final Mutator<String> mutator = this.getMutator(cluster);
        this.insert(mutator, SESSIONS, sid, "LastAccessed", now);
        this.insert(mutator, SESSIONS, sid, "LastAccessInterval", (now - last));
        if (debug) {
            this.insert(mutator, SESSIONS, sid, "LastAccessedStamp", this.getDateTime());
        }
        this.commitMutator(mutator);
    }

    private String getDateTime() {
        final Calendar cal = Calendar.getInstance();
        final StringBuffer buf = new StringBuffer();
        buf.append(cal.get(Calendar.YEAR));
        buf.append("-");
        int month =  cal.get(Calendar.MONTH) + 1;
        if (month < 10) buf.append("0");
        buf.append(month);
        buf.append("-");
        int day =  cal.get(Calendar.DAY_OF_MONTH);
        if (day < 10) buf.append("0");
        buf.append(day);
        buf.append("T");
        int hour =  cal.get(Calendar.HOUR_OF_DAY);
        if (hour < 10) buf.append("0");
        buf.append(hour);
        buf.append(":");
        int min =  cal.get(Calendar.MINUTE);
        if (min < 10) buf.append("0");
        buf.append(min);
        buf.append(":");
        int sec =  cal.get(Calendar.SECOND);
        if (sec < 10) buf.append("0");
        buf.append(sec);
        return buf.toString();
    }
    
    /**
     * Updates session data (not attributes) to mark access to the session.
     * This stored access timestamps and also updates the TTLs on the
     * columns that hold the session data, so that parts of the session won't
     * expire in Cassandra while the web session is actually still live.
     * @param sid Session ID.
     */
    public void endAccess(String sid) {
        final long created = this.getSessionValueAsLong(sid, "Created");
        final long now = System.currentTimeMillis();
        final Mutator<String> mutator = this.getMutator(cluster);
        this.insert(mutator, SESSIONS, sid, "ID", sid);
        this.insert(mutator, SESSIONS, sid, "New", "false");
        this.insert(mutator, SESSIONS, sid, "Valid", "true");
        this.insert(mutator, SESSIONS, sid, "Created", created);
        this.insert(mutator, SESSIONS, sid, "LastAccessed", now);
        this.insert(mutator, SESSIONS, sid, "LastEndAccessed", now);
        this.commitMutator(mutator);
    }
    
    /**
     * Stores a session value for a given session. These are not session
     * objects that are managed by the application, but internal values
     * that are used by Tomcat to manage the session.
     * @param sid Session ID.
     * @param name Name.
     * @param value Value.
     */
    public void setSessionValue(String sid, String name, String value) {
        final Mutator<String> mutator = this.getMutator(cluster);
        this.insert(mutator, SESSIONS, sid, name, value);
        this.commitMutator(mutator);
    }

    /**
     * Stores a session value for a given session. These are not session
     * objects that are managed by the application, but internal values
     * that are used by Tomcat to manage the session.
     * @param sid Session ID.
     * @param name Name.
     * @param value Value.
     */
    public void setSessionValue(String sid, String name, long value) {
        final Mutator<String> mutator = this.getMutator(cluster);
        this.insert(mutator, SESSIONS, sid, name, value);
        this.commitMutator(mutator);
    }
    
    /**
     * Obtains a session value for a given session. These are not session
     * objects that are managed by the application, but internal values
     * that are used by Tomcat to manage the session.
     * @param sid Session ID.
     * @param name Name.
     * @return value.
     */
    public String getSessionValueAsString(String sid, String name) {
        final Keyspace keyspace = this.getKeyspace();
        final StringSerializer str = StringSerializer.get();

        // Build the query.
        final ColumnQuery<String, String, String> query =
            HFactory.createColumnQuery(keyspace, str, str, str);
        query.setColumnFamily(SESSIONS);
        query.setKey(sid);
        query.setName(name);
        
        // Parse the results.
        final QueryResult<HColumn<String, String>> result = query.execute();
        final HColumn<String,String> col = result.get();
        if (col == null) return null;
        return result.get().getValue();
    }

    /**
     * Obtains a session value for a given session. These are not session
     * objects that are managed by the application, but internal values
     * that are used by Tomcat to manage the session.
     * @param sid Session ID.
     * @param name Name.
     * @return value.
     */
    public long getSessionValueAsLong(String sid, String name) {
        final Keyspace keyspace = this.getKeyspace();
        final StringSerializer str = StringSerializer.get();
        final LongSerializer lng = LongSerializer.get();

        // Build the query.
        final ColumnQuery<String, String, Long> query =
            HFactory.createColumnQuery(keyspace, str, str, lng);
        query.setColumnFamily(SESSIONS);
        query.setKey(sid);
        query.setName(name);
        
        // Parse the results.
        final QueryResult<HColumn<String, Long>> result = query.execute();
        final HColumn<String,Long> col = result.get();
        if (col == null) return 0L;
        return result.get().getValue().longValue();
    }

    /**
     * Removes an existing session from the cassandra cache.
     * Note that all session objects, conversations and conversation objects
     * are also removed.
     * @param sid Session ID.
     */
    public void removeSession(String sid) {
        final Mutator<String> mutator = this.getMutator(cluster);
        this.remove(mutator, SESSIONS, sid);
        this.remove(mutator, SESSIONOBJECTS, sid);
        this.commitMutator(mutator);
    }

    /**
     * Returns the number of sessions.
     * @return the number of sessions.
     */
    public int getSessionCount() {
        final Keyspace keyspace = getKeyspace();
        final StringSerializer str = StringSerializer.get();

        final CountQuery<String,String> query =
            HFactory.createCountQuery(keyspace, str, str);
        query.setColumnFamily(SESSIONS);
        
        final QueryResult<Integer> result = query.execute();
        return result.get().intValue();
    }
    
    /**
     * Obtains a list of all sessions. This is a very heavy operation and it
     * should be avoided at all costs.
     * @return List of active sessions.
     */
    public List<String> getSessions() {
        // List to store session IDs in.
        final List<String> sessions = new ArrayList<String>();

        final Keyspace keyspace = getKeyspace();
        final StringSerializer str = StringSerializer.get();

        // Initialise the query.
        final RangeSlicesQuery<String, String, String> query =
            HFactory.createRangeSlicesQuery(keyspace, str, str, str);
        query.setColumnFamily(SESSIONS);
        query.setKeys("", "");
        query.setRange("", "", false, Integer.MAX_VALUE);
        query.setReturnKeysOnly();
        
        // Iterate over all sessions in the results.
        final QueryResult<OrderedRows<String, String, String>> result = query.execute();
        final List<Row<String,String,String>> rows = result.get().getList();
        for (int i=0; i<rows.size(); i++) {
            final Row<String,String,String> row = rows.get(i);
            sessions.add(row.getKey());
        }
        
        return sessions;
    }

    /**
     * Obtains the IDs of all expired sessions.
     * @return List of expired sessions.
     */
    public List<String> getExpiredSessions() {
        // List to store IDs of expired sessions in.
        final List<String> sessions = new ArrayList<String>();
        // Get the configured keyspace.
        final Keyspace keyspace = getKeyspace();
        // Get serializers for the various column names/values
        final StringSerializer str = StringSerializer.get();
        final LongSerializer lng = LongSerializer.get();
        // Figure out what the timestamp for expired sessions must be older than.
        final long expired = System.currentTimeMillis() - this.config.getTimeout() * 1000;

        // Build the query to find expired sessions.
        final RangeSlicesQuery<String, String, Long> query =
        		HFactory.createRangeSlicesQuery(keyspace, str, str, lng);
        query.addLtExpression("LastAccessed", expired);
        query.addEqualsExpression("Sentinel", 0L);
        query.setColumnFamily(SESSIONS);
        query.setRange(null, null, true, Integer.MAX_VALUE);
        
        // We don't care about the data, we'll only need the keys.
        query.setReturnKeysOnly();
        
        // Iterate over all sessions in the results.
        final QueryResult<OrderedRows<String, String, Long>> result = query.execute();
        final List<Row<String,String,Long>> rows = result.get().getList();
        for (int i=0; i<rows.size(); i++) {
            final Row<String,String,Long> row = rows.get(i);
            sessions.add(row.getKey());
        }
        
        return sessions;
    }

    /**
     * Adds a session object.
     * @param sid Session ID.
     * @param name Session object name.
     * @param value Session object value.
     */
    public void addSessionObject(String sid, String name, Object object, SessionObjectSerialiser serialiser) {
        final long start = System.currentTimeMillis();

        final Mutator<String> mutator = this.getMutator(cluster);
        this.insert(mutator, SESSIONOBJECTS, sid, name, object, serialiser);
        this.commitMutator(mutator);

        final CassandraStatistics stats = CassandraStatistics.getInstance(); 
        if (stats != null) {
            final long stop = System.currentTimeMillis();
            final long duration = stop - start;
            stats.setLastDuration(duration);
            stats.write(name);
        }
    }

    /**
     * Obtains a session object value.
     * @param sid Session ID.
     * @param name Session object name.
     * @return Session object value.
     */
    public Object getSessionObject(String sid, String name, SessionObjectSerialiser serialiser) {
        final long start = System.currentTimeMillis();

        final Keyspace keyspace = this.getKeyspace();
        final StringSerializer str = StringSerializer.get();

        // Build the query.
        final ColumnQuery<String, String, Object> query =
            HFactory.createColumnQuery(keyspace, str, str, serialiser);
        query.setColumnFamily(SESSIONOBJECTS);
        query.setKey(sid);
        query.setName(name);
        
        // Parse the results.
        final QueryResult<HColumn<String, Object>> result = query.execute();
        final HColumn<String,Object> col = result.get();
        if (col == null) return null;
        final Object object = result.get().getValue();

        final CassandraStatistics stats = CassandraStatistics.getInstance(); 
        if (stats != null) {
            final long stop = System.currentTimeMillis();
            final long duration = stop - start;
            stats.setLastDuration(duration);
            stats.read(name);
        }
        
        return object;
    }
    
    /**
     * Removes a session object.
     * @param sid Session ID.
     * @param name Session object name.
     */
    public void removeSessionObject(String sid, String name) {
        final Mutator<String> mutator = this.getMutator(cluster);
        final StringSerializer str = StringSerializer.get();
        mutator.addDeletion(sid, SESSIONOBJECTS, name, str);
        this.commitMutator(mutator);
    }

    /**
     * Provides a list of all session objects for a given session.
     * @param sid Session ID.
     * @return List of session object names.
     */
    public List<String> getSessionObjects(String sid) {
        // Container to store results in.
        final List<String> objects = new ArrayList<String>();

        final Keyspace keyspace = this.getKeyspace();
        final StringSerializer str = StringSerializer.get();

        // Build the query.
        final SliceQuery<String, String, String> query =
            HFactory.createSliceQuery(keyspace, str, str, str);
        query.setColumnFamily(SESSIONOBJECTS);
        query.setKey(sid);
        query.setRange("", "", false, Integer.MAX_VALUE);
        final QueryResult<ColumnSlice<String, String>> result = query.execute();

        // Parse the results.
        final List<HColumn<String,String>> columns = result.get().getColumns();
        for (int i=0; i<columns.size(); i++) {
            final HColumn<String,String> column = columns.get(i);
            objects.add(column.getName());
        }
        
        return objects;
    }


    /**
     * Obtains size of a session object as stored in the DB.
     * @param sid Session ID.
     * @param name Session object name.
     * @return size of session object as stord in the DB.
     */
    public long getSessionObjectSize(String sid, String name) {
        final Keyspace keyspace = this.getKeyspace();
        final StringSerializer str = StringSerializer.get();

        final BytesArraySerializer serialiser = BytesArraySerializer.get();
        
        // Build the query.
        final ColumnQuery<String, String, byte[]> query =
            HFactory.createColumnQuery(keyspace, str, str, serialiser);
        query.setColumnFamily(SESSIONOBJECTS);
        query.setKey(sid);
        query.setName(name);
        
        // Parse the results.
        final QueryResult<HColumn<String, byte[]>> result = query.execute();
        final HColumn<String,byte[]> col = result.get();
        if (col == null) return 0L;
        final byte[] object = result.get().getValue();
        if (object == null) return 0L;
        // TTL adds an extra 8 bytes to the column.
        return (long)object.length;// + 8L;
    }
    
    /**
     * Provides the overall size of a session in bytes. Note that the returned
     * size includes the serialised object size only, and does NOT include
     * indexing overhead, but does include storage overhead. The value
     * returned by this method can be used to extrapolate the  overall session
     * size using Cassandra documentation. See the "Capacity Planning" section
     * in:
     * <a href="http://www.datastax.com/docs/0.8/cluster_architecture/cluster_planning">
     *   Cassandra Cluster Planning
     * </a>
     * @param sid Session ID.
     * @return size of session data in bytes.
     */
    public long getSessionSize(String sid) {
    	
    	// Cassandra has 15 bytes of column overhead...
    	long size = 15L;
    	
        final Keyspace keyspace = this.getKeyspace();
        final StringSerializer str = StringSerializer.get();

        // Build the query.
        final SliceQuery<String, String, String> query =
            HFactory.createSliceQuery(keyspace, str, str, str);
        query.setColumnFamily(SESSIONOBJECTS);
        query.setKey(sid);
        query.setRange("", "", false, Integer.MAX_VALUE);
        final QueryResult<ColumnSlice<String, String>> result = query.execute();

        // Parse the results.
        final List<HColumn<String,String>> columns = result.get().getColumns();
        for (int i=0; i<columns.size(); i++) {
            final HColumn<String,String> column = columns.get(i);
            final String name = column.getName();
            // Get the size of the column name.
            try {
            	// We assume that Cassandra uses UTF-8.
            	// I am not sure if this assumption is valid...
            	size += name.getBytes("UTF-8").length;
            }
            catch (Throwable e) {
            	// ignore... this will never fail!
            }
            // Get the size of the value (in bytes).
            size += this.getSessionObjectSize(sid, name);
        }
        
        return size;
    }    
}