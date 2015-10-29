// @(#)$Id: CassandraConfig.java,v 1.5 2012/04/13 16:07:47 morten Exp $
package org.apache.catalina.cassandra;

import java.util.zip.Deflater;
import org.apache.log4j.Logger;

import me.prettyprint.hector.api.HConsistencyLevel;

/**
 * <ul>
 *   <li><b>Project:</b> TomcatCassandra</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.CassandraConfig</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 15 Aug 2011 15:13:57</li>
 *   <li><b>Description:</b>
 *     Container for cassandra endpoint config and access credentials.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
public class CassandraConfig {

    /** Log4J logger instance for class CassandraConfig. */
    private final static Logger cat = Logger.getLogger(CassandraConfig.class);
    /** Log4J debug setting for class CassandraConfig. */
    private final static boolean debug = cat.isDebugEnabled();

    /** Comma-separated lists of host/port to connect to. */
    private String hosts;
    /** Cluster name. */
    private String cluster;
    /** Keyspace to use (default is "xrez"). */
    private String keyspace;
    /** Username for authentication. */
    private String username;
    /** Password for authentication. */
    private String password;
    /** Read operation consistency level. */
    private HConsistencyLevel readConsistencyLevel = HConsistencyLevel.LOCAL_QUORUM;
    /** Write operation consistency level. */
    private HConsistencyLevel writeConsistencyLevel = HConsistencyLevel.EACH_QUORUM;
    /** Compression level for persisted objects. */
    private int compressionLevel = -1;

    /**
     * Time-to-live for columns in Cassandra, which for us is the same as the
     * session expiry timeout.
     */
    private long timeout = 30 * 60; // Default is 30 minutes
    
    /**
     * Frequency for when we scan the database for expired sessions,
     * given in seconds.
     * We purge expired sessions every minute. This process is based on
     * calls from Tomcat to the backgroundProcess() method, which can
     * happen quite frequently. We don't want to hammer Cassandra with
     * (the relatively heavy) requests for expired sessions, so we only
     * run the session purge process every minute.
     */
    private long purgeInterval = 60; // Default is 60 seconds
    
    /**
     * Creates a new Cassandra configuration container.
     */
    CassandraConfig() {
        
    }

    /**
     * Creates a new Cassandra configuration container.
     * @param hosts  Comma-separated list of host/port to connect to.
     * @param cluster Name of Cassandra cluster.
     * @param keyspace Name of keyspace that contains data.
     * @param username Username for authentication (optional).
     * @param password Password for authentication (optional).
     */
    public CassandraConfig(String hosts, String cluster, String keyspace, String username, String password) {
        this.hosts = hosts;
        this.cluster = cluster;
        this.keyspace = keyspace;
        this.username = username;
        this.password = password;
    }

    /**
     * Sets the Cassandra hosts list.
     * @param hosts Cassandra server host names/ports.
     */
    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    /**
     * Returns the Cassandra server host list.
     * @return the Cassandra server host list.
     */
    public String getHosts() {
        return this.hosts;
    }

    /**
     * Sets the Cassandra cluster name.
     * @param cluster Cassandra cluster name.
     */
    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    /**
     * Returns the Cassandra cluster name.
     * @return the Cassandra cluster name.
     */
    public String getCluster() {
        return this.cluster;
    }

    /**
     * Sets the name of the keyspace to use. 
     * @param keyspace Name of keyspace to use.
     */
    public void setKeySpace(String keyspace) {
        this.keyspace = keyspace;
    }

    /**
     * Returns the name of the Cassandra keyspace.
     * @return the name of the Cassandra keyspace.
     */
    public String getKeySpace() {
        return this.keyspace;
    }

    /**
     * Sets the username for authentication. If authentication is not required
     * then this method need not be called.
     * @param username the username for authentication.
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Returns the username for authentication.
     * @return Username or null.
     */
    public String getUsername() {
        return this.username;
    }
    
    /**
     * Sets the password for authentication. If authentication is not required
     * then this method need not be called.
     * @param password The password for authentication.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Returns the password for authentication.
     * @return Password or null.
     */
    public String getPassword() {
        return this.password;
    }
 
    /**
     * Parses a thrift/cassandra consistency level.
     * @param level Consistency level as a string.
     * @return consistency level as an HConsistencyLevel instance.
     */
    private HConsistencyLevel parseConsistencyLevel(String level) throws Exception {
        if (level == null) throw new Exception("Consistency level is null!");
        
        if ("ALL".equals(level)) {
            return HConsistencyLevel.ALL;
        }
        else if ("ANY".equals(level)) {
            return HConsistencyLevel.ANY;
        }
        else if ("EACH_QUORUM".equals(level)) {
            return HConsistencyLevel.EACH_QUORUM;
        }
        else if ("LOCAL_QUORUM".equals(level)) {
            return HConsistencyLevel.LOCAL_QUORUM;
        }
        else if ("ONE".equals(level)) {
            return HConsistencyLevel.ONE;
        }
        else if ("QUORUM".equals(level)) {
            return HConsistencyLevel.QUORUM;
        }
        else if ("THREE".equals(level)) {
            return HConsistencyLevel.THREE;
        }
        else if ("TWO".equals(level)) {
            return HConsistencyLevel.TWO;
        }
        else {
            throw new Exception("Unknown consistency level defined: "+level);
        }
    }

    /**
     * Sets the consistency level to use for read operation.
     * @param level Consistency level.
     */
    public void setReadConsistencyLevel(String level) {
        try {
            this.readConsistencyLevel = this.parseConsistencyLevel(level);
        }
        catch (Throwable e) {
            cat.error("Unable to parse read consistency level: \""+level+"\" - using default LOCAL_QUORUM.");
            this.readConsistencyLevel = HConsistencyLevel.QUORUM;
        }
    }

    /**
     * Returns the consistency level used for read operations.
     * @return the consistency level used for read operations.
     */
    public HConsistencyLevel getReadConsistencyLevel() {
        return this.readConsistencyLevel;
    }
    
    /**
     * Sets the consistency level to use for write operation.
     * @param level Consistency level.
     */
    public void setWriteConsistencyLevel(String level) {
        try {
            this.writeConsistencyLevel = this.parseConsistencyLevel(level);
        }
        catch (Throwable e) {
            cat.error("Unable to parse write consistency level: \""+level+"\" - using default EACH_QUORUM.");
            this.writeConsistencyLevel = HConsistencyLevel.QUORUM;
        }
    }

    /**
     * Returns the consistency level used for write operations.
     * @return the consistency level used for read operations.
     */
    public HConsistencyLevel getWriteConsistencyLevel() {
        return this.writeConsistencyLevel;
    }
 
    /**
     * Sets the compression level for persisted objects.
     * @param level Compression level for persisted objects ("BEST" gives best
     *    compression, "SPEED" gives best performance, "DEFAULT" provides
     *    balanced compression/speed, and "NONE" gives no compression).
     */
    public void setCompressionLevel(String level) {
        // This setting offers the best compression level, meaning it will
        // incur the least I/O overhead, but CPU overhead on the Tomcat node
        // will be higher.
        if ("BEST".equals(level)) {
            this.compressionLevel = Deflater.BEST_COMPRESSION;
        }
        // This setting offers the fastest compression, meaning it will
        // incur the least CPU overhead on the Tomcat node, but I/O overhead
        // will be higher.
        else if ("SPEED".equals(level)) {
            this.compressionLevel = Deflater.BEST_SPEED;
        }
        // This setting offers the best compromise between speed
        // (CPU overhead) and compression (I/O overhead).
        else if ("DEFAULT".equals(level)) {
            this.compressionLevel = Deflater.DEFAULT_COMPRESSION;
        }
        // This setting disabled compression, but leaves decompression enabled.
        // The setting should be used for a short period of time after 
        // compression has been disabled.
        else if ("NONE".equals(level)) {
            this.compressionLevel = 0;
        }
        // This setting disabled compression and decompression completely. 
        else if ("OFF".equals(level)) {
            this.compressionLevel = -1;
        }
        // Default setting is OFF.
        else {
            // This allows the compression level to be set to a figure between
            // 0 and 9 (which is what the Java GZIP implementation allows).
            try {
                final int compressionLevel = Integer.parseInt(level);
                if (compressionLevel >=0 && compressionLevel <= 9) {
                    this.compressionLevel = compressionLevel;
                    return;
                }
            }
            catch (Throwable e) {
                // Ignore - error output below.
            }

            cat.error("Invalid compression level \""+level+"\" set for Cassandra session manager. Defaulting compression to OFF!");
            this.compressionLevel = -1;
        }
    }

    /**
     * Returns the compression level for serialised objects.
     * @return the compression level for serialised objects.
     */
    public int getCompressionLevel() {
        return this.compressionLevel;
    }

    /**
     * Returns the session timeout in seconds.
     * @return the session timeout in seconds.
     */
    public long getTimeout() {
    	return this.timeout;
    }
 
    /**
     * Sets the session timeout in seconds.
     * @param timeout The session timeout in seconds.
     */
    public void setTimeout(long timeout) {
    	this.timeout = timeout;
    }

    /**
     * Returns the interval for how often we should purge expired sessions.
     * @return Purge interval in seconds.
     */
    public long getPurgeInterval() {
    	return this.purgeInterval;
    }
    
    /**
     * Sets the interval for how often we should purge expired sessions.
     * @param interval Purge interval in seconds.
     */
    public void setPurgeInterval(long interval) {
    	this.purgeInterval = interval;
    }
}