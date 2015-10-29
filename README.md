#Tomcat/Cassandra Session Manager

This project offers a Tomcat session manager using Cassandra for session
persistence and replication.

Version 1.6.2 is now available, providing fixes for session expiry.


##Benefits

Session data exists only in Cassandra, and none of your Tomcat nodes maintain
session data in memory between serving requests. This has two benefits:

* Reduces the memory footprint of your application.
 
* Improves scalability as no Tomcat node becomes a memory/session bottleneck.
  
* The session is persisted as individual attributes, and session attributes
  are loaded on demand. For applications with large amount of session
  attributes, each JSP/servlet may not trigger a load of all session
  attributes, hence memory footprint should be reduced.

* No code changes to Tomcat or your application are required. 


##Configuration

All required resources are found in tomcat-cassandra-*.*.tar

1. **Configure Cassandra:**
   Cassandra is configured with the schema provided in schema/tomcat.txt.

2. **Configure Tomcat:**
   Copy the JARs from the tomcat/server/lib folder to the tomcat library
   folder (such as $CATALINA_HOME/lib).

3. **Configure Session Manager:**
   Tomcat is configured using standard manager configuration in your web
   application descriptor file (such as
   $CATALINA_HOME/conf/Catalina/localhost/sample.xml):

    <Context docBase="/foo/bar/sample" path="/sample" reloadable="false">

      <!-- Configures the Cassandra session manager for the web application.  -->
      <!-- @hosts = comma-separated list of Cassandra hosts to connect to.    -->
      <!-- @cluster = name of Cassandra cluster/ring.                         -->
      <!-- @keySpace = name of Cassandra key-space ("tomcat").                -->
      <!-- @username = username for authentication.                           -->
      <!-- @password = password for authentication.                           -->
      <!-- @readConsistency = consistency level for read operations.          -->
      <!--    Allowed values (default is "QUORUM"):                           -->
      <!--      "ALL" - read from all nodes.                                  -->
      <!--      "ANY" - read from any single node.                            -->
      <!--      "ONE" - read from a single node.                              -->
      <!--      "TWO" - read from two nodes.                                  -->
      <!--      "THREE" - read from three nodes.                              -->
      <!--      "QUORUM" - quorum read from local data centre.                -->
      <!--      "LOCAL_QUORUM" - quorum read from local data centre.          -->
      <!--      "EACH_QUORUM" - quorum read from all data centres.            -->
      <!-- @writeConsistency = consistency level for write operations.        -->
      <!--    Allowed values (default is "QUORUM"):                           -->
      <!--      "ALL" - write to all nodes.                                   -->
      <!--      "ANY" - write to any single node.                             -->
      <!--      "ONE" - write to a single node.                               -->
      <!--      "TWO" - write to two nodes.                                   -->
      <!--      "THREE" - write to three nodes.                               -->
      <!--      "QUORUM" - quorum write to local data centre.                 -->
      <!--      "LOCAL_QUORUM" - quorum write to local data centre.           -->
      <!--      "EACH_QUORUM" - quorum write to all data centres.             -->
      <!-- @compressionLevel = compression level for session objects.         -->
      <!--    Allowed values (default is "OFF"):                              -->
      <!--      "BEST"    - for best compression (less I/O, more CPU).        -->
      <!--      "SPEED"   - for fastest compression (more I/O, less CPU).     -->
      <!--      "DEFAULT" - for default compression (balanced I/O and CPU).   -->
      <!--      "NONE"    - for no compression, but allowing decompression of -->
      <!--                  already compressed objects. Use this setting for  -->
      <!--                  a short period to safely switch compression off.  -->
      <!--      "OFF"     - compression and decompression completely off.     -->
      <!-- @timeout = session expiry time in seconds.                         -->
      <Manager className="org.apache.catalina.cassandra.TomcatManager" hosts="heisenberg:9160,bohr:9160,einstein:9160" cluster="TestCluster" keySpace="tomcat" username="morten" password="******" readConsistency="QUORUM" writeConsistency="QUORUM" timeout="600"/>

      <!-- This valve allows logging of Cassandra IO and performance to Log4J -->
      <!-- Note that Log4J DEBUG-level logging must be enabled for the class  -->
      <!-- org.apache.catalina.cassandra.utils.StatisticsLogger for a full    -->
      <!-- set of statistics.                                                 -->
      <Valve className="org.apache.catalina.cassandra.utils.StatisticsLogger"/>
      
      <!-- This cache implementations writes updated session objects back to  -->
      <!-- Cassandra after the current request has completed processing. Only -->
      <!-- session objects that have been written back through calls to       -->
      <!-- HttpSession.setAttribute(String,Object) are presisted to           -->
      <!-- Cassandra. Other session objects are not considered changed.       -->
      <Valve className="org.apache.catalina.cassandra.cache.AggressiveWriteBackCache"/>
      
      <!-- This cache implementations writes all read session objects back to -->
      <!-- Cassandra after the current request has completed processing. All  -->
      <!-- session objects that have been read from Cassandra through calls   -->
      <!-- to HttpSession.getAttribute(String) will be persisted to Cassandra -->
      <!-- assuming the calling code may have changed the contents of the     -->
      <!-- object.                                                            -->
      <!-- Valve className="org.apache.catalina.cassandra.cache.WriteBackCache"/ -->
      
      <!-- This cache implementation writes changes to session objects back   -->
      <!-- to Cassandra as soon as the client code has called                 -->
      <!-- HttpSession.setAttribute(String,Object).                           -->
      <!-- Valve className="org.apache.catalina.cassandra.cache.WriteThroughCache"/ -->

    </Context>


##Caching

Three cache implementations are provided, and the cache implementation should
be selected to suit the behaviour of your web application.

* The write-through writes changes to session objects back to Cassandra as
  soon as the client code has called HttpSession.setAttribute(String,Object).
  This cache is useful if your application serves multiple, concurrent Ajax
  requests for the same user session.

* The write-back cache writes all read session objects back to Cassandra
  after the current request has completed processing. All session objects
  that have been read from Cassandra through calls to
  HttpSession.getAttribute(String) will be persisted to Cassandra assuming
  the calling code may have changed the contents of the object. This cache
  is useful when your web application maintains larger session objects, and
  your code may update data within these objects without calling
  HttpSession.setAttribute(String,Object).

* The aggressive write-back cache works as the normal write-back cache, but
  it writes back to Cassandra only those session objects that have been
  set/updated using HttpSession.setAttribute(String,Object). Use this cache
  when your code consistently calls HttpSession.setAttribute(String,Object)
  to flag that the a session object has been updated. 


##Performance Statistics

You can record statistics on the cache success rate, serialisation overhead,
Cassandra I/O overhead and Cassandra session sizes using Log4J. To generate
statistics output you must:

1. Configure the valve org.apache.catalina.cassandra.utils.StatisticsLogger
   for your web application.
    
2. Enable Log4J "DEBUG" log level for the Java class
   `org.apache.catalina.cassandra.utils.StatisticsLogger`


##Considerations

It is widespread practice to manage session data within a session scope bean,
unfortunately. To avail of the benefits of this session manager, individual
objects should be stored directly as session attributes, rather than
indirectly via a hash-map/table in a session-scope bean. To aid you in
migrating your code to a model better suited to this session manager, a Map
implementation that stores values directly as session attributes is provided.
To use this Map you need to:

1. Change your session-scope bean from using a standard HashMap to using the
   org.apache.catalina.cassandra.utils.SessionMap class.

2. Configure a Valve that binds requests to a thread-local variable used by
   the SessionMap class (don't worry, the thread-local variable will be
   cleared as soon as the request has been processed):

      <Valve className="org.apache.catalina.cassandra.utils.RequestHolderValve"/>