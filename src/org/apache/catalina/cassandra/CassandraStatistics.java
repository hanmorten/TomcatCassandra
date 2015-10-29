// @(#)$Id: SomeClass.java,v 1.1 2007/04/04 00:02:36 morten Exp $
package org.apache.catalina.cassandra;

import java.util.*;
import java.text.DecimalFormat;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import org.apache.log4j.Logger;

/**
 * (C)opyright OpenJaw Technologies 2012
 * <ul>
 *   <li><b>Project:</b> TomcatCassandraApache</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.CassandraStatistics</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 5 Nov 2012 10:03:37</li>
 *   <li><b>Description:</b>
 *     Interface for recording statistics on Cassandra read/write operations.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
public class CassandraStatistics {

    /** Log4J logger instance for class CassandraStatistics. */
    private final static Logger cat = Logger.getLogger(CassandraStatistics.class);
    /** Log4J debug setting for class CassandraStatistics. */
    private final static boolean debug = cat.isDebugEnabled();
    
	/**
	 * Global counters that can be used to extract statistics from a client
	 * application (you may hava to use reflection to get at this).
	 */
	private static StatisticsAverages global = new StatisticsAverages();
	
    /** This binds a cache instance to the current thread. */
    private static ThreadLocal<CassandraStatistics> instance =
        new ThreadLocal<CassandraStatistics>();
    
    /** URL for page stats are gathered for. */
    private String url;
    
    /** Start time for page processing. */
    private long start = 0;
    /** End time for page processing. */
    private long stop = 0;
    
    /** Statistics: Cache writes. */
    private int writes = 0;
    /** Statistics: Cache hits. */
    private int hits = 0;
    /** Statistics: Cache misses. */
    private int misses = 0;
    /** Statistics: Time taken to flush the cache (write-back caches). */
    private long flush = 0;
    
    /** Statistics: Objects written to Cassandra. */
    private int writeObjects = 0;
    /** Statistics: Bytes written to Cassandra. */
    private int writeBytes = 0;
    /** Statistics: Largest object written to Cassandra. */
    private int writeLargest = 0;
    /** Statistics: Largest object written name. */
    private String writeLargestName = null;
    /** Statistics: Slowest object write time. */
    private long writeSlowestTime = 0;
    /** Statistics: Slowest object name. */
    private String writeSlowestName = null;
    /** Statistics: Milliseconds taken to read from Cassandra. */
    private long rtime = 0;
    /** Statistics: Milliseconds taken to deserialise. */
    private long rstime = 0;

    /** Statistics: Objects read from Cassandra. */
    private int readObjects = 0;
    /** Statistics: Bytes read from Cassandra. */
    private int readBytes = 0;
    /** Statistics: Largest object read from Cassandra. */
    private int readLargest = 0;
    /** Statistics: Largest object read name. */
    private String readLargestName = null;
    /** Statistics: Slowest object read time. */
    private long readSlowestTime = 0;
    /** Statistics: Slowest object serialisation time. */
    private String readSlowestName = null;
    /** Statistics: Milliseconds taken to write to Cassandra. */
    private long wtime = 0;
    /** Statistics: Milliseconds taken to serialise. */
    private long wstime = 0;
    

    /** Current state: Size in bytes of last object read/written. */
    private int stateSize = 0;
    /** Current state: Time of last object serialisation. */
    private long stateSerialisation = 0;
    /** Current state: Time of last object read/write (including serialisation). */
    private long stateDuration = 0;
    
    private Set<String> readObjectNames = Collections.synchronizedSet(new HashSet<String>());
    private Set<String> writeObjectNames = Collections.synchronizedSet(new HashSet<String>());
    
    private static TomcatManager manager = null;
    
    /**
     * Creates a new statistics container.
     */
    public CassandraStatistics(String url) {
        this.url = url.toString();
        this.start = System.currentTimeMillis();
    }

    /**
     * Attaches a new statistics instance to the current thread.
     */
    public static void attach(String url) {
        CassandraStatistics.instance.set(new CassandraStatistics(url));

    }

    /**
     * Detaches any statistics instance from the current thread.
     */
    public static CassandraStatistics detach() {
        final CassandraStatistics instance = CassandraStatistics.instance.get(); 
        CassandraStatistics.instance.remove();
        instance.stop = System.currentTimeMillis();
        return instance;
    }

    static void register(TomcatManager manager) {
        CassandraStatistics.manager = manager;
    }
    
    /**
     * Returns the current statitics instance (if any).
     * @return the current statitics instance (if any).
     */
    public static CassandraStatistics getInstance() {
        return CassandraStatistics.instance.get();
    }

    /**
     * Increments the number of cache hits.
     */
    public void cacheHit() {
        this.hits++;
    }
    
    /**
     * Increments the number of cache misses.
     */
    public void cacheMiss() {
        this.misses++;
    }
    
    /**
     * Increments the number of cache writes.
     */
    public void cacheWrite() {
        this.writes++;
    }

    /**
     * Sets the time taken to flush the cache.
     * @param time the time taken to flush the cache.
     */
    public void setFlushTime(long time) {
        this.flush = time;
    }

    /**
     * Sets the size of the last object read/written.
     * @param size the size of the last object read/written.
     */
    public void setLastSize(int size) {
        this.stateSize = size;
    }

    /**
     * Sets the time it took to (de-)serialise the last object.
     * @param duration the time it took to (de-)serialise the last object.
     */
    public void setLastSerialisation(long duration) {
        this.stateSerialisation = duration;
    }

    /**
     * Sets the time it took to (de-)serialiser and read/write the last object.
     * @param duration the time it took to (de-)serialiser and read/write the
     *    last object.
     */
    public void setLastDuration(long duration) {
        this.stateDuration = duration;
    }

    /**
     * Indicates that the last operation was a read operation and logs the
     * statistics accordingly.
     * @param name Name of object that was read.
     */
    public void read(String name) {
        final long rstime = this.stateSerialisation;
        final long rtime = this.stateDuration - rstime;
        final int size = this.stateSize;
        
        this.read(name, size, rtime, rstime);

        this.stateSerialisation = 0;
        this.stateDuration = 0;
        this.stateSize = 0;
    }

    /**
     * Indicates that the last operation was a write operation and logs the
     * statistics accordingly.
     * @param name Name of object that was written.
     */
    public void write(String name) {
        final long wstime = this.stateSerialisation;
        final long wtime = this.stateDuration - wstime;
        final int size = this.stateSize;

        this.write(name, size, wtime, wstime);

        this.stateSerialisation = 0;
        this.stateDuration = 0;
        this.stateSize = 0;
    }
    
    /**
     * Stores statistics information about a Cassandra read operation.
     * @param name Name of object being read.
     * @param size Size if object (in serialised form).
     * @param rtime Time it took to read and deserialise the object from Cassandra.
     */
    private void read(String name, int size, long rtime, long rstime) {
        this.readObjectNames.add(name);
        this.readBytes += size;
        this.readObjects ++;
        if (size > this.readLargest) {
            this.readLargest = size;
            this.readLargestName = name; 
        }
        if (rtime > this.readSlowestTime) {
            this.readSlowestTime = rtime;
            this.readSlowestName = name;
        }
        this.rtime += rtime;
        this.rstime += rstime;
    }

    /**
     * Stores statistics information about a Cassandra write operation.
     * @param name Name of object being written.
     * @param size Size if object (in serialised form).
     * @param write Time it took to serialise and write the object to Cassandra.
     */
    private void write(String name, int size, long wtime, long wstime) {
        this.writeObjectNames.add(name);
        this.writeBytes += size;
        this.writeObjects ++;
        if (size > this.writeLargest) {
            this.writeLargest = size;
            this.writeLargestName = name;
        }
        if (wtime > this.writeSlowestTime) {
            this.writeSlowestTime = wtime;
            this.writeSlowestName = name;
        }
        this.wtime += wtime;
        this.wstime += wstime;
    }
    
    /**
     * Returns a string that shows the write/hit/miss counters for the cache.
     * @return a string that shows the write/hit/miss counters for the cache.
     */
    public String getCacheStatistics() {
        final StringBuffer buf = new StringBuffer();
        buf.append("Cache Statistics: Writes=");
        buf.append(this.writes);
        buf.append(", Misses=");
        buf.append(this.misses);
        buf.append(", Hits=");
        buf.append(this.hits);
        buf.append(", SuccessRate=");
        final double total = (double)this.hits + (double)this.misses;
        if (total != 0) {
            final double rate = ((double)this.hits / total) * 100d;
            buf.append(new DecimalFormat("#.##").format(rate));
        }
        else {
            buf.append("0.00");
        }
        buf.append("%");
        buf.append(", FlushTime=");
        buf.append(this.flush);
        buf.append("ms");
        return buf.toString();
    }

    /**
     * Returns the statistics for write operations.
     * @return the statistics for write operations.
     */
    public String getWriteStatistics() {
        final StringBuffer buf = new StringBuffer();
        buf.append("Cassandra Writes: Objects=");
        buf.append(this.writeObjects);
        buf.append(", Size=");
        buf.append(this.writeBytes);
        buf.append("b, Time=");
        buf.append(this.wtime);
        buf.append("ms, Serialisation=");
        buf.append(this.wstime);
        buf.append("ms, Largest=\"");
        buf.append(this.writeLargestName);
        buf.append("\" (");
        buf.append(this.writeLargest);
        buf.append("b), Slowest=\"");
        buf.append(this.writeSlowestName);
        buf.append("\" (");
        buf.append(this.writeSlowestTime);
        buf.append("ms), Instances=");
        buf.append(this.writeObjectNames);
        return buf.toString();
    }
    
    /**
     * Returns the statistics for read operations.
     * @return the statistics for read operations.
     */
    public String getReadStatistics() {
        final StringBuffer buf = new StringBuffer();
        buf.append("Cassandra Reads: Objects=");
        buf.append(this.readObjects);
        buf.append(", Size=");
        buf.append(this.readBytes);
        buf.append("b, Time=");
        buf.append(this.rtime);
        buf.append("ms, Serialisation=");
        buf.append(this.rstime);
        buf.append("ms, Largest=\"");
        buf.append(this.readLargestName);
        buf.append("\" (");
        buf.append(this.readLargest);
        buf.append("b), Slowest=\"");
        buf.append(this.readSlowestName);
        buf.append("\" (");
        buf.append(this.readSlowestTime);
        buf.append("ms), Instances=");
        buf.append(this.readObjectNames);
        return buf.toString();
    }

    /**
     * Outputs statistics on overhead read/write and serialisation.
     * Note that overhead from increased garbage collection is not tracked!
     * @return processing overhead statistics.
     */
    public String getOverheadStatistics() {
        final DecimalFormat formatter = new DecimalFormat("#.##");

        final long total = this.stop - this.start; 
        final long overhead = this.wtime + this.wstime + this.rtime + this.rstime;

        final StringBuffer buf = new StringBuffer();
        buf.append("Overhead: Total=");
        buf.append(total);
        buf.append("ms, Overhead=");
        buf.append(overhead);
        buf.append("ms (");
        if (total > 0) {
            final double rate = ((double)overhead / (double)total) * 100d;
            buf.append(formatter.format(rate));
        }
        else {
            buf.append("100.00");
        }
        buf.append("%)");
        
        return buf.toString();
    }

    /*
     * Outputs all statistics in CSV format so that they can be imported into
     * a spreadsheet, such as Excel.
     */
    public String toString() {
        final DecimalFormat formatter = new DecimalFormat("#.##");

        final StringBuffer buf = new StringBuffer();
        
        // Append start marker for easier use of cut command, such as
        //   grep "AggressiveWriteBackCache:145" logfile.txt | cut -d"|" -f2
        buf.append("|");

        // Append URL
        buf.append(this.url);
        buf.append(",");

        // Append overall processing time.
        final long total = this.stop - this.start; 
        buf.append(total);
        buf.append(",");
        
        // Append cache stats
        buf.append(this.writes);
        buf.append(",");
        buf.append(this.misses);
        buf.append(",");
        buf.append(this.hits);
        buf.append(",");
        final double ctotal = (double)this.hits + (double)this.misses;
        if (ctotal != 0) {
            final double rate = ((double)this.hits / ctotal) * 100d;
            buf.append(formatter.format(rate));
        }
        else {
            buf.append("100.00");
        }
        buf.append("%");
        buf.append(",");
        buf.append(this.flush);
        buf.append(",");

        // Append write stats
        buf.append(this.writeObjects);
        buf.append(",");
        buf.append(this.writeBytes);
        buf.append(",");
        buf.append(this.wtime);
        buf.append(",");
        buf.append(this.wstime);
        buf.append(",");
        buf.append(this.writeLargestName);
        buf.append(",");
        buf.append(this.writeLargest);
        buf.append(",");
        buf.append(this.writeSlowestName);
        buf.append(",");
        buf.append(this.writeSlowestTime);
        buf.append(",");

        // Append read stats
        buf.append(this.readObjects);
        buf.append(",");
        buf.append(this.readBytes);
        buf.append(",");
        buf.append(this.rtime);
        buf.append(",");
        buf.append(this.rstime);
        buf.append(",");
        buf.append(this.readLargestName);
        buf.append(",");
        buf.append(this.readLargest);
        buf.append(",");
        buf.append(this.readSlowestName);
        buf.append(",");
        buf.append(this.readSlowestTime);
        buf.append(",");

        // Append overhead stats;
        final long overhead = this.wtime + this.wstime + this.rtime + this.rstime;
        buf.append(overhead);
        buf.append(",");
        if (total > 0) {
            final double rate = ((double)overhead / (double)total) * 100d;
            buf.append(formatter.format(rate));
        }
        else {
            buf.append("100.00");
        }
        
        return buf.toString();
    }

    /**
     * Dumps the most important statistics into the global, static counters.
     * This allows external applications to read the stats periodically.
     */
    public void dump() {
    	global.setReadWriteOverhead(this.url, this.wtime + this.rtime);
    	global.setSerialisationOverhead(this.url, this.wstime + this.rstime);
    }

    /**
     * Returns the current state of the global statistics as a DOM Document.
     * If you don't want to compile your code against this session manager,
     * then you may access this method using reflection as follows
     * (this method is not intended accessed frequently, so using reflection
     * should be OK in terms of performance):
     * <pre>
     *   final Class clazz = Class.forName("org.apache.catalina.cassandra.CassandraStatistics");
     *   final Method method = clazz.getMethod("toDocument", new Class[0] );
     *   final Document cassandra = (Document)method.invoke(null,  null);
     *   params.put("CassandraStatistics", cassandra);
     * </pre>
     * @return current state of global statistics as a DOM document.
     */
    public static Document toDocument() {
    	final Document xml = global.toDocument();
    	if (manager != null) {
    	    final Element sessions = manager.getSessionSizes(xml);
    	    xml.getDocumentElement().appendChild(sessions);
    	}
    	return xml;
    }

    /**
     * This class maintains min/avg/max values for a single statistics
     * counter. This is used to hold either serialisation/deserialisation
     * or Cassandra read/write times for a single page/URL.
     */
    private static class StatisticsAverage {
    	
        /** Maximum value we allow before we truncate. */
        private final static long MAX_VALUE = Long.MAX_VALUE / 2;

        private String type;
        /** Identifying name for this statistics counter. */
        private String name;
        /** Aggregated values. */
        private long aggregate = 0;
        /** Number of values. */
        private long count = 0;
        /** Minimum value. */
        private long min = Long.MAX_VALUE;
        /** Maximum value. */
        private long max = 0;

        /**
         * Creates a new statistics min/avg/max counter.
         * @param name Identifying name.
         */
        public StatisticsAverage(String type, String name) {
        	this.type = type;
        	this.name = name;
        }
        
        /**
         * Adds a value to the average stats counter.
         * @param value Value to add.
         */
        public synchronized void addValue(long value) {
            // Truncate the values if we've gone past the max value.
            if (this.aggregate > MAX_VALUE) {
                this.count = this.count / 2;
                this.aggregate = this.aggregate / 2;
            }
            this.count++;
            this.aggregate += value;
            if (value > 0 && value < min) min = value;
            if (value > max) max = value;
        }
        
        /**
         * Returns the average of all values stored in this counter.
         * @return the average of all values stored in this counter.
         */
        public synchronized long getAverage() {
            if (this.count == 0)
                return 0;
            else
                return this.aggregate / this.count;
        }

        /**
         * Returns the minimum value recorded.
         * @return the minimum value recorded.
         */
        public long getMin() {
            if (this.min == Long.MAX_VALUE)
                return 0;
            else
                return this.min;
        }
        
        /**
         * Returns the maximum value recorded.
         * @return the maximum value recorded.
         */
        public long getMax() {
            return this.max;
        }
        
        /**
         * Writes the statistics entry as a subelement of the specified parent.
         * @param parent Parent statistics XML element.
         */
        public Element toElement(Document doc) {
            final Element element = doc.createElement(this.type);
            element.setAttribute("url", this.name);
            element.setAttribute("value", Long.toString(this.getAverage()));
            element.setAttribute("min", Long.toString(this.getMin()));
            element.setAttribute("max", Long.toString(this.getMax()));
            return element;
        }
    }

    /**
     * Maintains min/avg/max values for serialisation/deserialisation and
     * Cassandra read/write across all accessed page URLs.
     */
    private static class StatisticsAverages {

    	/** Stats counters for serialisation times. */
    	private Map<String,StatisticsAverage> serialisation = 
    			Collections.synchronizedMap(new HashMap<String,StatisticsAverage>());
    	/** Stats counters for I/O read/write times. */
    	private Map<String,StatisticsAverage> readwrite = 
    			Collections.synchronizedMap(new HashMap<String,StatisticsAverage>());
    	
    	/**
    	 * Creates a new statistics container.
    	 */
    	public StatisticsAverages() {
    		
    	}

    	/**
    	 * Obtains the statistics counter for serialisation/deserialisation
    	 * for a single page. The counter is created on demand.
    	 * @param uri URI of page to obtain counter for.
    	 * @return Serialisation/deserialisation counter for the page.
    	 */
    	private StatisticsAverage getSerialisationCounter(String uri) {
    		StatisticsAverage counter = this.serialisation.get(uri);
    		if (counter == null) {
    			synchronized (this) {
    				counter = this.serialisation.get(uri);
    	    		if (counter == null) {
    	    			this.serialisation.put(uri, counter = new StatisticsAverage("Serialisation", uri));
    	    		}
    			}
    		}
    		return counter;
    	}
    	
    	/**
    	 * Obtains the statistics counter for Cassandra read/write
    	 * for a single page. The counter is created on demand.
    	 * @param uri URI of page to obtain counter for.
    	 * @return Cassandra read/write counter for the page.
    	 */
    	private StatisticsAverage getReadWriteCounter(String uri) {
    		StatisticsAverage counter = this.readwrite.get(uri);
    		if (counter == null) {
    			synchronized (this) {
    				counter = this.readwrite.get(uri);
    	    		if (counter == null) {
    	    			this.readwrite.put(uri, counter = new StatisticsAverage("ReadWrite", uri));
    	    		}
    			}
    		}
    		return counter;
    	}

    	/**
    	 * Records serialisation overhead for a given page.
    	 * @param uri Page URL.
    	 * @param time Combined serialisation/deserialisation time for a
    	 *    single access to the page.
    	 */
    	public void setSerialisationOverhead(String uri, long time) {
    		this.getSerialisationCounter(uri).addValue(time);
    	}

    	/**
    	 * Records Cassandra I/O overhead for a given page.
    	 * @param uri Page URL.
    	 * @param time Combined Cassandra read/write time for a
    	 *    single access to the page.
    	 */
    	public void setReadWriteOverhead(String uri, long time) {
    		this.getReadWriteCounter(uri).addValue(time);
    	}
    
    	/**
    	 * Creates an XML representation of all contained counters.
    	 * @return an XML representation of all contained counters.
    	 */
    	public Document toDocument() {
    		try {
	            final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	            factory.setNamespaceAware(true);
	            factory.setFeature("http://apache.org/xml/features/dom/defer-node-expansion", false);
	            final DocumentBuilder builder = factory.newDocumentBuilder();
	            final Document doc = builder.newDocument();
	            final Element root = doc.createElement("TomcatCassandraStatistics");
	            doc.appendChild(root);
	            
	            final Iterator<StatisticsAverage> serialisations = this.serialisation.values().iterator();
	            while (serialisations.hasNext()) {
	            	final StatisticsAverage counter = serialisations.next();
	            	root.appendChild(counter.toElement(doc));
	            }
	
	            final Iterator<StatisticsAverage> readwrites = this.readwrite.values().iterator();
	            while (readwrites.hasNext()) {
	            	final StatisticsAverage counter = readwrites.next();
	            	root.appendChild(counter.toElement(doc));
	            }
	            
	            return doc;
    		}
    		catch (Throwable e) {
    			// ignore
    			return null;
    		}
    	}
    }
    
}
