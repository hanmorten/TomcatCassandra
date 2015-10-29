// @(#)$Id: SomeClass.java,v 1.1 2007/04/04 00:02:36 morten Exp $
package org.apache.catalina.cassandra.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.cassandra.serializers.AbstractSerializer;

import org.apache.log4j.Logger;

import org.apache.catalina.cassandra.CassandraStatistics;
import org.apache.catalina.cassandra.TomcatManager;

/**
 * (C)opyright OpenJaw Technologies 2012
 * <ul>
 *   <li><b>Project:</b> TomcatCassandraApache</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.utils.ImprovedSerialiser</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 13 Nov 2012 09:42:31</li>
 *   <li><b>Description:</b>
 *     
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
public class SessionObjectSerialiser extends AbstractSerializer<Object> implements Serializer<Object> {

    /** Log4J logger instance for class ImprovedSerialiser. */
    private final static Logger cat = Logger.getLogger(SessionObjectSerialiser.class);
    /** Log4J debug setting for class ImprovedSerialiser. */
    private final static boolean debug = cat.isDebugEnabled();

    private TomcatManager manager;
    
    public SessionObjectSerialiser(TomcatManager manager) {
        this.manager = manager;
    }
    
    public ByteBuffer toByteBuffer(Object object) {
        final int compressionLevel = this.manager.getConfig().getCompressionLevel();

        try {
            final CassandraStatistics stats = CassandraStatistics.getInstance(); 
            final long start = System.currentTimeMillis();

            byte[] result = null;
            
            // Don't compress if compression level is OFF or NONE.
            if (compressionLevel <= 0) {
                final ByteArrayOutputStream baostr = new ByteArrayOutputStream();
                final ObjectOutputStream obostr = new ObjectOutputStream(baostr);
                obostr.writeObject(object);
                obostr.flush();
                baostr.flush();
                result = baostr.toByteArray();
            }
            // Otherwise compress using the given level.
            else {
                final ByteArrayOutputStream baostr = new ByteArrayOutputStream();
                final CompressedOutputStream costr = new CompressedOutputStream(baostr);
                costr.setCompressionLevel(compressionLevel);
                final ObjectOutputStream obostr = new ObjectOutputStream(costr);
                obostr.writeObject(object);
                obostr.flush();
                costr.flush();
                costr.close();
                baostr.flush();
                result = baostr.toByteArray();
            }

            if (stats != null) {
                final long stop = System.currentTimeMillis();
                final long duration = stop - start;
                stats.setLastSerialisation(duration);
                final int size = result.length;
                stats.setLastSize(size);
            }

            return ByteBuffer.wrap(result);
        }
        catch (StackOverflowError e) {
            cat.error("Stack overflow error saving object "+object.getClass().getName()+" - it most likely has a circular reference: "+e.getMessage(), e);
            throw new RuntimeException(e);
        }
        catch (IOException e) {
        	if (object != null) {
        		cat.error("Unable to serialise session object "+object.getClass().getName()+": "+e.getMessage(), e);
        	}
        	else {
        		cat.error("Unable to serialise null-session object: "+e.getMessage(), e);
        	}
            throw new RuntimeException(e);
        }
    }

    public Object fromByteBuffer(ByteBuffer bytes) {
        if ((bytes == null) || !bytes.hasRemaining()) {
            return null;
        }

        final int compressionLevel = this.manager.getConfig().getCompressionLevel();
        final ClassLoader classLoader = this.manager.getClassLoader();
        
        // Attempt to de-compress if compression level is not OFF.
        if (compressionLevel >= 0) {
            try {
                final CassandraStatistics stats = CassandraStatistics.getInstance(); 
                final long start = System.currentTimeMillis();

                final int remaining = bytes.remaining();
                final ByteArrayInputStream baistr = new ByteArrayInputStream(bytes.array(), bytes.arrayOffset() + bytes.position(), remaining);
                final CompressedInputStream cistr = new CompressedInputStream(baistr);
                final ObjectInputStream obistr = new ObjectInputStream(cistr, classLoader);
                final Object result = obistr.readObject();
                obistr.close();
                bytes.position(bytes.position() + (remaining - obistr.available()));
                
                if (stats != null) {
                    final long stop = System.currentTimeMillis();
                    final long duration = stop - start;
                    stats.setLastSerialisation(duration);
                    final int size = remaining - bytes.remaining();
                    stats.setLastSize(size);
                }
                
                return result;
            }
            catch (ClassNotFoundException e) {
                cat.warn("Unable to parse compressed object: "+e.getMessage());
                // Ignore - try non-compressed parse of the object instead as
                // the compression level might have been adjusted since last
                // restart. This allows previously on-compressed objects to be
                // read from Cassandra, but once they are updated they will be
                // written back to Cassandra in compressed form.
            }
            catch (IOException e) {
                cat.warn("Unable to parse compressed object: "+e.getMessage());
                // Ignore - try non-compressed parse of the object instead as
                // the compression level might have been adjusted since last
                // restart. This allows previously on-compressed objects to be
                // read from Cassandra, but once they are updated they will be
                // written back to Cassandra in compressed form.
            }
        }

        // Compression is OFF or de-compression failed.
        try {
            final CassandraStatistics stats = CassandraStatistics.getInstance(); 
            final long start = System.currentTimeMillis();

            final int remaining = bytes.remaining();
            final ByteArrayInputStream baistr = new ByteArrayInputStream(bytes.array(), bytes.arrayOffset() + bytes.position(), remaining);
            final ObjectInputStream obistr = new ObjectInputStream(baistr, classLoader);
            final Object result = obistr.readObject();
            obistr.close();
            bytes.position(bytes.position() + (remaining - obistr.available()));

            if (stats != null) {
                final long stop = System.currentTimeMillis();
                final long duration = stop - start;
                stats.setLastSerialisation(duration);
                final int size = remaining - bytes.remaining();
                stats.setLastSize(size);
            }

            return result;
        }
        catch (java.io.InvalidClassException e) {
            cat.error("Unable to read class "+e.classname+": "+e.getMessage(), e);
            return null;
        }
        catch (ClassNotFoundException e) {
            cat.error("Unable to parse object: "+e.getMessage(), e);
            return null;
        }
        catch (IOException e) {
            cat.error("Unable to parse object: "+e.getMessage(), e);
            return null;
        }
    }

    /**
     * Extension of the ObjectInputStream class that allows you to specify
     * which class loader to use to create your objects. This is important
     * when using a web application server, as this ObjectSerialiser class
     * is loaded by the app server's global class loader, while the
     * serialised objects may have to be loaded by the indivudual web
     * applications class loaders. 
     */
    private static class ObjectInputStream extends java.io.ObjectInputStream {

        /** Class loader instance (from web app). */
        public ClassLoader classLoader;

        /**
         * Creates a new object input stream.
         * @param istr Underlying input stream.
         * @throws IOException if the input stream cannot be read.
         */
        public ObjectInputStream(InputStream istr) throws IOException {
            super(istr);
            this.classLoader = this.getClass().getClassLoader();
        }

        /**
         * Creates a new object input stream.
         * @param istr Underlying input stream.
         * @param classLoader Class loader to use to create object instances.
         * @throws IOException if the input stream cannot be read.
         */
        public ObjectInputStream(InputStream istr, ClassLoader classLoader) throws IOException {
            super(istr);
            this.classLoader = classLoader;
        }

        /**
         * Overloaded method from base class that is responsible for finding
         * the class the obejct is to be instanciated from.
         */
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            String name = desc.getName();
            try {
                return Class.forName(name, false, classLoader);
            }
            catch (ClassNotFoundException ex) {
                if (debug) cat.debug("Unable to resolve class "+name+": "+ex.getMessage()+" - trying superclass before giving up...", ex);
                try {
                    return super.resolveClass(desc);
                }
                catch (ClassNotFoundException e) {
                    cat.error("Unable to resolve class "+name+": "+ex.getMessage(), ex);
                    throw e;
                }
                catch (IOException e) {
                    cat.error("Unable to resolve class "+name+": "+ex.getMessage(), ex);
                    throw e;
                }
            }
        }
        
    }

    /**
     * Encapsulates a GZIP output stream, allowing the compression level to
     * be defined (0=none, 1=fastest, 9=best).
     */
    private static class CompressedOutputStream extends GZIPOutputStream {

        /**
         * Creates a new compressed (gzip) output stream.
         * @param ostr Underlying output stream to write data to.
         * @throws IOException on any I/O error.
         */
        public CompressedOutputStream(OutputStream ostr) throws IOException {
            super(ostr);
        }

        /**
         * Sets the compression level for the gzip/deflate algorithm.
         * @param level Compression level, where 0=none, 1=fastest and 9=best.
         */
        public void setCompressionLevel(int level) {
            this.def.setLevel(level);
        }
        
    }

    /**
     * Encapsulates a GZIP input stream.
     */
    private static class CompressedInputStream extends GZIPInputStream {

        /**
         * Creates a new compressed input stream.
         * @param istr Input stream to read compressed data from.
         * @throws IOException on any I/O error.
         */
        public CompressedInputStream(InputStream istr) throws IOException {
            super(istr);
        }

    }

}
