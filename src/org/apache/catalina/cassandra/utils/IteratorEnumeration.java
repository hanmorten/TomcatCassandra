// @(#)$Id: IteratorEnumeration.java,v 1.3 2012/04/13 16:07:47 morten Exp $
package org.apache.catalina.cassandra.utils;

import java.util.Enumeration;
import java.util.Iterator;

import org.apache.log4j.Logger;

/**
 * <ul>
 *   <li><b>Project:</b> TomcatCassandra</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.utils.IteratorEnumeration</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 11 Oct 2011 15:27:34</li>
 *   <li><b>Description:</b>
 *     Simple utility class that maps an iterator to an enumeration.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
public class IteratorEnumeration implements Enumeration {

    /** Log4J logger instance for class IteratorEnumeration. */
    private final static Logger cat = Logger .getLogger(IteratorEnumeration.class);
    /** Log4J debug setting for class IteratorEnumeration. */
    private final static boolean debug = cat.isDebugEnabled();

    /** Iterator to read from. */
    private Iterator iterator;

    /**
     * Creates an enumeration from an iterator.
     * @param iterator Underlying iterator.
     */
    public IteratorEnumeration(Iterator iterator) {
        this.iterator = iterator;
    }

    /*
     * (non-Javadoc)
     * @see java.util.Enumeration#hasMoreElements()
     */
    public boolean hasMoreElements() {
        return iterator.hasNext();
    }

    /*
     * (non-Javadoc)
     * @see java.util.Enumeration#nextElement()
     */
    public Object nextElement() {
        return iterator.next();
    }

}
