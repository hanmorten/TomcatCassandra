// @(#)$Id: SessionMap.java,v 1.3 2012/04/13 16:07:47 morten Exp $
package org.apache.catalina.cassandra.utils;

import java.util.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;

/**
 * <ul>
 *   <li><b>Project:</b> TomcatCassandra</li>
 *   <li><b>Class:</b> org.apache.catalina.cassandra.SessionMap</li>
 *   <li><b>Author:</b> morten.jorgensen@openjawtech.com</li>
 *   <li><b>Created:</b> 18 Nov 2011 10:49:59</li>
 *   <li><b>Description:</b>
 *     This is an implementation of the Map interface that piggybacks on an
 *     HTTP session. This map accesses the current HTTP session via the
 *     org.apache.catalina.cassandra.utils.RequestHolderValve class, which
 *     <b>must</b> be installed as a filter for this class to work.
 *   </li>
 *   <li><b>Functional spec. ref.:</b> </li>
 *   <li><b>Design spec.ref.:</b> </li>
 * </ul>
 */
public class SessionMap<K,V> implements Map<K,V>, java.io.Serializable {

    /** Log4J logger instance for class SessionMap. */
    private final static Logger cat = Logger.getLogger(SessionMap.class);
    /** Log4J debug setting for class SessionMap. */
    private final static boolean debug = cat.isDebugEnabled();

    /** Prefix to use for all data stored in the HTTP session for this map. */
    private String prefix;

    /**
     * Name of HTTP session object that stores the names of the keys in this
     * map. The keys are stored in a set of String objects.
     */
    private String keys;

    /**
     * Creates a new session map. Note that the map overlays the current HTTP
     * session, and that the map therefore may have "contained" data as soon
     * as this object is instantiated.  
     * @param prefix HTTP session object prefix.
     */
    public SessionMap(String prefix) {
        this.prefix = prefix + "##VALUE##";
        this.keys = prefix + "##KEYS##";
    }

    /**
     * Maps names used for this map to HTTP session object names.
     * @param name Object name used for this Map implementation.
     * @return Object name used for the HTTP session.
     */
    private String _(Object name) {
        return this.prefix + name.toString();
    }

    /**
     * Returns the current HTTP servlet session.
     * @return HTTP session container.
     */
    private HttpSession getSession() {
        final HttpServletRequest request = RequestHolderValve.getCurrentRequest();
        if (request == null) return null;
        return request.getSession();
    }

    /**
     * Reads the set of key names from the HTTP session.
     * @return Set of key names.
     */
    private Set<K> getKeys() {
        final HttpSession session = this.getSession();
        Set<K> keys = (Set<K>)session.getAttribute(this.keys);
        if (keys == null) {
            keys = new HashSet<K>();
            session.setAttribute(this.keys, keys);
        }
        return keys;
    }

    /**
     * Updates the key names set in the HTTP session.
     * @param keys Set of key names.
     */
    private void setKeys(Set<K> keys) {
        final HttpSession session = this.getSession();
        session.setAttribute(this.keys, keys);
    }
    
    /**
     * Removes name/value pairs in this map from the underlying HTTP session.
     */
    public void clear() {
        final HttpSession session = this.getSession();

        // Get the list of keys
        final Set<K> keys = this.getKeys();
        
        // Iterate over the keys and remove them from the session.
        final Iterator<K> names = keys.iterator();
        while (names.hasNext()) {
            final K name = names.next();
            session.removeAttribute(_(name));
        }

        // Clear the set of keys.
        this.setKeys(new HashSet<K>());
    }

    /**
     * Returns true if this map contains a given key.
     * @param key Name of object to look for.
     * @return true if this map contains a given key.
     */
    public boolean containsKey(Object key) {
        final HttpSession session = this.getSession();
        return (session.getAttribute(_(key)) != null);
    }

    /**
     * Returns true if this map contains a given value.
     * <b>NOTE:</b> This is a very heavy operation and must be avoided!
     * @param value Object value to look for.
     * @return true if the map contains the given value.
     */
    public boolean containsValue(Object value) {
        final Set<K> keys = this.getKeys();
        final Iterator<K> names = keys.iterator();
        while (names.hasNext()) {
            final K name = names.next();
            final Object obj = this.get(name);
            if (obj.equals(value)) return true;
        }
        return false;
    }

    /**
     * Returns a list of all contained name/value pairs.
     * <b>NOTE:</b> This is a very heavy operation and must be avoided!
     * @return Set containing all values in this map.
     */
    public Set<Map.Entry<K, V>> entrySet() {
        final Set<Map.Entry<K,V>> set = new HashSet<Map.Entry<K, V>>();
        final Set<K> keys = this.getKeys();
        final Iterator<K> names = keys.iterator();
        while (names.hasNext()) {
            final K key = names.next();
            final V value = this.get(key);
            set.add(new Entry<K, V>(this, key, value));
        }
        return set;
    }

    /**
     * Inner class to contain map entries. Used only for implementation of
     * entrySet() method above.
     */
    private class Entry<K,V> implements Map.Entry<K, V> {
        
        private SessionMap<K,V> owner;

        private K key;
        private V value;
        
        public Entry(SessionMap<K,V> owner, K key, V value) {
            this.owner = owner;
            this.key = key;
            this.value = value;
        }
        
        public K getKey() {
            return this.key;
        }
        
        public V getValue() {
            return this.value;
        }
        
        public V setValue(V value) {
            final V prev = this.value;
            this.value = value;
            this.owner.put(this.key, value);
            return prev;
        }
        
        public boolean equals(Object other) {
            if (other instanceof SessionMap.Entry) {
                final SessionMap.Entry entry = (SessionMap.Entry)other;
                if (!entry.getKey().equals(key)) return false;
                if (!entry.getValue().equals(value)) return false;
                return true;
            }
            else {
                return false;
            }
        }
        
        public int hashCode() {
            return (this.getKey()==null   ? 0 : this.getKey().hashCode()) ^ (this.getValue()==null ? 0 : this.getValue().hashCode());            
        }

    }
    
    
    /**
     * Obtains a single value from the map.
     * @param key Name of object.
     * @return Object instance, or null.
     */
    public V get(Object key) {
        final HttpSession session = this.getSession();
        return (V)session.getAttribute(_((K)key));
    }

    /**
     * Returns <tt>true</tt> if this map contains no key-value mappings.
     * @return <tt>true</tt> if this map contains no key-value mappings.
     */
    public boolean isEmpty() {
        return this.getKeys().size() > 0;
    }

    /**
     * Returns the set of keys in this map.
     * @return the set of keys in this map.
     */
    public Set<K> keySet() {
        return this.getKeys();
    }

    /**
     * Inserts an object into this map.
     * @param key Object key.
     * @param value Object value.
     */
    public V put(K key, V value) {
        final HttpSession session = this.getSession();
        final V prev = (V)session.getAttribute(_(key));

        session.setAttribute(_(key), value);

        final Set<K> keys = this.getKeys();
        keys.add(key);
        this.setKeys(keys);

        return prev;
    }

    /**
     * Copies all of the mappings from the specified map to this map
     * (optional operation).  The effect of this call is equivalent to that
     * of calling {@link #put(Object,Object) put(k, v)} on this map once
     * for each mapping from key <tt>k</tt> to value <tt>v</tt> in the 
     * specified map.  The behavior of this operation is unspecified if the
     * specified map is modified while the operation is in progress.
     * @param t Mappings to be stored in this map.
     */
    public void putAll(Map<? extends K, ? extends V> t) {
        final Iterator<? extends K> keys = t.keySet().iterator();
        while (keys.hasNext()) {
            final K key = keys.next();
            final V value = t.get(key);
            this.put(key, value);
        }
    }

    /**
     * Removes a given value from the map.
     * @param key Value key.
     * @return Previous value for the given key, or null.
     */
    public V remove(Object key) {
        final HttpSession session = this.getSession();
        final V prev = (V)session.getAttribute(_(key));
        
        session.removeAttribute(_(key));
        
        final Set<K> keys = this.getKeys();
        keys.remove(key);
        this.setKeys(keys);
        
        return prev;
    }

    /**
     * Returns the size of this map.
     * @return size of this map.
     */
    public int size() {
        return this.getKeys().size();
    }

    /**
     * Returns all values in this map.
     * @return all values in this map.
     */
    public Collection<V> values() {
        final Set<V> set = new HashSet<V>();
        final Set<K> keys = this.getKeys();
        final Iterator<K> names = keys.iterator();
        while (names.hasNext()) {
            final K key = names.next();
            final V value = this.get(key);
            set.add(value);
        }
        return set;
    }

}
