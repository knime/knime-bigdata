/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Apr 8, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.job;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 *
 * This class is a thin wrapper around a map (the "internal map") and meant to simplify data exchange between the KNIME
 * Extension for Apache Spark and Spark jobs running in a Spark environment (possibly on a remote server). Do not use this
 * class directly, please make a subclass of {@link JobInput} to ship parameters to your Spark job, and one of
 * {@link JobOutput} to ship output back into KNIME.
 *
 * The internal map associates String keys with values. The {@link #set(String, Object)}, {@link #get(String)} and
 * {@link #getOrDefault(String, Object)} methods provide access to the internal map. All keys will be automatically
 * prefixed with a key prefix given to the constructor. This allows you to have multiple instances of this class backed
 * by the same internal map, working with different key prefixes. Note that however no thread-safety will be guaranteed!
 *
 *
 * Please also note the following implicit restrictions on the values in the internal map.
 *
 * <p>
 * RESTRICTION 1: If possible, please avoid storing values of other types than the following in the internal map:
 * <ul>
 * <li>Boxed primitives types (Long, Integer, Short, Byte, Double, Float, Boolean, Char)</li>
 * <li>Instances of {@link List} that contain objects of the supported types</li>
 * <li>Instances of {@link Map}, that map Strings keys to objects of the supported types.</li>
 * </ul>
 *
 * Not that you /CAN/ use other types. But the transport mechanism between KNIME and the Spark environment may be
 * inefficient for them (e.g. for Spark jobserver, non-JSON compatible values need to be serialized and then Base64
 * encoded).
 * </p>
 *
 * <p>
 * RESTRICTION 2: Only use small values. The transport mechanism between KNIME and the Spark environment may go over the
 * network, hence serializing the internal map should not result into a lot of data, e.g. less than 5 MB.
 * </p>
 *
 * @see JobInput
 * @see JobOutput
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class JobData {

    private final static String KEY_NAMED_OBJECT_SPECS = "specs";

    private final static String INTERNAL_PREFIX = "x";

    private Map<String, Object> m_internalMap;

    private final String m_keyPrefix;

    /**
     * Creates an instance of this class with an empty internal map.
     *
     * @param keyPrefix prefix that will be automatically prepended to all keys
     */
    public JobData(final String keyPrefix) {
        this(keyPrefix, new HashMap<String, Object>());
    }

    /**
     * Creates an instance of this class backed by the given internal map.
     *
     * @param keyPrefix prefix that will be automatically prepended to all keys
     * @param internalMap backing map to use
     */
    public JobData(final String keyPrefix, final Map<String, Object> internalMap) {
        m_keyPrefix = keyPrefix;
        m_internalMap = internalMap;
    }

    /**
     * Sets the given key and value into the internal map.
     *
     * @param key
     * @param value
     * @return the previous value associated with the key (if any)
     */
    @SuppressWarnings("unchecked")
    protected <T> T set(final String key, final T value) {
        return (T)m_internalMap.put(String.format("%s.%s.%s", INTERNAL_PREFIX, m_keyPrefix, key), value);
    }

    /**
     * @param key
     * @return {@link Integer} value
     */
    protected Integer getInteger(final String key) {
        final Number num = getNumberInternal(key);
        if (num == null) {
            return null;
        }
        return Integer.valueOf(num.intValue());
    }

    /**
     * @param key
     * @return {@link Long} value
     */
    protected Long getLong(final String key) {
        final Number num = getNumberInternal(key);
        if (num == null) {
            return null;
        }
        return Long.valueOf(num.longValue());
    }

    /**
     * @param key
     * @return {@link Double} value
     */
    protected Double getDouble(final String key) {
        final Number num = getNumberInternal(key);
        if (num == null) {
            return null;
        }
        return Double.valueOf(num.doubleValue());
    }

    /**
     * @param key
     * @return the {@link Number} value
     */
    protected Number getNumber(final String key) {
        return getNumberInternal(key);
    }

    private Number getNumberInternal(final String key) {
        return (Number)m_internalMap.get(String.format("%s.%s.%s", INTERNAL_PREFIX, m_keyPrefix, key));
    }

    /**
     * Retrieves the value associated with the given key from the internal map.
     *
     * @param key
     * @return the value associated with the key, or null if no value is associated with the key
     * @see #getNumber(String) for numerical methods
     * @throws IllegalArgumentException if the returned value is instance of {@link Number}
     */
    protected <T> T get(final String key) {
        return getInternal(key);
    }

    @SuppressWarnings("unchecked")
    private <T> T getInternal(final String key) {
        final Object val = m_internalMap.get(String.format("%s.%s.%s", INTERNAL_PREFIX, m_keyPrefix, key));
        if (val != null && val instanceof Number) {
            throw new IllegalArgumentException("Instance of Number not supported. Use dedicated methods e.g. getLong()");
        }
        return (T)val;
    }

    /**
     * Retrieves the value associated with the given key from the internal map, or returns the provided default value if
     * no value os associated with the key.
     *
     * @param key
     * @param defaultValue
     * @return the value associated with the key (if any), or the provided default value
     */
    protected <T> T getOrDefault(final String key, final T defaultValue) {
        T toReturn = getInternal(key);
        if (toReturn == null) {
            toReturn = defaultValue;
        }

        return toReturn;
    }

    /**
     * Checks whether the given key has an association with a value in the internal map.
     *
     * @param key
     * @return true if key has an association with a value, false otherwise
     */
    protected boolean has(final String key) {
        return m_internalMap.containsKey(String.format("%s.%s.%s", INTERNAL_PREFIX, m_keyPrefix, key));
    }

    /**
     * Retrieves a map with all {@link IntermediateSpec}s.
     *
     * @return an immutable map, mapping named object IDs to their respective {@link IntermediateSpec}s
     */
    @SuppressWarnings("unchecked")
    public Map<String, IntermediateSpec> getAllSpecs() {
        Map<String, IntermediateSpec> specs = (Map<String, IntermediateSpec>) m_internalMap.get(KEY_NAMED_OBJECT_SPECS);
        if (specs == null) {
            specs = Collections.emptyMap();
        }

        return Collections.unmodifiableMap(specs);
    }

    /**
     * Retrieves the {@link IntermediateSpec} of a named object.
     *
     * @param namedObjectId Id of the named object
     * @return the {@link IntermediateSpec} of the named object, null if there is no {@link IntermediateSpec} for the given
     *         named object.
     */
    public IntermediateSpec getSpec(final String namedObjectId) {
        return getAllSpecs().get(namedObjectId);
    }

    /**
     * Adds all data of the given {@link JobData} into this job data into the internal, under the given key.
     *
     * @param key The key under which to store the given {@link JobData}.
     * @param other The {@link JobData} to add.
     */
    public void setJobData(final String key, final JobData other) {
        for (String otherKey : other.m_internalMap.keySet()) {
            m_internalMap.put(String.format("%s.%s.%s.%s", INTERNAL_PREFIX, m_keyPrefix, key, otherKey),
                other.m_internalMap.get(otherKey));
        }
    }

    /**
     * Retrieves a {@link JobData} object that was previously stored with the given key.
     *
     * @param key The key from which to restore the {@link JobData} object.
     * @param jobDataClass The concrete {@link JobData} class to instantiate.
     * @return a new {@link JobData} of type T.
     * @throws RuntimeException if jobDataClass could not be instantiated.
     */
    public <T extends JobData> T getJobData(final String key, final Class<T> jobDataClass) {
        try {
            final HashMap<String, Object> otherInternalMap = new HashMap<>();

            final String prefix = String.format("%s.%s.%s.", INTERNAL_PREFIX, m_keyPrefix, key);
            for (String internalKey : m_internalMap.keySet()) {
                if (internalKey.startsWith(prefix)) {
                    otherInternalMap.put(internalKey.substring(prefix.length()), m_internalMap.get(internalKey));
                }
            }

            final T other = jobDataClass.newInstance();
            other.setInternalMap(otherInternalMap);
            return other;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Add a {@link IntermediateSpec} to this instance.
     *
     * @param namedObjectId Id of the named object.
     * @param spec The {@link IntermediateSpec} of the named object.
     * @return this object, with additional mapping from the given namedObjectId to the given {@link IntermediateSpec}
     */
    @SuppressWarnings("unchecked")
    public <T extends JobData> T withSpec(final String namedObjectId, final IntermediateSpec spec) {
        Map<String, IntermediateSpec> specs = (Map<String, IntermediateSpec>)m_internalMap.get(KEY_NAMED_OBJECT_SPECS);
        if (specs == null) {
            specs = new HashMap<>();
            m_internalMap.put(KEY_NAMED_OBJECT_SPECS, specs);
        }

        specs.put(namedObjectId, spec);
        return (T)this;
    }

    /**
     * Compares the underlying map to the one of the given object (if it also an instance of {@link JobData}).
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        JobData rhs = (JobData)obj;
        return m_internalMap.equals(rhs.m_internalMap);
    }

    /**
     *
     * Returns the hash code of the underlying map (which is computed from the map's contents).
     */
    @Override
    public int hashCode() {
        return m_internalMap.hashCode();
    }

    /**
     * ONLY FOR SERIALIZATION. If you are using this method your are probably doing something wrong. Returns the
     * internal map for serialization purposes.
     *
     * @return the internal map (mutable).
     */
    public Map<String, Object> getInternalMap() {
        return m_internalMap;
    }

    /**
     * ONLY FOR DESERIALIZATION. If you are using this method your are probably doing something wrong. Sets the internal
     * map.
     *
     * @param newInternalMap
     */
    public void setInternalMap(final Map<String, Object> newInternalMap) {
        m_internalMap = newInternalMap;
    }
}
