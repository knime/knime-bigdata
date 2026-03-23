package org.knime.bigdata.spark3_5.base;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark3_5.api.NamedObjects;

/**
 * Implementation of {@link NamedObjects} that is backed by a hash map.
 *
 * @author Nico Siebert, KNIME GmbH
 */
@SparkClass
class NamedObjectsImpl implements NamedObjects {

    /**
     * Singleton instance of this implementation class for use by Spark job binding classes.
     */
    static final NamedObjects SINGLETON_INSTANCE = new NamedObjectsImpl();

    private final Map<String, Object> m_namedObjects = new HashMap<>();

    /**
     * Private constructor for singleton.
     */
    private NamedObjectsImpl() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void add(final String key, final Object obj) {
        m_namedObjects.put(key, obj);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized <T> T get(final String key) {
        return (T)m_namedObjects.get(key);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized <T> T delete(final String key) {
        final T namedObject = (T) m_namedObjects.get(key);
        if (namedObject != null && namedObject instanceof Dataset) {
            deleteNamedDataFrame(key);
        } else {
            m_namedObjects.remove(key);
        }
        return namedObject;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void addDataFrame(final String key, final Dataset<Row> dataset) {
        m_namedObjects.put(key, dataset);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized Dataset<Row> getDataFrame(final String key) {
        return (Dataset<Row>)m_namedObjects.get(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized JavaRDD<Row> getJavaRdd(final String key) {
        return getDataFrame(key).toJavaRDD();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean validateNamedObject(final String key) {
        return m_namedObjects.containsKey(key);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized void deleteNamedDataFrame(final String key) {
        final Dataset<Row> frame = (Dataset<Row>) m_namedObjects.remove(key);
        if (frame != null) {
            frame.unpersist();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Set<String> getNamedObjects() {
        return new HashSet<>(m_namedObjects.keySet());
    }

    static void ensureNamedOutputObjectsDoNotExist(final JobInput input) throws KNIMESparkException {
        // validate named output objects do not exist
        for (String namedOutputObject : input.getNamedOutputObjects()) {
            if (SINGLETON_INSTANCE.validateNamedObject(namedOutputObject)) {
                throw new KNIMESparkException(
                    "Spark RDD/DataFrame to create already exists. Please reset all preceding nodes and reexecute.");
            }
        }
    }

    static void ensureNamedInputObjectsExist(final JobInput input) throws KNIMESparkException {
        for (String namedInputObject : input.getNamedInputObjects()) {
            if (!SINGLETON_INSTANCE.validateNamedObject(namedInputObject)) {
                throw new KNIMESparkException(
                    "Missing input Spark RDD/DataFrame. Please reset all preceding nodes and reexecute.");
            }
        }
    }
}
