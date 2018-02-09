package org.knime.bigdata.spark2_1.base;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark2_1.api.NamedObjects;

/**
 * Implementation of {@link NamedObjects} that is backed by a hash map.
 *
 * @author Nico Siebert, KNIME GmbH
 */
@SparkClass
public class NamedObjectsImpl implements NamedObjects {

    private final Map<String, Object> namedObjects = new HashMap<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void add(final String key, final Object obj) {
        namedObjects.put(key, obj);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized <T> T get(final String key) {
        return (T)namedObjects.get(key);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized <T> T delete(final String key) {
        return (T)namedObjects.remove(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void addDataFrame(final String key, final Dataset<Row> dataset) {
        namedObjects.put(key, dataset);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized Dataset<Row> getDataFrame(final String key) {
        return (Dataset<Row>)namedObjects.get(key);
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
        return namedObjects.containsKey(key);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized void deleteNamedDataFrame(final String key) {
        final Dataset<Row> frame = (Dataset<Row>) namedObjects.remove(key);
        if (frame != null) {
            frame.unpersist();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Set<String> getNamedObjects() {
        return new HashSet<>(namedObjects.keySet());
    }
}
