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
 *   Created on Feb 7, 2017 by Sascha Wolke, KNIME.com
 */
package org.knime.bigdata.spark1_6.jobs;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import org.knime.bigdata.spark1_6.api.NamedObjects;

/**
 * Simple {@link HashMap} based named objects handler.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class SimpleNamedObjectHandler implements NamedObjects, Serializable {
    private static final long serialVersionUID = 1L;
    private HashMap<String, JavaRDD<Row>> store;

    /** Default constructor */
    public SimpleNamedObjectHandler() {
        store = new HashMap<>();
    }

    /** Clear all named objects */
    public void clear() {
        store.clear();
    }

    @Override
    public void addJavaRdd(final String key, final JavaRDD<Row> rdd) {
        store.put(key, rdd);
    }

    @Override
    public void addJavaRdd(final String key, final JavaRDD<Row> rdd, final boolean forceComputation) {
        store.put(key, rdd);
    }

    @Override
    public void addJavaRdd(final String key, final JavaRDD<Row> rdd, final boolean forceComputation, final StorageLevel storageLevel) {
        store.put(key, rdd);
    }

    @Override
    public void addJavaRdd(final String key, final JavaRDD<Row> rdd, final StorageLevel storageLevel) {
        store.put(key, rdd);
    }

    @Override
    public JavaRDD<Row> getJavaRdd(final String key) {
        return store.get(key);
    }

    @Override
    public boolean validateNamedObject(final String key) {
        return store.containsKey(key);
    }

    @Override
    public void deleteNamedObject(final String key) {
        store.remove(key);
    }

    @Override
    public Set<String> getNamedObjects() {
        return store.keySet();
    }

    @Override
    public <T> void addRdd(final String key, final RDD<T> rdd) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> void addRdd(final String key, final RDD<T> rdd, final boolean forceComputation) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> void addRdd(final String key, final RDD<T> rdd, final boolean forceComputation, final StorageLevel storageLevel) {
        throw new RuntimeException("Not implemented");

    }
    @Override
    public <T> void addRdd(final String key, final RDD<T> rdd, final StorageLevel storageLevel) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> RDD<T> getRdd(final String key) {
        throw new RuntimeException("Not implemented");
    }
}
