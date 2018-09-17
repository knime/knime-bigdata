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
 *   Created on 13.08.2018 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark2_2.jobs.scripting.python;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Class for Exchanging Java Objects between PySpark and Java
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
@SparkClass
public class PySparkDataExchanger {
    /**
     * Singleton instance of this implementation class for use by Spark and PySpark
     */
    public static final PySparkDataExchanger SINGLETON_INSTANCE = new PySparkDataExchanger();

    private final Map<String, Object> m_objectsMap = new HashMap<>();

    private JavaSparkContext m_context;

    private SparkSession m_sparkSession;

    /**
     * Private constructor for singleton.
     */
    private PySparkDataExchanger() {
    }

    /**
     * Sets the java context
     *
     * @param con
     */
    public void setContext(final JavaSparkContext con) {
        m_context = con;
    }

    /**
     * @return the Java Spark Context
     */
    public JavaSparkContext getContext() {
        return m_context;
    }

    /**
     * Set the Session
     *
     * @param session the Spark Session to set
     */
    public void setSession(final SparkSession session) {
        m_sparkSession = session;
    }

    /**
     * @return the spark session
     */
    public SparkSession getSession() {
        return m_sparkSession;
    }

    /**
     * Adds a named object under a given key.
     *
     * @param key The key under which to store the named object.
     * @param obj the named object to add.
     */
    public synchronized void add(final String key, final Object obj) {
        m_objectsMap.put(key, obj);
    }

    /**
     * Get the named object by key.
     *
     * @param key The key under which the named object was previously stored.
     * @return the named object.
     */
    @SuppressWarnings("unchecked")
    public synchronized <T> T get(final String key) {
        return (T)m_objectsMap.get(key);
    }

    /**
     * Delete the named object by key.
     *
     * @param key The key under which the named object was previously stored.
     * @return the named object that was deleted, or null, if none was deleted.
     */
    @SuppressWarnings("unchecked")
    public synchronized <T> T delete(final String key) {
        return (T)m_objectsMap.remove(key);
    }

    /**
     * Adds the given data frame to the named objects.
     *
     * @param key The key under which the named object was previously stored.
     * @param dataset The data frame to add.
     */
    public synchronized void addDataFrame(final String key, final Dataset<Row> dataset) {
        m_objectsMap.put(key, dataset);
    }

    /**
     * Retrieves a previously stored data frame.
     *
     * @param key The key under which the data frame was previously stored.
     * @return the data frame.
     */
    @SuppressWarnings("unchecked")
    public synchronized Dataset<Row> getDataFrame(final String key) {
        return (Dataset<Row>)m_objectsMap.get(key);
    }

    /**
     * Checks whether a named object is stored under the given key.
     *
     * @param key The key under which to look up the named object.
     * @return true if stored, false otherwise.
     */
    public synchronized boolean validateNamedObject(final String key) {
        return m_objectsMap.containsKey(key);
    }

    /**
     * Deletes the given named object from the map of named objects.
     *
     * @param key The key under which the named object was previously stored.
     */
    @SuppressWarnings("unchecked")
    public synchronized void deleteNamedDataFrame(final String key) {
        final Dataset<Row> frame = (Dataset<Row>)m_objectsMap.remove(key);
        if (frame != null) {
            frame.unpersist();
        }
    }
}
