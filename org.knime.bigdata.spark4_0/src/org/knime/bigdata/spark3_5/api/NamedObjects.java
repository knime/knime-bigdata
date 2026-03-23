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
 *   Created on Apr 5, 2016 by bjoern
 */
package org.knime.bigdata.spark3_5.api;

import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Interface for storing and retrieving "named objects" in Spark. This interface is used by Spark jobs to retrieve and
 * store data frames for example.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @author Sascha Wolke, KNIME GmbH
 * @author Nico Siebert, KNIME GmbH
 */
@SparkClass
public interface NamedObjects {

    /**
     * Adds a named object under a given key.
     *
     * @param key The key under which to store the named object.
     * @param obj the named object to add.
     */
    public <T> void add(final String key, T obj);

    /**
     * Get the named object by key.
     *
     * @param key The key under which the named object was previously stored.
     * @return the named object.
     */
    public <T> T get(final String key);

    /**
     * Delete the named object by key.
     *
     * @param key The key under which the named object was previously stored.
     * @return the named object that was deleted, or null, if none was deleted.
     */
    public <T> T delete(final String key);

    /**
     * Adds the given data frame to the named objects.
     *
     * @param key The key under which the named object was previously stored.
     * @param dataset The data frame to add.
     */
    public void addDataFrame(final String key, final Dataset<Row> dataset);

    /**
     * Retrieves a previously stored data frame.
     *
     * @param key The key under which the data frame was previously stored.
     * @return the data frame.
     */
    public Dataset<Row> getDataFrame(final String key);

    /**
     * Retrieves a previously stored data frame as an RDD.
     *
     * @param key the key under which the RDD was previously stored.
     * @return the RDD as {@link RDD}.
     * @see #getDataFrame(String)
     */
    public JavaRDD<Row> getJavaRdd(final String key);

    /**
     * Checks whether a named object is stored under the given key.
     *
     * @param key The key under which to look up the named object.
     * @return true if stored, false otherwise.
     */
    public boolean validateNamedObject(final String key);

    /**
     * Deletes the given named object from the map of named objects.
     *
     * @param key The key under which the named object was previously stored.
     */
    public void deleteNamedDataFrame(final String key);

    /**
     * @return the keys of all currently stored named objects.
     */
    public Set<String> getNamedObjects();
}
