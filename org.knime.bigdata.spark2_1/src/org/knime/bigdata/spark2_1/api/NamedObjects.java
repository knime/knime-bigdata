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
package org.knime.bigdata.spark2_1.api;

import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * @author Bjoern Lohrmann, KNIME.com
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public interface NamedObjects {

    /**
     * Adds the given data frame (without forcing computation and storage level is {@link StorageLevel#NONE}).
     *
     * @param key
     * @param dataset
     */
    public void addDataFrame(final String key, final Dataset<Row> dataset);

    /**
     * Retrieves a previously stored data frame.
     *
     * @param key the key under which the data frame was previously stored
     * @return the data frame
     */
    public Dataset<Row> getDataFrame(final String key);

    /**
     * Retrieves a previously stored RDD as {@link RDD}.
     *
     * @param key the key under which the RDD was previously stored
     * @return the RDD as {@link RDD}.
     * @see #getDataFrame(String)
     */
    @Deprecated
    public  JavaRDD<Row> getJavaRdd(final String key);


    /**
     * Checks whether an object is stored under the given key.
     *
     * @param key
     * @return true if stored, false otherwise.
     */
    public boolean validateNamedObject(final String key);


    /**
     * Deletes the given object from the map of named objects.
     *
     * @param key
     */
    public void deleteNamedDataFrame(final String key);

    /**
     * @return Returns the set of all named objects
     */
    public Set<String> getNamedObjects();
}
