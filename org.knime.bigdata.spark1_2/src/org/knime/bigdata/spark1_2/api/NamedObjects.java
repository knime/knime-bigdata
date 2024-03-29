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
package org.knime.bigdata.spark1_2.api;

import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.storage.StorageLevel;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public interface NamedObjects {

    /**
     * Adds the given RDD to the named RDDs (without forcing computation and storage level is {@link StorageLevel#NONE}).
     *
     * @param key
     * @param rdd
     */
    public  void addJavaRdd(final String key, final JavaRDD<Row> rdd);


    /**
     * Adds the given RDD to the named RDDs forcing computation as given (storage level is {@link StorageLevel#NONE}).
     *
     * @param key
     * @param rdd
     * @param forceComputation
     */
    public  void addJavaRdd(final String key, final JavaRDD<Row> rdd, final boolean forceComputation);


    /**
     * @param key
     * @param rdd
     * @param forceComputation
     * @param storageLevel
     */
    public  void addJavaRdd(final String key, final JavaRDD<Row> rdd, final boolean forceComputation, final StorageLevel storageLevel);



    /**
     * Adds the given RDD to the named RDDs forcing computation as given (storage level is {@link StorageLevel#NONE}).
     *
     * @param key
     * @param rdd
     * @param storageLevel
     */
    public  void addJavaRdd(final String key, final JavaRDD<Row> rdd, final StorageLevel storageLevel);


    /**
     * Retrieves a previously stored RDD as {@link RDD}.
     *
     * @param key the key under which the RDD was previously stored
     * @return the RDD as {@link RDD}.
     */
    public  JavaRDD<Row> getJavaRdd(final String key);


    /**
     * Checks whether an rdd is stored under the given key.
     *
     * @param key
     * @return true if stored, false otherwise.
     */
    public boolean validateNamedObject(final String key);


    /**
     * Deletes the given RDD from the map of named RDDs.
     *
     * @param key
     */
    public void deleteNamedObject(final String key);

    /**
     * @return Returns the set of all named objects
     */
    public Set<String> getNamedObjects();

    /**
     * Adds the given RDD to the named RDDs (without forcing computation and storage level is {@link StorageLevel#NONE}).
     *
     * @param key
     * @param rdd
     */
    public <T> void addRdd(final String key, final RDD<T> rdd);


    /**
     * Adds the given RDD to the named RDDs forcing computation as given (storage level is {@link StorageLevel#NONE}).
     *
     * @param key
     * @param rdd
     * @param forceComputation
     */
    public <T> void addRdd(final String key, final RDD<T> rdd, final boolean forceComputation);


    /**
     * @param key
     * @param rdd
     * @param forceComputation
     * @param storageLevel
     */
    public <T> void addRdd(final String key, final RDD<T> rdd, final boolean forceComputation, final StorageLevel storageLevel);



    /**
     * Adds the given RDD to the named RDDs forcing computation as given (storage level is {@link StorageLevel#NONE}).
     *
     * @param key
     * @param rdd
     * @param storageLevel
     */
    public <T> void addRdd(final String key, final RDD<T> rdd, final StorageLevel storageLevel);


    /**
     * Retrieves a previously stored RDD as {@link RDD}.
     *
     * @param key the key under which the RDD was previously stored
     * @return the RDD as {@link RDD}.
     */
    public <T> RDD<T> getRdd(final String key);
}
