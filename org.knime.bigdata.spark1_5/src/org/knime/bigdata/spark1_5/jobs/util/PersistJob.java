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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark1_5.jobs.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.util.rdd.persist.PersistJobInput;
import org.knime.bigdata.spark1_5.api.NamedObjects;
import org.knime.bigdata.spark1_5.api.SimpleSparkJob;

/**
 * (Un)persists the given named RDD using the defined storage level.
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class PersistJob implements SimpleSparkJob<PersistJobInput> {

    private static final long serialVersionUID = 1L;
    private final static Logger LOGGER = Logger.getLogger(PersistJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public void runJob(final SparkContext sparkContext, final PersistJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {
        final String name = input.getFirstNamedInputObject();
        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(name);
        final StorageLevel level = StorageLevels.create(input.useDisk(), input.useMemory(), input.useOffHeap(),
            input.deserialized(), input.getReplication());
        LOGGER.log(Level.INFO, "Persisting: " + name + " with storage level: " + level.description());
        rowRDD.persist(level);
        LOGGER.info(name + " successful persisted");
    }
}
