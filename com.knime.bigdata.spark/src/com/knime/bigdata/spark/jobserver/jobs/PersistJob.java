/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.storage.StorageLevel;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * (Un)persists the given named RDD using the defined storage level.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PersistJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    /**Flag that indicates if the RDD should be unpersisted (<code>true</code>) or persisted (<code>false</code>).*/
    public static final String PARAM_UNPERSIST = "unpersist";
    /**Use disk flag.*/
    public static final String PARAM_USE_DISK = "useDisk";
    /**Use memory flag.*/
    public static final String PARAM_USE_MEMORY = "useMemory";
    /**Use off heap flag.*/
    public static final String PARAM_USE_OFF_HEAP = "useOffHeap";
    /**Deserialized flag.*/
    public static final String PARAM_DESERIALIZED = "deserialized";
    /**Number of replications.*/
    public static final String PARAM_REPLICATION = "replication";

    private static final String[] PERSIST_PARAMS = new String[] {PARAM_USE_DISK, PARAM_USE_MEMORY,
        PARAM_USE_OFF_HEAP, PARAM_DESERIALIZED, PARAM_REPLICATION};

    private final static Logger LOGGER = Logger.getLogger(PersistJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig config) {
        String msg = null;
        if (!config.hasInputParameter(PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + PARAM_INPUT_TABLE + "' missing.";
        }
        if (!config.hasInputParameter(PARAM_UNPERSIST)) {
            msg = "Input parameter '" + PARAM_UNPERSIST + "' missing.";
        }
        final boolean unpersist = config.getInputParameter(PARAM_UNPERSIST, Boolean.class);
        if (!unpersist) {
            for (String param : PERSIST_PARAMS) {
                if (!config.hasInputParameter(param)) {
                    msg = "Input parameter '" + param + "' missing.";
                }
            }
        }
        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * run the actual job, the result is serialized back to the client the true result is a Hive data table that
     * contains the data of the incoming rdd
     *
     * @return the JobResult
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        final boolean unpersist = aConfig.getInputParameter(PARAM_UNPERSIST, Boolean.class);
        if (unpersist) {
            LOGGER.log(Level.INFO, "unpersist RDD...");
            rowRDD.unpersist();
        } else {
            final boolean useDisk = aConfig.getInputParameter(PARAM_USE_DISK, Boolean.class);
            final boolean useMemory = aConfig.getInputParameter(PARAM_USE_MEMORY, Boolean.class);
            final boolean useOffHeap = aConfig.getInputParameter(PARAM_USE_OFF_HEAP, Boolean.class);
            final boolean deserialized = aConfig.getInputParameter(PARAM_DESERIALIZED, Boolean.class);
            final int replication = aConfig.getInputParameter(PARAM_REPLICATION, Integer.class);
            final StorageLevel level = StorageLevels.create(useDisk, useMemory, useOffHeap, deserialized, replication);
            LOGGER.log(Level.INFO, "persist RDD with storage level: " + level.description());
            rowRDD.persist(level);
        }
        return JobResult.emptyJobResult().withMessage("OK");
    }
}
