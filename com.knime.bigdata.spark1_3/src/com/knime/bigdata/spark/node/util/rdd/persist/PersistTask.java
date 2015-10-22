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
 *   Created on 13.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.util.rdd.persist;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.PersistJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.data.SparkDataTable;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PersistTask {

    private String predictorDef(final String inputID, final boolean useDisk,
        final boolean useMemory, final boolean useOffHeap, final boolean deserialized, final int replication)
                throws GenericKnimeSparkException {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new Object[]{
                KnimeSparkJob.PARAM_INPUT_TABLE, inputID,
                PersistJob.PARAM_UNPERSIST, false,
                PersistJob.PARAM_USE_DISK, useDisk,
                PersistJob.PARAM_USE_MEMORY, useMemory,
                PersistJob.PARAM_USE_OFF_HEAP, useOffHeap,
                PersistJob.PARAM_DESERIALIZED, deserialized,
                PersistJob.PARAM_REPLICATION, replication},
                ParameterConstants.PARAM_OUTPUT,
                    new String[]{}});
    }

    /**
     * @param exec {@link ExecutionMonitor} for progress and cancellation
     * @param data the {@link SparkDataTable} to persist
     * @param useDisk use disk flag
     * @param useMemory use memory flag
     * @param useOffHeap use off heap flag
     * @param deserialized use deserialized flag
     * @param replication number of replications
     * @throws GenericKnimeSparkException
     * @throws CanceledExecutionException
     */
    public void execute(final ExecutionMonitor exec, final SparkDataTable data, final boolean useDisk,
        final boolean useMemory, final boolean useOffHeap, final boolean deserialized, final int replication) throws GenericKnimeSparkException, CanceledExecutionException {
        final String jasonParams = predictorDef(data.getID(), useDisk, useMemory, useOffHeap, deserialized, replication);
        if (exec != null) {
            exec.checkCanceled();
        }
        JobControler.startJobAndWaitForResult(data.getContext(), PersistJob.class.getCanonicalName(),
            jasonParams, exec);
        return ;
    }

}
