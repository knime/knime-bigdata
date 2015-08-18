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
 *   Created on 18.08.2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.partition;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.jobs.SamplingJob;
import com.knime.bigdata.spark.node.preproc.sampling.SparkSamplingNodeModel;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDs;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkPartionNodeModel extends SparkSamplingNodeModel {


    /**
     * Constructor.
     */
    public SparkPartionNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rdd = (SparkDataPortObject) inData[0];
        final String partition1 = SparkIDs.createRDDID();
        final String partition2 = SparkIDs.createRDDID();
        final String paramInJson = paramDef(rdd, getSettings(), partition1, partition2);
        final KNIMESparkContext context = rdd.getContext();
        exec.checkCanceled();
        exec.setMessage("Start Spark sampling job...");
        final String jobId = JobControler.startJob(context, SamplingJob.class.getCanonicalName(), paramInJson);
        JobControler.waitForJobAndFetchResult(context, jobId, exec);
        final SparkDataTable result1 = new SparkDataTable(context, partition1, rdd.getTableSpec());
        final SparkDataTable result2 = new SparkDataTable(context, partition2, rdd.getTableSpec());
        return new PortObject[] {new SparkDataPortObject(result1), new SparkDataPortObject(result2)};
    }
}
