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
 *   Created on 18.08.2015 by koetter
 */
package org.knime.bigdata.spark.node.preproc.partition;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.node.preproc.sampling.SparkSamplingNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkPartitionNodeModel extends SparkSamplingNodeModel {

    /** The unique Spark job id. */
    public static final String PARTITION_JOB_ID = SparkPartitionNodeModel.class.getCanonicalName();

    /**
     * Constructor.
     */
    public SparkPartitionNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        PortObjectSpec[] result = super.configureInternal(inSpecs);
        return new PortObjectSpec[] {result[0], result[0]};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rdd = (SparkDataPortObject) inData[0];
        SparkContextID contextID = rdd.getContextID();
        final SparkDataTable resultTable1 = new SparkDataTable(contextID, rdd.getData().getTableSpec());
        final SparkDataTable resultTable2 = new SparkDataTable(contextID, rdd.getData().getTableSpec());
        exec.setMessage("Start Spark partitioning job...");
        final boolean samplesRddIsInputRdd = runJob(exec, PARTITION_JOB_ID, rdd, resultTable1.getID(), resultTable2.getID());
        SparkDataTable firstOutput;
        if (samplesRddIsInputRdd) {
            //disable the automatic RDD handling since the first output RDD is the input RDD which
            //should not be deleted on node reset
            setAutomaticSparkDataHandling(false);
            addAdditionalSparkDataObjectsToDelete(contextID, resultTable2.getID());
            firstOutput = rdd.getData();
        } else {
            //ensure that the automatic RDD handling is enabled
            setAutomaticSparkDataHandling(true);
            firstOutput = resultTable1;
        }
        return new PortObject[] {new SparkDataPortObject(firstOutput), new SparkDataPortObject(resultTable2)};
    }
}
