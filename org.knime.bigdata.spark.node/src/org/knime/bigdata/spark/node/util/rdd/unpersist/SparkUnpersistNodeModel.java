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
 *   Created on 28.08.2015 by koetter
 */
package org.knime.bigdata.spark.node.util.rdd.unpersist;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SimpleJobRunFactory;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkUnpersistNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkUnpersistNodeModel.class.getCanonicalName();

    SparkUnpersistNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[0], false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new PortObjectSpec[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rdd = (SparkDataPortObject)inData[0];
        final SparkContextID contextID = rdd.getContextID();
        final UnpersistJobInput input = new UnpersistJobInput(rdd.getTableName());
        final SimpleJobRunFactory<JobInput> runFactory = getSimpleJobRunFactory(rdd, JOB_ID);
        runFactory.createRun(input).run(contextID, exec);
        return new PortObject[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        //nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        //nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        //nothing to do
    }
}
