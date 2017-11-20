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
 *   Created on 12.02.2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.sampling;

import java.util.LinkedList;
import java.util.List;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.JobRunFactory;
import com.knime.bigdata.spark.core.job.util.EnumContainer.CountMethod;
import com.knime.bigdata.spark.core.job.util.EnumContainer.SamplingMethod;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;
import com.knime.bigdata.spark.core.util.SparkUtil;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkSamplingNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkSamplingNodeModel.class.getCanonicalName();

    private final SparkSamplingNodeSettings m_settings = new SparkSamplingNodeSettings();

    /**
     * Constructor.
     */
    public SparkSamplingNodeModel() {
        this(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * @param inPortTypes the input port types
     * @param outPortTypes the output port types
     */
    protected SparkSamplingNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        super(inPortTypes, outPortTypes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 1 || !(inSpecs[0] instanceof SparkDataPortObjectSpec)) {
            throw new InvalidSettingsException("No input found");
        }
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[0];
        SparkSamplingNodeSettings.checkSettings(sparkSpec.getTableSpec(), m_settings);
        return new PortObjectSpec[] {sparkSpec};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rdd = (SparkDataPortObject) inData[0];
        final SparkContextID context = rdd.getContextID();
        final SparkDataTable resultTable = new SparkDataTable(context, rdd.getData().getTableSpec());
        exec.setMessage("Start Spark sampling job...");
        final boolean samplesRddIsInputRdd = runJob(exec, JOB_ID, rdd, resultTable.getID(), null);
        final SparkDataTable output;
        if (samplesRddIsInputRdd) {
            output = rdd.getData();
            setDeleteOnReset(false);
        } else {
            output = resultTable;
            setDeleteOnReset(true);
        }
        return new PortObject[] {new SparkDataPortObject(output)};
    }

    /**
     * @param exec
     * @param rdd
     * @param jobID sampling or partitioning job id
     * @param aOutputTable1
     * @param aOutputTable2
     * @return <code>true</code> if first output RDD (samples) is the input RDD (all input data sampled)
     * @throws InvalidSettingsException
     * @throws KNIMESparkException
     * @throws CanceledExecutionException
     */
    protected boolean runJob(final ExecutionContext exec, final String jobID, final SparkDataPortObject rdd, final String aOutputTable1,
            final String aOutputTable2) throws InvalidSettingsException, KNIMESparkException, CanceledExecutionException {

        final SparkSamplingNodeSettings settings = getSettings();
        final SparkContextID contextID = rdd.getContextID();

        final Integer[] columnIndices = SparkUtil.getColumnIndices(rdd.getTableSpec(), settings.classColumn());
        final Integer clasColIdx = columnIndices[0];

        final CountMethod sparkCountMethod = CountMethod.fromKNIMEMethodName(settings.countMethod().name());
        final SamplingMethod sparkSamplingMethod = SamplingMethod.fromKNIMEMethodName(settings.samplingMethod().name());

        final List<String> aOutputTable = new LinkedList<>();
        aOutputTable.add(aOutputTable1);
        if (aOutputTable2 != null) {
            aOutputTable.add(aOutputTable2);
        }
        exec.checkCanceled();
        final JobRunFactory<SamplingJobInput, SamplingJobOutput> runFactory = SparkContextUtil.getJobRunFactory(contextID, jobID);
        final SamplingJobInput jobInput = new SamplingJobInput(rdd.getTableName(), aOutputTable.toArray(new String[0]), sparkCountMethod, settings.count(),
            sparkSamplingMethod, settings.fraction(), clasColIdx, settings.withReplacement(), settings.seed(), settings.exactSampling());
        final SamplingJobOutput jobOutput= runFactory.createRun(jobInput).run(contextID, exec);

        if (jobOutput.samplesRddIsInputRdd()) {
            //if the sampling failed the job returns the input RDD as output RDD so we shouldn't delete it on node reset
            setWarningMessage("Sampling failed. Sampling size larger then number of available rows in Spark. Return input data.");
        }

        return jobOutput.samplesRddIsInputRdd();
    }

    /**
     * @return the {@link SparkSamplingNodeSettings}
     */
    protected SparkSamplingNodeSettings getSettings() {
        return m_settings;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        SparkSamplingNodeSettings.validateSamplingSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings, false);
    }
}
