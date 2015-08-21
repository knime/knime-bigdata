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
 *   Created on 12.02.2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.sampling;

import javax.annotation.Nullable;

import org.knime.base.node.preproc.sample.SamplingNodeSettings;
import org.knime.base.node.preproc.sample.SamplingNodeSettings.CountMethods;
import org.knime.base.node.preproc.sample.SamplingNodeSettings.SamplingMethods;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.SamplingJob;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.node.AbstractSparkNodeModel;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDs;
import com.knime.bigdata.spark.util.SparkUtil;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkSamplingNodeModel extends AbstractSparkNodeModel {

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
        final String outputTableName = SparkIDs.createRDDID();
        final String paramInJson = paramDef(rdd, getSettings(), outputTableName, null);
        final KNIMESparkContext context = rdd.getContext();
        exec.checkCanceled();
        exec.setMessage("Start Spark sampling job...");
        final String jobId = JobControler.startJob(context, SamplingJob.class.getCanonicalName(), paramInJson);
        //TODO: Check that the result RDD is a new one. If not do not delete the result RDD on node reset!!!
        final JobResult jobResult = JobControler.waitForJobAndFetchResult(context, jobId, exec);
        final Boolean successful = (Boolean)jobResult.getObjectResult();
        if (!successful.booleanValue()) {
            //if the sampling failed the job returns the input RDD as output RDD so we shouldn't delete it on node reset
            setWarningMessage("Sampling failed.");
            setDeleteOnReset(false);
        }
        final SparkDataTable result = new SparkDataTable(context, outputTableName, rdd.getTableSpec());
        return new PortObject[] {new SparkDataPortObject(result)};
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
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        SparkSamplingNodeSettings.validateSamplingSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings, false);
    }

    /**
     * @param rdd Spark RDD to sample
     * @param settings the {@link SamplingNodeSettings}
     * @param outputTableName the name of the first partition of the sampled data
     * @param outputTableName2 the name of the optional second partition of the sample data
     * @return the JSON settings string
     * @throws InvalidSettingsException if the class column is not present
     */
    public static String paramDef(final SparkDataPortObject rdd, final SparkSamplingNodeSettings settings,
        final String outputTableName, @Nullable final String outputTableName2) throws InvalidSettingsException {
        final Integer[] columnIndices = SparkUtil.getColumnIndices(rdd.getTableSpec(), settings.classColumn());
        return paramDef(rdd.getTableName(), settings.countMethod(), settings.count(), settings.samplingMethod(),
            settings.fraction(), columnIndices[0], settings.withReplacement(), settings.seed(),
            settings.exactSampling(), outputTableName, outputTableName2);
    }

    /**
     * (for better unit testing)
     * @param aTableToSample
     * @param aSettings
     * @param aClassColIx - index of class column label (for stratified sampling)
     * @param aIsWithReplacement - sampling with replacement
     * @param aSeed  random seed
     * @param aExact - currently only supported for stratified sampling - exact means that the sample is > 99.% true to the class distribution
     * @param aOutputTable1
     * @param aOutputTable2 - optional, if provided, then data is split into 2 RDDs
     *
     * @return Json String with parameter settings
     */
    public static String paramDef(final String aTableToSample, final SamplingNodeSettings aSettings,
        final int aClassColIx, final boolean aIsWithReplacement, final long aSeed, final boolean aExact,
        final String aOutputTable1, @Nullable final String aOutputTable2) {
        final Object[] outputParams;
        if (aOutputTable2 == null) {
            outputParams = new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, aOutputTable1};
        } else {
            outputParams =
                new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, aOutputTable1, SamplingJob.PARAM_SPLIT_TABLE_2,
                    aOutputTable2};
        }
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new Object[]{KnimeSparkJob.PARAM_INPUT_TABLE, aTableToSample, SamplingJob.PARAM_COUNT_METHOD,
                aSettings.countMethod().toString(),  SamplingJob.PARAM_COUNT, aSettings.count(),
                SamplingJob.PARAM_SAMPLING_METHOD, aSettings.samplingMethod().toString(),
                SamplingJob.PARAM_WITH_REPLACEMENT, aIsWithReplacement,
                SamplingJob.PARAM_EXACT, aExact, SamplingJob.PARAM_SEED, aSeed,
                SamplingJob.PARAM_FRACTION, aSettings.fraction(),  SamplingJob.PARAM_CLASS_COLUMN, aClassColIx},
            ParameterConstants.PARAM_OUTPUT, outputParams});
    }

    private static String paramDef(final String aTableToSample, final CountMethods countMethod, final int count,
        final SamplingMethods samplingMethod, final double fraction, final Integer aClassColIx,
        final boolean aIsWithReplacement, final Long aSeed, final boolean aExact,
        final String aOutputTable1, @Nullable final String aOutputTable2) {
        final Object[] outputParams;
        if (aOutputTable2 == null) {
            outputParams = new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, aOutputTable1};
        } else {
            outputParams =
                new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, aOutputTable1, SamplingJob.PARAM_SPLIT_TABLE_2,
                    aOutputTable2};
        }
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new Object[]{KnimeSparkJob.PARAM_INPUT_TABLE, aTableToSample,
                SamplingJob.PARAM_COUNT_METHOD, countMethod.toString(),
                SamplingJob.PARAM_COUNT, count,
                SamplingJob.PARAM_SAMPLING_METHOD, samplingMethod.toString(),
                SamplingJob.PARAM_WITH_REPLACEMENT, aIsWithReplacement,
                SamplingJob.PARAM_EXACT, aExact,
                SamplingJob.PARAM_SEED, aSeed == null ? Long.valueOf(System.currentTimeMillis()) : aSeed,
                SamplingJob.PARAM_FRACTION, fraction,
                SamplingJob.PARAM_CLASS_COLUMN, aClassColIx},
            ParameterConstants.PARAM_OUTPUT, outputParams});
    }

}
