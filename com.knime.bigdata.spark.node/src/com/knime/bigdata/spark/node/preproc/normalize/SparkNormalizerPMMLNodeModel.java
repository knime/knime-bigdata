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
 *   Created on 31.07.2015 by dwk
 */
package com.knime.bigdata.spark.node.preproc.normalize;

import java.util.Arrays;

import org.knime.base.data.normalize.AffineTransConfiguration;
import org.knime.base.data.normalize.Normalizer2;
import org.knime.base.data.normalize.PMMLNormalizeTranslator;
import org.knime.base.node.preproc.normalize3.NormalizerConfig;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.port.pmml.PMMLPortObjectSpecCreator;
import org.knime.core.node.port.pmml.preproc.DerivedFieldMapper;
import org.knime.core.node.util.ConvenienceMethods;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.job.JobRunFactory;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.core.util.SparkIDs;
import com.knime.bigdata.spark.core.util.SparkUtil;

/**
 * The NormalizeNodeModel normalizes the input RDD in Spark.
 *
 * @author dwk
 */
public class SparkNormalizerPMMLNodeModel extends SparkNodeModel {

    /** The unique spark job id. */
    public static final String JOB_ID = SparkNormalizerPMMLNodeModel.class.getCanonicalName();

    //TODO: add an option to replace processed columns
    private static final int MAX_UNKNOWN_COLS = 3;

    /** Configuration. */
    private NormalizerConfig m_config;

    /**
     * Creates an new normalizer. One input, two outputs (one of which is the model).
     */
    public SparkNormalizerPMMLNodeModel() {
        this(PMMLPortObject.TYPE);
    }

    /**
     * @param modelPortType the port type of the model
     */
    protected SparkNormalizerPMMLNodeModel(final PortType modelPortType) {
        super(PMMLPortObject.TYPE.equals(modelPortType) ? new PortType[]{SparkDataPortObject.TYPE} : new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{
            SparkDataPortObject.TYPE, modelPortType});
    }

    /**
     * All {@link org.knime.core.data.def.IntCell} columns are converted to {@link org.knime.core.data.def.DoubleCell}
     * columns.
     *
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        final DataTableSpec dataSpec = spec.getTableSpec();
        boolean hasGuessedDefaults = false;
        if (m_config == null) {
            NormalizerConfig config = new NormalizerConfig();
            config.guessDefaults(dataSpec);
            hasGuessedDefaults = true;
            m_config = config;
        }
        FilterResult filterResult = m_config.getDataColumnFilterConfig().applyTo(dataSpec);
        String[] includes = filterResult.getIncludes();
        if (includes.length == 0) {
            StringBuilder warnings = new StringBuilder("No columns included - input stays unchanged.");
            if (filterResult.getRemovedFromIncludes().length > 0) {
                warnings.append("\nThe following columns were included before but no longer exist:\n");
                warnings.append(ConvenienceMethods.getShortStringFrom(
                    Arrays.asList(filterResult.getRemovedFromIncludes()), MAX_UNKNOWN_COLS));
            }
            setWarningMessage(warnings.toString());
        } else if (hasGuessedDefaults) {
            setWarningMessage("Auto-configure: [0, 1] normalization on all numeric columns: "
                + ConvenienceMethods.getShortStringFrom(Arrays.asList(includes), MAX_UNKNOWN_COLS));
        }
        final DataTableSpec resultSpec = Normalizer2.generateNewSpec(dataSpec, includes);
        final PMMLPortObjectSpecCreator pmmlSpecCreator = new PMMLPortObjectSpecCreator(resultSpec);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(spec.getContextID(), resultSpec),
            pmmlSpecCreator.createSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_config = new NormalizerConfig();
        m_config.loadConfigurationInModel(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_config != null) {
            m_config.saveSettings(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_config = new NormalizerConfig();
        m_config.loadConfigurationInModel(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rdd = (SparkDataPortObject)inData[0];
        final SparkContextID contextID = rdd.getContextID();
        final DataTableSpec spec = rdd.getTableSpec();
        exec.checkCanceled();
        final FilterResult filterResult = m_config.getDataColumnFilterConfig().applyTo(spec);
        final String[] includes = filterResult.getIncludes();
        final String[] includedCols = filterResult.getIncludes();
        final Integer[] includeColIdxs = SparkUtil.getColumnIndices(spec, includes);
        for (int i = 0, length = includedCols.length; i < length; i++) {
            includeColIdxs[i] = spec.findColumnIndex(includedCols[i]);
        }
        final String outputTableName = SparkIDs.createRDDID();

        final JobRunFactory<NormalizeJobInput, NormalizeJobOutput> runFactory = SparkContextUtil.getJobRunFactory(contextID, JOB_ID);
        final NormalizeJobInput jobInput = new NormalizeJobInput(rdd.getData().getID(), outputTableName, includeColIdxs, convertToSettings());
        final NormalizeJobOutput jobOutput = runFactory.createRun(jobInput).run(contextID, exec);
        //create from result
        final double[] min = new double[includedCols.length];
        Arrays.fill(min, m_config.getMin());
        final double[] max = new double[includedCols.length];
        Arrays.fill(max, m_config.getMax());
        AffineTransConfiguration normConfig =
            new AffineTransConfiguration(includedCols, jobOutput.getScales(), jobOutput.getTranslations(), min, max, null);
        final PMMLNormalizeTranslator trans =
                new PMMLNormalizeTranslator(normConfig, new DerivedFieldMapper((PMMLPortObject)null));
        final PMMLPortObjectSpecCreator creator = new PMMLPortObjectSpecCreator(spec);
        final PMMLPortObject outPMMLPort = new PMMLPortObject(creator.createSpec());
        outPMMLPort.addGlobalTransformations(trans.exportToTransDict());
        final DataTableSpec resultSpec = Normalizer2.generateNewSpec(spec, includes);
        return new PortObject[]{createSparkPortObject(rdd, resultSpec, outputTableName), outPMMLPort};
    }

    /**
     * @param mode
     * @return
     * @throws InvalidSettingsException
     */
    private NormalizationSettings convertToSettings() throws InvalidSettingsException {
        switch (m_config.getMode()) {
            case MINMAX:
                //set min, max
                return NormalizationSettingsFactory.createNormalizationSettingsForMinMaxScaling(m_config.getMin(),
                    m_config.getMax());
            case Z_SCORE:
                return NormalizationSettingsFactory.createNormalizationSettingsForZScoreNormalization();
            case DECIMALSCALING:
                return NormalizationSettingsFactory.createNormalizationSettingsForDecimalScaling();
            default:
                throw new InvalidSettingsException("No mode set");
        }
    }
}
