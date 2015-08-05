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
package com.knime.bigdata.spark.node.preproc.pmml.normalize;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.knime.base.data.normalize.AffineTransConfiguration;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.port.pmml.PMMLPortObjectSpec;
import org.knime.core.node.port.pmml.PMMLPortObjectSpecCreator;
import org.knime.core.node.util.ConvenienceMethods;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.NormalizeColumnsJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.NormalizationSettings;
import com.knime.bigdata.spark.jobserver.server.NormalizationSettingsFactory;
import com.knime.bigdata.spark.jobserver.server.Normalizer;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDs;

/**
 * The NormalizeNodeModel normalizes the input RDD in Spark.
 *
 * @author dwk
 */
public class SparkNormalizerPMMLNodeModel extends NodeModel {

    private static final int MAX_UNKNOWN_COLS = 3;

    /** Configuration. */
    private SparkNormalizerPMMLConfig m_config;

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
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        final DataTableSpec dataSpec = spec.getTableSpec();
        //TK_TODO - we have no incoming PMMLPort, need to create a new object
        PMMLPortObjectSpec pmmlSpec = null; //(PMMLPortObjectSpec)inSpecs[1];

        //TK_TODO - please check this
        PMMLPortObjectSpecCreator pmmlSpecCreator
                = new PMMLPortObjectSpecCreator(pmmlSpec);
        return new PortObjectSpec[]{
                dataSpec, //Normalizer2.generateNewSpec(spec, getIncludedComlumns(dataSpec)),
                pmmlSpecCreator.createSpec()};
    }

    /**
     * Finds all numeric columns in spec.
     *
     * @param spec input table spec
     * @return array of numeric column names
     */
    static final String[] findAllNumericColumns(final DataTableSpec spec) {
        int nrcols = spec.getNumColumns();
        List<String> poscolumns = new ArrayList<String>();
        for (int i = 0; i < nrcols; i++) {
            if (spec.getColumnSpec(i).getType().isCompatible(DoubleValue.class)) {
                poscolumns.add(spec.getColumnSpec(i).getName());
            }
        }
        return poscolumns.toArray(new String[poscolumns.size()]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
        CanceledExecutionException {
        // empty
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
        CanceledExecutionException {
        // empty
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_config = new SparkNormalizerPMMLConfig();
        m_config.loadConfigurationInModel(settings);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // empty
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
        m_config = new SparkNormalizerPMMLConfig();
        m_config.loadConfigurationInModel(settings);
    }

    /**
     * @param spec the data table spec
     * @throws InvalidSettingsException if no normalization mode is set
     * @return the included columns
     */
    private String[] getIncludedComlumns(final DataTableSpec spec) throws InvalidSettingsException {
        boolean hasGuessedDefaults = false;
        if (m_config == null) {
            SparkNormalizerPMMLConfig config = new SparkNormalizerPMMLConfig();
            config.guessDefaults(spec);
            hasGuessedDefaults = true;
            m_config = config;
        }

        FilterResult filterResult = m_config.getDataColumnFilterConfig().applyTo(spec);
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

        return includes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {

        final SparkDataPortObject rdd = (SparkDataPortObject)inData[0];
        final KNIMESparkContext context = rdd.getContext();
        final DataTableSpec spec = rdd.getTableSpec();

        exec.checkCanceled();
        final String[] includedCols = getIncludedComlumns(spec);
        Integer[] includeColIdxs = new Integer[includedCols.length];

        for (int i = 0, length = includedCols.length; i < length; i++) {
            includeColIdxs[i] = spec.findColumnIndex(includedCols[i]);
        }

        final String outputTableName = SparkIDs.createRDDID();

        final String paramInJson =
            paramsToJson(rdd.getTableName(), includeColIdxs, convertToSettings(), outputTableName);

        final String jobId = JobControler.startJob(context, NormalizeColumnsJob.class.getCanonicalName(), paramInJson);

        final JobResult result = JobControler.waitForJobAndFetchResult(context, jobId, exec);

        Normalizer res = (Normalizer)result.getObjectResult();

        //create from result
        final double[] min = new double[includedCols.length];
        Arrays.fill(min, m_config.getMin());
        final double[] max = new double[includedCols.length];
        Arrays.fill(max, m_config.getMax());
        AffineTransConfiguration normConfig =
            new AffineTransConfiguration(includedCols, res.getScales(), res.getTranslations(), min, max,
                "PORT SUMMARY...");

        //TODO - fix me...
        final DataColumnSpec[] mappingSpecs = null;
        final DataTableSpec firstSpec = new DataTableSpec(spec, new DataTableSpec(mappingSpecs));
        SparkDataTable firstRDD = new SparkDataTable(context, outputTableName, firstSpec);

        // TODO - what would the incoming PMML be good for?
        // the optional PMML in port (can be null)
        //PMMLPortObject inPMMLPort = (PMMLPortObject)inData[1];
        //PMMLNormalizeTranslator trans = new PMMLNormalizeTranslator(normConfig, new DerivedFieldMapper(inPMMLPort));

        PMMLPortObjectSpecCreator creator = new PMMLPortObjectSpecCreator(spec);
        PMMLPortObject outPMMLPort = new PMMLPortObject(creator.createSpec());
        //outPMMLPort.addGlobalTransformations(trans.exportToTransDict());

        return new PortObject[]{new SparkDataPortObject(firstRDD), outPMMLPort};
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

    /**
     * (public for unit testing)
     *
     * @param aInputTableName
     * @param aNumericColIdx
     * @param aNormalization
     * @param aOutputTableName
     * @return Json representation of parameters
     * @throws GenericKnimeSparkException
     */
    public static String paramsToJson(final String aInputTableName, final Integer[] aNumericColIdx,
        final NormalizationSettings aNormalization, final String aOutputTableName) throws GenericKnimeSparkException {

        final Object[] inputParamas =
            new Object[]{ParameterConstants.PARAM_STRING, JobConfig.encodeToBase64(aNormalization),
                ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray((Object[])aNumericColIdx),
                ParameterConstants.PARAM_TABLE_1, aInputTableName};

        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParamas,
            ParameterConstants.PARAM_OUTPUT, new String[]{ParameterConstants.PARAM_TABLE_1, aOutputTableName}});
    }
}
