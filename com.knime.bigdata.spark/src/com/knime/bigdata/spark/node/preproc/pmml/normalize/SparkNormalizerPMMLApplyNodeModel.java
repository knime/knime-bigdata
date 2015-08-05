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

import org.knime.base.data.normalize.AffineTransConfiguration;
import org.knime.base.data.normalize.PMMLNormalizeTranslator;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
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
import org.knime.core.node.port.pmml.PMMLPortObjectSpecCreator;
import org.knime.core.node.port.pmml.preproc.DerivedFieldMapper;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.NormalizeColumnsJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.Normalizer;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDs;

/**
 * The NormalizeNodeModel normalizes the input RDD in Spark.
 *
 * @author dwk
 */
public class SparkNormalizerPMMLApplyNodeModel extends NodeModel {
    /**
    *
    */
    public SparkNormalizerPMMLApplyNodeModel() {
        super(new PortType[]{PMMLPortObject.TYPE, SparkDataPortObject.TYPE}, new PortType[]{PMMLPortObject.TYPE,
            SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        /* So far we can only get all preprocessing fields from the PMML port
         * object spec. There is no way to determine from the spec which
         * of those fields contain normalize operations. Hence we cannot
         * determine the data table output spec at this point.
         * Bug 2985
         *
         */
        return new PortObjectSpec[]{inSpecs[0], null};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
        CanceledExecutionException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
        CanceledExecutionException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        PMMLPortObject model = (PMMLPortObject)inData[0];

        PMMLNormalizeTranslator translator = new PMMLNormalizeTranslator();
        translator.initializeFrom(model.getDerivedFields());
        AffineTransConfiguration config = getAffineTrans(translator.getAffineTransConfig());
        if (config.getNames().length == 0) {
            throw new IllegalArgumentException("No normalization configuration " + "found.");
        }

        final SparkDataPortObject rdd = (SparkDataPortObject)inData[1];
        final KNIMESparkContext context = rdd.getContext();
        final DataTableSpec spec = rdd.getTableSpec();

        exec.checkCanceled();
        final String[] includedCols = config.getNames();
        Integer[] includeColIdxs = new Integer[includedCols.length];

        for (int i = 0, length = includedCols.length; i < length; i++) {
            includeColIdxs[i] = spec.findColumnIndex(includedCols[i]);
        }

        final String outputTableName = SparkIDs.createRDDID();

        final String paramInJson = paramsToJson(rdd.getTableName(), includeColIdxs, config, outputTableName);

        final String jobId = JobControler.startJob(context, NormalizeColumnsJob.class.getCanonicalName(), paramInJson);

        final JobResult result = JobControler.waitForJobAndFetchResult(context, jobId, exec);

        Normalizer res = (Normalizer)result.getObjectResult();

        //TODO - fix me...
        final DataColumnSpec[] mappingSpecs = null;
        final DataTableSpec firstSpec = new DataTableSpec(spec, new DataTableSpec(mappingSpecs));
        SparkDataTable firstRDD = new SparkDataTable(context, outputTableName, firstSpec);

        PMMLNormalizeTranslator trans = new PMMLNormalizeTranslator(config, new DerivedFieldMapper(model));

        PMMLPortObjectSpecCreator creator = new PMMLPortObjectSpecCreator(spec);
        PMMLPortObject outPMMLPort = new PMMLPortObject(creator.createSpec());
        //outPMMLPort.addGlobalTransformations(trans.exportToTransDict());

        return new PortObject[]{new SparkDataPortObject(firstRDD), outPMMLPort};
    }

    /**
     * Return the configuration with possible additional transformations made.
     *
     * @param affineTransConfig the original affine transformation configuration.
     * @return the (possible modified) configuration.
     */
    protected AffineTransConfiguration getAffineTrans(final AffineTransConfiguration affineTransConfig) {
        return affineTransConfig;
    }

    private static String paramsToJson(final String aInputTableName, final Integer[] aNumericColIdx,
        final AffineTransConfiguration config, final String aOutputTableName) throws GenericKnimeSparkException {
        final Double[][] normalizationParameters = new Double[2][];

        final double[] scales = config.getScales();
        final double[] translations = config.getTranslations();
        normalizationParameters[0] = new Double[scales.length];
        normalizationParameters[1] = new Double[translations.length];
        for (int i = 0; i < translations.length; i++) {
            normalizationParameters[0][i] = scales[i];
            normalizationParameters[1][i] = translations[i];
        }
        return paramsToJson(aInputTableName, aNumericColIdx, normalizationParameters, aOutputTableName);
    }

    /**
     * (public for unit testing)
     *
     * @param aInputTableName
     * @param aNumericColIdx
     * @param aNormalizationParameters
     * @param aOutputTableName
     * @return Json representation of parameters
     * @throws GenericKnimeSparkException
     */
    public static String paramsToJson(final String aInputTableName, final Integer[] aNumericColIdx,
        final Double[][] aNormalizationParameters, final String aOutputTableName) throws GenericKnimeSparkException {

        final Object[] inputParamas =
            new Object[]{ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_STRING, 1),
            JobConfig.encodeToBase64(aNormalizationParameters), ParameterConstants.PARAM_COL_IDXS,
                JsonUtils.toJsonArray((Object[])aNumericColIdx), ParameterConstants.PARAM_TABLE_1, aInputTableName};

        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParamas,
            ParameterConstants.PARAM_OUTPUT, new String[]{ParameterConstants.PARAM_TABLE_1, aOutputTableName}});
    }
}
