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
package com.knime.bigdata.spark.node.preproc.pmml.transformation;

import org.knime.base.data.normalize.AffineTransConfiguration;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.node.AbstractSparkNodeModel;
import com.knime.bigdata.spark.node.mllib.pmml.predictor.PMMLAssignTask;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDs;
import com.knime.bigdata.spark.util.SparkUtil;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObject;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObjectSpec;

/**
 * The PMML transformation node model.
 *
 * @author koetter
 */
public class SparkTransformationPMMLApplyNodeModel extends AbstractSparkNodeModel {
    /**
    *
    */
    public SparkTransformationPMMLApplyNodeModel() {
        super(new PortType[]{CompiledModelPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final CompiledModelPortObjectSpec pmmlSpec = (CompiledModelPortObjectSpec) inSpecs[0];
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[1];
        DataTableSpec resultSpec = createResultSpec(sparkSpec.getTableSpec(), pmmlSpec);
        return new PortObjectSpec[] {new SparkDataPortObjectSpec(sparkSpec.getContext(), resultSpec)};
    }

    private DataTableSpec createResultSpec(final DataTableSpec inSpec,
                                                          final CompiledModelPortObjectSpec cms) {
        final DataColumnSpec[] specs = cms.getTransformationsResultColSpecs(inSpec);
        return new DataTableSpec(inSpec, new DataTableSpec(specs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final CompiledModelPortObject pmml = (CompiledModelPortObject)inObjects[0];
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[1];
        final String aOutputTableName = SparkIDs.createRDDID();
        final CompiledModelPortObjectSpec cms = (CompiledModelPortObjectSpec)pmml.getSpec();
        final DataTableSpec resultSpec = createResultSpec(data.getTableSpec(), cms);
        final SparkDataTable resultRDD = new SparkDataTable(data.getContext(), aOutputTableName, resultSpec);
        final Integer[] colIdxs = SparkUtil.getColumnIndices(data.getTableSpec(), pmml.getModel());
        final PMMLAssignTask assignTask = new PMMLAssignTask();
        assignTask.execute(exec, data.getData(), pmml, colIdxs, true, resultRDD);
        return new PortObject[] {new SparkDataPortObject(resultRDD)};
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
    protected void saveSettingsTo(final NodeSettingsWO settings) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
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
