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
package com.knime.bigdata.spark.node.pmml.transformation;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.job.EmptyJobOutput;
import com.knime.bigdata.spark.core.job.JobWithFilesRunFactory;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark.core.util.SparkIDs;
import com.knime.bigdata.spark.core.util.SparkPMMLUtil;
import com.knime.bigdata.spark.node.pmml.predictor.AbstractSparkPMMLPredictorNodeModel;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObject;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObjectSpec;

/**
 * The PMML transformation node model.
 *
 * @author Tobias Koetter, KNIME.com
 */
public abstract class AbstractSparkTransformationPMMLApplyNodeModel extends SparkNodeModel {

    /**The unique Spark job id.*/
    public static final String JOB_ID = AbstractSparkTransformationPMMLApplyNodeModel.class.getCanonicalName();

    private final SettingsModelBoolean m_replace = createReplaceModel();

    /**
     * @param inPortTypes the expected input {@link PortType}s
     * @param outPortTypes the expected output {@link PortType}s
     */
    protected AbstractSparkTransformationPMMLApplyNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        super(inPortTypes, outPortTypes);
    }

    /**
     * @return
     */
    static SettingsModelBoolean createReplaceModel() {
        return new SettingsModelBoolean("replaceTransformedCols", false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final CompiledModelPortObject pmml = getCompiledPMMLModel(exec, inObjects);
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[1];
        final CompiledModelPortObjectSpec cms = (CompiledModelPortObjectSpec)pmml.getSpec();
        exec.setMessage("Create table specification");
        final Collection<String> missingFieldNames  = new LinkedList<>();
        final Integer[] colIdxs = SparkPMMLUtil.getColumnIndices(data.getTableSpec(),
            (CompiledModelPortObjectSpec)pmml.getSpec(), missingFieldNames);
        if (!missingFieldNames.isEmpty()) {
            setWarningMessage("Missing input fields: " + missingFieldNames);
        }
        //TODO: Implement replace function once we can better determine the columns to replace
        final List<Integer> addCols = new LinkedList<>();
        final List<Integer> skipCols = new LinkedList<>();
        final DataTableSpec resultSpec = SparkPMMLUtil.createTransformationResultSpec(data.getTableSpec(), cms,
            colIdxs, addCols, m_replace.getBooleanValue(), skipCols);
        final String aOutputTableName = SparkIDs.createRDDID();
        final IntermediateSpec outputSchema = SparkDataTableUtil.toIntermediateSpec(resultSpec);
        final File jobFile = AbstractSparkPMMLPredictorNodeModel.createJobFile(pmml);
        addFileToDeleteAfterExecute(jobFile);
        final PMMLTransformationJobInput input = new PMMLTransformationJobInput(data.getTableName(), colIdxs,
            pmml.getModelClassName(), aOutputTableName, outputSchema, addCols, m_replace.getBooleanValue(), skipCols);
        exec.setMessage("Execute Spark job");
        final JobWithFilesRunFactory<PMMLTransformationJobInput, EmptyJobOutput> execProvider =
                SparkContextUtil.getJobWithFilesRunFactory(data.getContextID(), JOB_ID);
        execProvider.createRun(input, Collections.singletonList(jobFile)).run(data.getContextID(), exec);
        return new PortObject[] {createSparkPortObject(data, resultSpec, aOutputTableName)};
    }

    /**
     * @return <code>true</code> if the transformed columns should be replaced otherwise <code>false</code>
     */
    protected boolean replace() {
        return m_replace.getBooleanValue();
    }

    /**
     * @param exec {@link ExecutionMonitor} to provide progress
     * @param inObjects the nodes input object array
     * @return the {@link CompiledModelPortObject} to use
     * @throws CanceledExecutionException if the operation was canceled
     * @throws InvalidSettingsException if the settings are invalid
     * @throws Exception if anything else goes wrong
     */
    public abstract CompiledModelPortObject getCompiledPMMLModel(ExecutionMonitor exec, final PortObject[] inObjects)
            throws CanceledExecutionException, InvalidSettingsException, Exception;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        //TODO: Implement replace option
//        m_replace.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        //TODO: Implement replace option
//        m_replace.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        //TODO: Implement replace option
//        m_replace.validateSettings(settings);
    }
}
