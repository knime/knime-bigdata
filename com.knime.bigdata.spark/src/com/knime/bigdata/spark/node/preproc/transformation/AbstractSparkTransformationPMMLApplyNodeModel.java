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
package com.knime.bigdata.spark.node.preproc.transformation;

import java.util.Collection;
import java.util.LinkedList;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.util.SparkIDs;
import com.knime.bigdata.spark.util.SparkPMMLUtil;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObject;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObjectSpec;

/**
 * The PMML transformation node model.
 *
 * @author Tobias Koetter, KNIME.com
 */
public abstract class AbstractSparkTransformationPMMLApplyNodeModel extends SparkNodeModel {
    //TODO: add an option to replace processed columns
    /**
     * @param inPortTypes the expected input {@link PortType}s
     * @param outPortTypes the expected output {@link PortType}s
     */
    protected AbstractSparkTransformationPMMLApplyNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        super(inPortTypes, outPortTypes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
//        final CompiledModelPortObjectSpec pmmlSpec = (CompiledModelPortObjectSpec) inSpecs[0];
//        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[1];
//        DataTableSpec resultSpec = createResultSpec(sparkSpec.getTableSpec(), pmmlSpec);
//        return new PortObjectSpec[] {new SparkDataPortObjectSpec(sparkSpec.getContext(), resultSpec)};
        return new PortObjectSpec[] {null};
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
        final Collection<String> missingFieldNames  = new LinkedList<String>();
        final Integer[] colIdxs = SparkPMMLUtil.getColumnIndices(data.getTableSpec(), pmml.getModel(), missingFieldNames);
        if (!missingFieldNames.isEmpty()) {
            setWarningMessage("Missing input fields: " + missingFieldNames);
        }
        final DataTableSpec resultSpec = SparkPMMLUtil.createResultSpec(data.getTableSpec(), cms, colIdxs);
        final PMMLTransformationTask task = new PMMLTransformationTask();
        exec.setMessage("Execute Spark job");
        exec.checkCanceled();
        final String aOutputTableName = SparkIDs.createRDDID();
        task.execute(exec, data.getData(), pmml, colIdxs, true, aOutputTableName);
        return new PortObject[] {createSparkPortObject(data, resultSpec, aOutputTableName)};
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
}
