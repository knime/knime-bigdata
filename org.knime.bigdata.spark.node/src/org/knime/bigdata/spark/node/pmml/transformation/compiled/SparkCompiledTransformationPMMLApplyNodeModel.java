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
 *   Created on 31.07.2015 by dwk
 */
package org.knime.bigdata.spark.node.pmml.transformation.compiled;

import java.util.LinkedList;
import java.util.List;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.util.SparkPMMLUtil;
import org.knime.bigdata.spark.node.pmml.transformation.AbstractSparkTransformationPMMLApplyNodeModel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.pmml.compilation.java.compile.CompiledModelPortObject;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObjectSpec;

/**
 * The PMML transformation node model.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkCompiledTransformationPMMLApplyNodeModel extends AbstractSparkTransformationPMMLApplyNodeModel {


    SparkCompiledTransformationPMMLApplyNodeModel() {
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
        final Integer[] colIdxs = SparkPMMLUtil.getColumnIndices(sparkSpec.getTableSpec(), pmmlSpec);
        final List<Integer> addCols = new LinkedList<>();
        final List<Integer> skipCols = new LinkedList<>();
        final DataTableSpec resultSpec = SparkPMMLUtil.createTransformationResultSpec(sparkSpec.getTableSpec(),
            pmmlSpec, colIdxs, addCols, replace(), skipCols);
        return new PortObjectSpec[]{
            new SparkDataPortObjectSpec(sparkSpec.getContextID(), resultSpec)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompiledModelPortObject getCompiledPMMLModel(final ExecutionMonitor exec, final PortObject[] inObjects) {
        return (CompiledModelPortObject)inObjects[0];
    }
}
