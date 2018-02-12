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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.util.SparkPMMLUtil;
import org.knime.bigdata.spark.node.pmml.transformation.AbstractSparkTransformationPMMLApplyNodeModel;
import org.knime.core.data.DataColumnSpec;
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

        final Collection<String> missingFieldNames  = new LinkedList<>();
        SparkPMMLUtil.getColumnIndices(sparkSpec.getTableSpec(), pmmlSpec, missingFieldNames);
        if (!missingFieldNames.isEmpty()) {
            setWarningMessage("Missing input fields: " + missingFieldNames);
        }

        final List<Integer> addCols = new LinkedList<>();
        final List<Integer> skipCols = new LinkedList<>();
        final Map<Integer, Integer> replaceCols = new HashMap<>();
        final DataTableSpec resultSpec = createTransformationResultSpec(sparkSpec.getTableSpec(),
            null, pmmlSpec, addCols, skipCols, replaceCols);
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

    @Override
    protected DataTableSpec createTransformationResultSpec(final DataTableSpec inSpec, final PortObject pmmlPort,
        final CompiledModelPortObjectSpec cms, final List<Integer> addCols, final List<Integer> skipCols,
        final Map<Integer, Integer> replaceCols) throws InvalidSettingsException {

        final DataColumnSpec[] pmmlResultColSpecs = cms.getTransformationsResultColSpecs(inSpec);
        final Set<String> pmmlInputColNames = cms.getInputIndices().keySet();
        final List<DataColumnSpec> appendSpecs = new ArrayList<>(pmmlResultColSpecs.length);

        // add only the specs to the result that have a matching input column
        for (int i = 0; i < pmmlResultColSpecs.length; i++) {
            if (hasInputColumn(inSpec, pmmlInputColNames, pmmlResultColSpecs[i].getName())) {
                appendSpecs.add(pmmlResultColSpecs[i]);
                addCols.add(i);
            }
        }

        return new DataTableSpec(inSpec, new DataTableSpec(appendSpecs.toArray(new DataColumnSpec[0])));
    }

    /**
     * Find PMML input column with longest matching result col name and return <code>true</code> if input spec contains
     * this column.
     */
    private boolean hasInputColumn(final DataTableSpec inSpec, final Set<String> pmmlInputCols, final String resultColName) {
        // find best matching PMML input column name
        String inputCol = null;
        for (String name : pmmlInputCols) {
            if (resultColName.startsWith(name) && (inputCol == null || inputCol.length() < name.length())) {
                inputCol = name;
            }
        }

        // validate that input spec has detected column
        return inputCol != null && inSpec.containsName(inputCol);
    }
}
