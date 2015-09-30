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
 *   Created on Feb 12, 2015 by knime
 */
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;

import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.node.mllib.MLlibNodeSettings;
import com.knime.bigdata.spark.node.mllib.MLlibSettings;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.port.model.SparkModelPortObjectSpec;

/**
 *
 * @author knime
 */
public class MLlibDecisionTreeNodeModel extends SparkNodeModel {

    private final MLlibNodeSettings m_settings = new MLlibNodeSettings(true);

    private final DecisionTreeSettings m_treeSettings = new DecisionTreeSettings();

    /** Index of input data port. */
    static final int DATA_INPORT = 0;

    /**
     * Constructor.
     */
    MLlibDecisionTreeNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE, new PortType(PMMLPortObject.class, true)},
            new PortType[]{SparkModelPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 2) {
            throw new InvalidSettingsException("");
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        //        final PMMLPortObjectSpec pmmlSpec = (PMMLPortObjectSpec)inSpecs[1];
        //        if (mapSpec != null && !SparkCategory2NumberNodeModel.MAP_SPEC.equals(mapSpec.getTableSpec())) {
        //            throw new InvalidSettingsException("Invalid mapping dictionary on second input port.");
        //        }
        final DataTableSpec tableSpec = spec.getTableSpec();
        m_settings.check(tableSpec);
        //MLlibClusterAssignerNodeModel.createSpec(tableSpec),
        return new PortObjectSpec[]{createMLSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[0];
        final PMMLPortObject mapping = (PMMLPortObject)inObjects[1];
        exec.setMessage("Starting Decision Tree (SPARK) Learner");
        exec.checkCanceled();
        final MLlibSettings settings = m_settings.getSettings(data, mapping);
        final int maxDepth = m_treeSettings.getMaxDepth();
        final int maxNoOfBins = m_treeSettings.getMaxNoOfBins();
        final String qualityMeasure = m_treeSettings.getQualityMeasure();
        final DecisionTreeTask task =
            new DecisionTreeTask(data.getData(), settings.getFeatueColIdxs(), settings.getFatureColNames(),
                settings.getNominalFeatureInfo(), settings.getClassColName(), settings.getClassColIdx(),
                settings.getNumberOfClasses(), maxDepth, maxNoOfBins, qualityMeasure);
        final DecisionTreeModel treeModel = task.execute(exec);


        final MLlibDecisionTreeInterpreter interpreter = MLlibDecisionTreeInterpreter.getInstance();
        return new PortObject[]{new SparkModelPortObject<>(new SparkModel<>(treeModel, interpreter, settings))};

    }

    /**
     * @return
     */
    private SparkModelPortObjectSpec createMLSpec() {
        return new SparkModelPortObjectSpec(MLlibDecisionTreeInterpreter.getInstance().getModelName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
        m_treeSettings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
        m_treeSettings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
        m_treeSettings.loadSettingsFrom(settings);
    }

}
