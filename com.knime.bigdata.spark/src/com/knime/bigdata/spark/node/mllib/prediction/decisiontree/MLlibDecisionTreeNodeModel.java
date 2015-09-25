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
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;

import com.knime.bigdata.spark.jobserver.jobs.AbstractTreeLearnerJob;
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

    private final SettingsModelString m_qualityMeasure = createQualityMeasureModel();

    private final SettingsModelInteger m_maxNumberOfBins = createMaxNumberBinsModel();

    private final SettingsModelInteger m_maxDepth = createMaxDepthModel();

    /**
     * Constructor.
     */
    MLlibDecisionTreeNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE, new PortType(PMMLPortObject.class, true)},
            new PortType[]{SparkModelPortObject.TYPE});
    }

    /**
     * @return the quality measure model
     */
    static SettingsModelString createQualityMeasureModel() {
        return new SettingsModelString("qualityMeasure", AbstractTreeLearnerJob.VALUE_GINI);
    }

    /**
     * @return the maximum number of bins model
     */
    static SettingsModelIntegerBounded createMaxNumberBinsModel() {
        return new SettingsModelIntegerBounded("maxNumBins", 10, 1, Integer.MAX_VALUE);
    }

    /**
     * @return the maximum depth model
     */
    static SettingsModelIntegerBounded createMaxDepthModel() {
        return new SettingsModelIntegerBounded("maxDepth", 25, 1, Integer.MAX_VALUE);
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
        final int maxDepth = m_maxDepth.getIntValue();
        final int maxNoOfBins = m_maxNumberOfBins.getIntValue();
        final String qualityMeasure = m_qualityMeasure.getStringValue();
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
        m_maxNumberOfBins.saveSettingsTo(settings);
        m_maxDepth.saveSettingsTo(settings);
        m_qualityMeasure.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
        m_maxNumberOfBins.validateSettings(settings);
        m_maxDepth.validateSettings(settings);
        m_qualityMeasure.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
        m_maxNumberOfBins.loadSettingsFrom(settings);
        m_maxDepth.loadSettingsFrom(settings);
        m_qualityMeasure.loadSettingsFrom(settings);
    }

}
