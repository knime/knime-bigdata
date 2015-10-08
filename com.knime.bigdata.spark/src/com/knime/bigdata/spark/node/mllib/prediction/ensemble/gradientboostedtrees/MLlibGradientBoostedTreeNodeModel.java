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
 *   Created on 27.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees;

import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
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
import com.knime.bigdata.spark.node.mllib.MLlibSettings;
import com.knime.bigdata.spark.node.mllib.prediction.decisiontree.MLlibDecisionTreeInterpreter;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.port.model.SparkModelPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibGradientBoostedTreeNodeModel extends SparkNodeModel {

    private final GradientBoostedTreeSettings m_forestSettings = new GradientBoostedTreeSettings();

    /**
     * Constructor.
     */
    protected MLlibGradientBoostedTreeNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE, PMMLPortObject.TYPE_OPTIONAL},
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
        final DataTableSpec tableSpec = spec.getTableSpec();
        m_forestSettings.check(tableSpec);
        return new PortObjectSpec[]{createMLSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inData[0];
        final PMMLPortObject mapping = (PMMLPortObject)inData[1];
        exec.setMessage("Starting Gradient Boosted Tree (SPARK) Learner");
        exec.checkCanceled();
        final MLlibSettings settings = m_forestSettings.getSettings(data, mapping);
        final GradientBoostedTreesTask task =
            new GradientBoostedTreesTask(data.getData(), settings.getFeatueColIdxs(), settings.getNominalFeatureInfo(),
                settings.getClassColName(), settings.getClassColIdx(), settings.getNumberOfClasses(),
                m_forestSettings.getMaxDepth(), m_forestSettings.getMaxNoOfBins(), m_forestSettings.getNoOfIterations(),
                m_forestSettings.getLearningRate(), m_forestSettings.isClassification());
        final GradientBoostedTreesModel model = task.execute(exec);
        final MLlibGradientBoostedTreeInterpreter interpreter = MLlibGradientBoostedTreeInterpreter.getInstance();
        return new PortObject[]{new SparkModelPortObject<>(new SparkModel<>(model, interpreter, settings))};

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
        m_forestSettings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_forestSettings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_forestSettings.loadSettingsFrom(settings);
    }

}
