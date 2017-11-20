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
 *   Created on Feb 12, 2015 by knime
 */
package org.knime.bigdata.spark.node.mllib.prediction.linear;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import org.knime.bigdata.spark.core.job.JobRunFactory;
import org.knime.bigdata.spark.core.job.ModelJobOutput;
import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.SparkContextProvider;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.model.SparkModelPortObject;
import org.knime.bigdata.spark.core.port.model.SparkModelPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class LinearMethodsNodeModel extends SparkNodeModel {

    private final LinearMethodsSettings m_settings = new LinearMethodsSettings();

    private final String m_jobID;

    private final String m_modelName;

    /**
     * Constructor.
     *
     * @param jobID the unique Spark job id
     * @param modelName the unique model name
     */
    public LinearMethodsNodeModel(final String jobID, final String modelName) {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkModelPortObject.TYPE});
        m_jobID = jobID;
        m_modelName = modelName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1) {
            throw new InvalidSettingsException("No input data available");
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        //        final PMMLPortObjectSpec pmmlSpec = (PMMLPortObjectSpec)inSpecs[1];
        //      if (mapSpec != null && !SparkCategory2NumberNodeModel.MAP_SPEC.equals(mapSpec.getTableSpec())) {
        //          throw new InvalidSettingsException("Invalid mapping dictionary on second input port.");
        //      }
        m_settings.check(spec.getTableSpec());
        //MLlibClusterAssignerNodeModel.createSpec(tableSpec),
        return new PortObjectSpec[]{createMLSpec(spec)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[0];
        exec.setMessage("Starting " + m_modelName + " learner");
        final JobRunFactory<LinearLearnerJobInput, ModelJobOutput> runFactory = getJobRunFactory(data, m_jobID);
        exec.checkCanceled();
        final MLlibSettings s = m_settings.getSettings(data);
        final LinearLearnerJobInput jobInput;
        if (m_settings.getUseSGD()) {
            jobInput = new LinearLearnerJobInput(data.getData().getID(), s.getFeatueColIdxs(), s.getClassColIdx(),
                m_settings.getNoOfIterations(), m_settings.getRegularization(), m_settings.getRegularizerType(),
                m_settings.getValidateData(), m_settings.getAddIntercept(), m_settings.getUseFeatureScaling(),
                m_settings.getLossFuntion(), m_settings.getStepSize(), m_settings.getFraction());
        } else {
            jobInput = new LinearLearnerJobInput(data.getData().getID(), s.getFeatueColIdxs(), s.getClassColIdx(),
                m_settings.getNoOfIterations(), m_settings.getRegularization(), m_settings.getRegularizerType(),
                m_settings.getValidateData(), m_settings.getAddIntercept(), m_settings.getUseFeatureScaling(),
                m_settings.getLossFuntion(), m_settings.getNoOfCorrections(), m_settings.getTolerance());
        }
        final ModelJobOutput result = runFactory.createRun(jobInput).run(data.getContextID(), exec);
        return new PortObject[]{createSparkModelPortObject(data, m_modelName, s, result)};

    }

    /**
     * @return
     */
    private SparkModelPortObjectSpec createMLSpec(final SparkContextProvider context) {
        return new SparkModelPortObjectSpec(getSparkVersion(context), m_modelName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
    }
}
