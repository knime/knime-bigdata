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
package org.knime.bigdata.spark.node.mllib.prediction.bayes.naive;

import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.bigdata.spark.core.node.SparkModelLearnerNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelDouble;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;
import org.knime.core.node.port.PortObject;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibNaiveBayesNodeModel extends SparkModelLearnerNodeModel<NaiveBayesJobInput, MLlibNodeSettings> {

    private final SettingsModelDouble m_lambda = createLambdaModel();

    /**Unique model name.*/
    public static final String MODEL_NAME = "Naive Bayes";

    /**Unique job id.*/
    public static final String JOB_ID = MLlibNaiveBayesNodeModel.class.getCanonicalName();

    /**
     * Constructor.
     */
    MLlibNaiveBayesNodeModel() {
        super(MODEL_NAME, JOB_ID, true);
    }

    /**
     * @return the lambda model
     */
    static SettingsModelDouble createLambdaModel() {
        return new SettingsModelDoubleBounded("lambda", 1, 0, 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected NaiveBayesJobInput createJobInput(final PortObject[] inData, final MLlibNodeSettings settings)
        throws InvalidSettingsException {
        final SparkDataPortObject data = (SparkDataPortObject)inData[0];
        final MLlibSettings mLlibSettings = settings.getSettings(data);
        return new NaiveBayesJobInput(data.getTableName(), mLlibSettings.getClassColIdx(),
            mLlibSettings.getFeatueColIdxs(), m_lambda.getDoubleValue(), null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveModelLearnerSettingsTo(final NodeSettingsWO settings) {
        m_lambda.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateModelLearnerSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_lambda.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedModelLearnerSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_lambda.loadSettingsFrom(settings);
    }

}
