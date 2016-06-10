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
 *   Created on Oct 27, 2015 by ste
 */
package com.knime.bigdata.spark1_2.jobs.mllib.prediction.linear.regression;

import org.knime.base.node.mine.regression.PMMLRegressionTranslator;
import org.knime.base.node.mine.regression.PMMLRegressionTranslator.RegressionTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.port.pmml.PMMLPortObjectSpecCreator;
import org.knime.core.node.port.pmml.PMMLTranslator;

import com.knime.bigdata.spark.core.port.model.SparkModel;
import com.knime.bigdata.spark.node.mllib.prediction.linear.regression.MLlibLinearRegressionNodeFactory;
import com.knime.bigdata.spark1_2.jobs.mllib.prediction.linear.GeneralizedLinearModelPMMLPortObjectFactory;

/**
 *
 * @author Stefano Baghino <stefano.baghino@databiz.it>
 */
public class LinearRegressionModelPMMLPortObjectFactory extends GeneralizedLinearModelPMMLPortObjectFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public String getModelName() {
        return MLlibLinearRegressionNodeFactory.MODEL_NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PMMLPortObject convert(final SparkModel knimeModel) throws InvalidSettingsException {
        final DataTableSpec learnerSpec = knimeModel.getTableSpec();
        final String targetField = knimeModel.getClassColumnName();
        PMMLPortObjectSpecCreator creator = new PMMLPortObjectSpecCreator(learnerSpec);
        creator.setLearningCols(learnerSpec);
        creator.setTargetColName(targetField);
        PMMLPortObject pmmlOutputPort = new PMMLPortObject(creator.createSpec());

        final String modelName = learnerSpec.getName();
        final String algorithmName = knimeModel.getModelName();

        final RegressionTable regressionTable = regressionTableFromModel(knimeModel);

        PMMLTranslator modelTranslator =
                new PMMLRegressionTranslator(modelName, algorithmName, regressionTable, targetField);

        pmmlOutputPort.addModelTranslater(modelTranslator);

        return pmmlOutputPort;

    }

}
