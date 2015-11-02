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
 *   Created on Oct 26, 2015 by ste
 */
package com.knime.bigdata.spark.node.pmml.converter.impl;

import org.apache.spark.mllib.classification.SVMModel;
import org.knime.base.node.mine.regression.PMMLRegressionTranslator;
import org.knime.base.node.mine.regression.PMMLRegressionTranslator.RegressionTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.port.pmml.PMMLPortObjectSpecCreator;
import org.knime.core.node.port.pmml.PMMLTranslator;

import com.knime.bigdata.spark.port.model.SparkModel;

/**
 *
 * @author Stefano Baghino <stefano.baghino@databiz.it>
 */
public class SVMModelPMMLPortObjectFactory extends GeneralizedLinearModelPMMLPortObjectFactory<SVMModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<SVMModel> getSupportedModelClass() {
        return SVMModel.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PMMLPortObject convert(final SparkModel<SVMModel> knimeModel) throws InvalidSettingsException {
        final DataTableSpec learnerSpec = knimeModel.getTableSpec();
        final String targetField = knimeModel.getClassColumnName();
        PMMLPortObjectSpecCreator creator = new PMMLPortObjectSpecCreator(learnerSpec);
        creator.setLearningCols(learnerSpec);
        creator.setTargetColName(targetField);
        PMMLPortObject pmmlOutputPort = new PMMLPortObject(creator.createSpec());

        String modelName = learnerSpec.getName();
        String algorithmName = knimeModel.getType();
        RegressionTable regressionTable = regressionTableFromModel(knimeModel);

        PMMLTranslator translator = new PMMLRegressionTranslator(modelName, algorithmName, regressionTable, targetField);

        pmmlOutputPort.addModelTranslater(translator);
        pmmlOutputPort.initializeModelTranslator(translator);

        return pmmlOutputPort;

    }

}
