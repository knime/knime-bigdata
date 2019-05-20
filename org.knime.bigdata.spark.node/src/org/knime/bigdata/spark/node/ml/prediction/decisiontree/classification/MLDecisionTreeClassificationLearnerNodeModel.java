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
package org.knime.bigdata.spark.node.ml.prediction.decisiontree.classification;

import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.AbstractMLTreeNodeModel;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeSettings;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTreeJobInput;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class MLDecisionTreeClassificationLearnerNodeModel extends AbstractMLTreeNodeModel<MLDecisionTreeJobInput, DecisionTreeSettings> {

    /**Unique model name.*/
    public static final String MODEL_NAME = "MLDecisionTreeClassification";

    /**Unique job id.*/
    public static final String JOB_ID = "MLDecisionTreeClassificationLearnerJob";

    /**
     * Constructor.
     */
    public MLDecisionTreeClassificationLearnerNodeModel() {
        super(MODEL_NAME, JOB_ID, new DecisionTreeSettings(DecisionTreeLearnerMode.CLASSIFICATION));
     }

    @Override
    protected MLDecisionTreeJobInput createJobInput(final PortObject[] inData,
        final String newNamedModelId,
        final DecisionTreeSettings settings) throws InvalidSettingsException {

        final SparkDataPortObject data = (SparkDataPortObject) inData[0];
        final MLlibSettings mlSettings = settings.getSettings(data);

        return new MLDecisionTreeClassificationLearnerJobInput(data.getTableName(),
            newNamedModelId,
            mlSettings.getClassColIdx(),
            mlSettings.getFeatueColIdxs(),
            settings.getMaxDepth(),
            settings.getMaxNoOfBins(),
            settings.getMinRowsPerNodeChild(),
            settings.getQualityMeasure());
    }
}
