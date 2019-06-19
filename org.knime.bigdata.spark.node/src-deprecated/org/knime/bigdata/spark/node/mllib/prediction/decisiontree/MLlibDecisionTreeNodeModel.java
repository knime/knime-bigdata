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
package org.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeSettings;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @author Ole Ostergaard
 */
@Deprecated
public class MLlibDecisionTreeNodeModel extends AbstractMLlibTreeNodeModel<DecisionTreeJobInput, DecisionTreeSettings> {

    /**Unique model name.*/
    public static final String MODEL_NAME = "DecisionTree";
    /**Unique job id.*/
    public static final String JOB_ID = MLlibDecisionTreeNodeModel.class.getCanonicalName();

    /**
     * Constructor.
     */
    public MLlibDecisionTreeNodeModel() {
        super(MODEL_NAME, JOB_ID, new DecisionTreeSettings(DecisionTreeLearnerMode.DEPRECATED));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DecisionTreeJobInput getJob(final DecisionTreeSettings settings, final SparkDataPortObject data,
        final MLlibSettings mllibSettings) {
        return new DecisionTreeJobInput(data.getTableName(), mllibSettings.getFeatueColIdxs(),
            mllibSettings.getNominalFeatureInfo(), mllibSettings.getClassColIdx(), mllibSettings.getNumberOfClasses(),
            settings.isClassification(),
            settings.getMaxDepth(), settings.getMaxNoOfBins(), settings.getQualityMeasure());
    }
}
