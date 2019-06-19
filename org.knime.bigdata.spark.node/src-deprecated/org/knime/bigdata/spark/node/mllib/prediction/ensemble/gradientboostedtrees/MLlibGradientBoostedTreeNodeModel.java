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
 *   Created on 27.09.2015 by koetter
 */
package org.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees;

import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.gbt.GradientBoostedTreesLearnerSettings;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.AbstractMLlibTreeNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @author Ole Ostergaard
 */
public class MLlibGradientBoostedTreeNodeModel
extends AbstractMLlibTreeNodeModel<GradientBoostedTreesJobInput, GradientBoostedTreesLearnerSettings> {

    /**Unique model name.*/
    public static final String MODEL_NAME = "GradientBoostedTree";

    /**Unique job id.*/
    public static final String JOB_ID = MLlibGradientBoostedTreeNodeModel.class.getCanonicalName();

    /**
     * Constructor.
     */
    protected MLlibGradientBoostedTreeNodeModel() {
        super(MODEL_NAME, JOB_ID, new GradientBoostedTreesLearnerSettings(DecisionTreeLearnerMode.DEPRECATED));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected GradientBoostedTreesJobInput getJob(final GradientBoostedTreesLearnerSettings settings, final SparkDataPortObject data,
        final MLlibSettings mllibSettings) {
        return new GradientBoostedTreesJobInput(data.getTableName(), mllibSettings.getFeatueColIdxs(),
            mllibSettings.getNominalFeatureInfo(), mllibSettings.getClassColIdx(), mllibSettings.getNumberOfClasses(),
            settings.getMaxDepth(), settings.getMaxNoOfBins(), settings.getMaxNoOfModels(), settings.getLearningRate(),
            settings.isClassification(), settings.getQualityMeasure());
    }


}
