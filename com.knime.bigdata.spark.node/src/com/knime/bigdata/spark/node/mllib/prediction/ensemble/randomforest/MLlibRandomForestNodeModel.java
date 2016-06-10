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
package com.knime.bigdata.spark.node.mllib.prediction.ensemble.randomforest;

import com.knime.bigdata.spark.core.job.util.MLlibSettings;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.node.mllib.prediction.decisiontree.AbstractMLlibTreeNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @author Ole Ostergaard
 */
public class MLlibRandomForestNodeModel
extends AbstractMLlibTreeNodeModel<RandomForestJobInput, RandomForestSettings> {

    /**Unique model name.*/
    public static final String MODEL_NAME = "RandomForests";

    /**Unique job id.*/
    public static final String JOB_ID = MLlibRandomForestNodeModel.class.getCanonicalName();

    /**
     * Constructor.
     */
    protected MLlibRandomForestNodeModel() {
        super(MODEL_NAME, JOB_ID, new RandomForestSettings());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RandomForestJobInput getJob(final RandomForestSettings settings, final SparkDataPortObject data,
        final MLlibSettings mllibSettings) {
        return new RandomForestJobInput(data.getTableName(), mllibSettings.getFeatueColIdxs(),
            mllibSettings.getNominalFeatureInfo(), mllibSettings.getClassColIdx(), mllibSettings.getNumberOfClasses(),
            settings.getMaxDepth(), settings.getMaxNoOfBins(),  settings.getNoOfTrees(), settings.isClassification(),
            settings.getFeatureSubsetStragegy(), settings.getSeed(), settings.getQualityMeasure());
    }

}