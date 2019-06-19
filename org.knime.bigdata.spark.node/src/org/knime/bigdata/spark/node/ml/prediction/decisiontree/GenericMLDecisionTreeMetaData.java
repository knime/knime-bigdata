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
 *   Created on Jun 19, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;

/**
 * Abstract superclass for MLMetaData object that contain metadata about decision trees or tree ensembles.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class GenericMLDecisionTreeMetaData extends MLMetaData {

    private static final String KEY_FEATURE_IMPORTANCES = "featureImportances";

    /**
     * Constructor for (de)serialization.
     */
    public GenericMLDecisionTreeMetaData() {
    }

    /**
     *
     * @param featureImportances Estimate of the importance of each feature.
     */
    public GenericMLDecisionTreeMetaData(final double[] featureImportances) {
        setDoubleArray(KEY_FEATURE_IMPORTANCES, featureImportances);
    }

    /**
     * Each feature's importance is the average of its importance across all trees in the ensemble The feature
     * importances are normalized to sum to 1.
     *
     * @return an estimate of the importance of each feature.
     */
    public double[] getFeatureImportances() {
        return getDoubleArray(KEY_FEATURE_IMPORTANCES);
    }
}
