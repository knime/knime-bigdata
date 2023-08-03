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
 *   Created on 21.07.2015 by koetter
 */
package org.knime.bigdata.spark3_4.jobs.mllib.prediction.ensemble.randomforest;

import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.knime.bigdata.spark.node.mllib.prediction.ensemble.randomforest.MLlibRandomForestNodeModel;
import org.knime.bigdata.spark3_4.jobs.mllib.prediction.ensemble.MLlibTreeEnsembleModelInterpreter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class RandomForestInterpreter extends MLlibTreeEnsembleModelInterpreter<RandomForestModel> {
    //implements SparkModelInterpreter<SparkModel<DecisionTreeModel>> {

    private static final long serialVersionUID = 1L;

    private static volatile RandomForestInterpreter instance;

    private RandomForestInterpreter() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public static RandomForestInterpreter getInstance() {
        if (instance == null) {
            synchronized (RandomForestInterpreter.class) {
                if (instance == null) {
                    instance = new RandomForestInterpreter();
                }
            }
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getModelName() {
        return MLlibRandomForestNodeModel.MODEL_NAME;
    }
}
