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
 *   Created on 21.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.bayes.naive;

import org.apache.spark.mllib.classification.NaiveBayesModel;

import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.interpreter.HTMLModelInterpreter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibNaiveBayesInterpreter extends HTMLModelInterpreter<SparkModel<NaiveBayesModel>> {

    private static volatile MLlibNaiveBayesInterpreter instance;

    private MLlibNaiveBayesInterpreter() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static MLlibNaiveBayesInterpreter getInstance() {
        if (instance == null) {
            synchronized (MLlibNaiveBayesInterpreter.class) {
                if (instance == null) {
                    instance = new MLlibNaiveBayesInterpreter();
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
        return "Naive Bayes";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final SparkModel<NaiveBayesModel> model) {
        final NaiveBayesModel naiveBayesModel = model.getModel();
        return "No of labels: " + naiveBayesModel.labels().length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHTMLDescription(final SparkModel<NaiveBayesModel> model) {
        final NaiveBayesModel naiveBayesModel = model.getModel();
        return naiveBayesModel.toString();
    }

}
