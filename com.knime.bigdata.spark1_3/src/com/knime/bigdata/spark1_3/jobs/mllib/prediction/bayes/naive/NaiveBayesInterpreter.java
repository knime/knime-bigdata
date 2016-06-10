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
package com.knime.bigdata.spark1_3.jobs.mllib.prediction.bayes.naive;

import java.util.List;

import org.apache.spark.mllib.classification.NaiveBayesModel;

import com.knime.bigdata.spark.core.port.model.SparkModel;
import com.knime.bigdata.spark.core.port.model.interpreter.HTMLModelInterpreter;
import com.knime.bigdata.spark.node.mllib.prediction.bayes.naive.MLlibNaiveBayesNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class NaiveBayesInterpreter extends HTMLModelInterpreter {

    private static final long serialVersionUID = 1L;

    private static volatile NaiveBayesInterpreter instance;

    private NaiveBayesInterpreter() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static NaiveBayesInterpreter getInstance() {
        if (instance == null) {
            synchronized (NaiveBayesInterpreter.class) {
                if (instance == null) {
                    instance = new NaiveBayesInterpreter();
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
        return MLlibNaiveBayesNodeModel.MODEL_NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final SparkModel model) {
        final NaiveBayesModel naiveBayesModel = (NaiveBayesModel)model.getModel();
        return "No of labels: " + naiveBayesModel.labels().length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHTMLDescription(final SparkModel model) {
        final NaiveBayesModel naiveBayesModel = (NaiveBayesModel)model.getModel();
        double[][] theta = naiveBayesModel.theta();
        List<String> featureColumnNames = model.getLearningColumnNames();
        double[] classLabels = naiveBayesModel.labels();
        double[] pi = naiveBayesModel.pi();
        StringBuilder buf = new StringBuilder();
        final String tdTag = "<td align='center' bgcolor='#F0F0F0'>";
        buf.append("<table width='100%'>");
        buf.append("<tr>");
        buf.append("<th>").append("Class").append("</th>");
        buf.append("<th>").append("Pi").append("</th>");
        for (final String featureCol : featureColumnNames) {
            buf.append("<th>").append(featureCol).append("</th>");
        }
        buf.append("</tr>");
        for (int i = 0; i < classLabels.length; i++) {
            buf.append("<tr>");
            buf.append("<th>").append(classLabels[i]).append("</th>");
            buf.append("<td>").append(NF.format(pi[i])).append("</td>");
            for (int j = 0; j < featureColumnNames.size(); j++) {
                buf.append(tdTag).append(NF.format(theta[i][j])).append("</td>");
            }
            buf.append("</tr>");
        }
        buf.append("</table>");
        return buf.toString();
    }

}
