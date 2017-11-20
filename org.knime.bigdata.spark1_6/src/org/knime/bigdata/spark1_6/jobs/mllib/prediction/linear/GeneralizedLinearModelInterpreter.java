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
 *   Created on 02.10.2015 by koetter
 */
package org.knime.bigdata.spark1_6.jobs.mllib.prediction.linear;

import java.text.NumberFormat;
import java.util.List;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.GeneralizedLinearModel;
import org.knime.bigdata.spark.core.port.model.SparkModel;
import org.knime.bigdata.spark.core.port.model.interpreter.HTMLModelInterpreter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class GeneralizedLinearModelInterpreter extends HTMLModelInterpreter {

    private static final long serialVersionUID = 1L;

    private final String m_modelName;

    /**
     * Constructor.
     * @param modelName the name of the model
     */
    protected GeneralizedLinearModelInterpreter(final String modelName) {
        m_modelName = modelName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getModelName() {
        return m_modelName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final SparkModel sparkModel) {
        final GeneralizedLinearModel regressionModel = (GeneralizedLinearModel)sparkModel.getModel();
        final Vector weightsVec = regressionModel.weights();
        final NumberFormat nf = getNumberFormat();
        final String weightString = printWeights(weightsVec, nf);
        return "Model weights: " + weightString;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getHTMLDescription(final SparkModel sparkModel) {
        final GeneralizedLinearModel model = (GeneralizedLinearModel)sparkModel.getModel();
        final List<String> columnNames = sparkModel.getLearningColumnNames();
        final NumberFormat nf = getNumberFormat();
        final double[] weights = model.weights().toArray();
        return printWeightedColumnHTMLList("Weight", columnNames, nf, weights);
    }

    /**
     * @param weights the weights vector
     * @param nf {@link NumberFormat} to use
     * @return the String representation
     */
    public static String printWeights(final Vector weights, final NumberFormat nf) {
        final StringBuilder buf = new StringBuilder();
        final double[] weightsArray = weights.toArray();
        for (int i = 0, length = weightsArray.length; i < length; i++) {
            if (i > 0) {
                buf.append(", ");
            }
            buf.append(nf.format(weightsArray[i]));
        }
        final String weightString = buf.toString();
        return weightString;
    }

    /**
     * @param numericColName the title of the numeric column
     * @param columnNames the column names
     * @param nf
     * @param weights the weight of each column
     * @return a string of an HTML list with the columns and their weight
     */
    public static String printWeightedColumnHTMLList(final String numericColName, final List<String> columnNames,
        final NumberFormat nf, final double[] weights) {
        final StringBuilder buf = new StringBuilder();
        //        for (String string : columnNames) {
        //            buf.append("&nbsp;&nbsp;<tt>").append(string).append(":</tt>");
        //            buf.append("&nbsp;").append(weights[idx++]).append("<br>");
        //        }
        buf.append("<table border ='0'>");
        buf.append("<tr>");
        buf.append("<th>").append("Column Name").append("</th>");
        buf.append("<th>").append(numericColName).append("</th>");
        buf.append("</tr>");
        int idx = 0;
        for (String colName : columnNames) {
            if (idx % 2 == 0) {
                buf.append("<tr>");
            } else {
                buf.append("<tr bgcolor='#EEEEEE'>");
            }
            buf.append("<th align='left'>").append(colName).append("</th>");
            buf.append("<td align='right'>&nbsp;&nbsp;").append(nf.format(weights[idx++])).append("</td>");
            buf.append("</tr>");
        }
        buf.append("</table>");
        //        buf.append("<dl>");
        //        for (String string : columnNames) {
        //            buf.append("<dt>&nbsp;&nbsp;").append(string).append("</dt>");
        //            buf.append("<dd>").append(weights[idx++]).append("</dd>");
        //        }
        //        buf.append("</dl>");
        return buf.toString();
    }

}
