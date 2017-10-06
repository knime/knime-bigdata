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
 *   Created on 02.10.2015 by koetter
 */
package com.knime.bigdata.spark2_0.jobs.ml.prediction.linear;

import java.text.NumberFormat;
import java.util.List;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegressionModel;

import com.knime.bigdata.spark.core.port.model.SparkModel;
import com.knime.bigdata.spark.core.port.model.interpreter.HTMLModelInterpreter;
import com.knime.bigdata.spark2_0.api.PipelineUtils;

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
        final LinearRegressionModel regressionModel = getRegressionModelStage(sparkModel);
        final Vector weightsVec = regressionModel.coefficients();
        final String weightString = printWeights(weightsVec, NF);
        return "Model intercept: "+regressionModel.intercept()+", weights: " + weightString;
    }

    private LinearRegressionModel getRegressionModelStage(final SparkModel sparkModel) {
        return PipelineUtils.getRegressionModelStage((PipelineModel)sparkModel.getModel(), sparkModel.getModelName());
    }

    private List<String> getLearningFeatureNames(final SparkModel sparkModel) {
        return PipelineUtils.getLearningFeatureNamesFromVectorAssembler((PipelineModel)sparkModel.getModel(), sparkModel.getModelName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getHTMLDescription(final SparkModel sparkModel) {
        final LinearRegressionModel regressionModel = getRegressionModelStage(sparkModel);

        List<String> columnNames = sparkModel.getLearningColumnNames();
        final double[] weights = regressionModel.coefficients().toArray();
        if (columnNames.size() != weights.length) {
            columnNames = getLearningFeatureNames(sparkModel);
        }
        return printWeightedColumnHTMLList("Weight", columnNames, NF, weights, regressionModel.intercept());
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
    static String printWeightedColumnHTMLList(final String numericColName, final List<String> columnNames,
        final NumberFormat nf, final double[] weights, final double intercept) {
        final StringBuilder buf = new StringBuilder();
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
        if (intercept != 0.0) {
            if (idx % 2 == 0) {
                buf.append("<tr>");
            } else {
                buf.append("<tr bgcolor='#EEEEEE'>");
            }
            buf.append("<th align='left'>").append("Intercept").append("</th>");
            buf.append("<td align='right'>&nbsp;&nbsp;").append(nf.format(intercept)).append("</td>");
            buf.append("</tr>");
        }
        buf.append("</table>");
        return buf.toString();
    }

}
