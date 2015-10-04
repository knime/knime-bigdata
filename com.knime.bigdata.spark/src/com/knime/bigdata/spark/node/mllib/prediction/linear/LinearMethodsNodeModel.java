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
 *   Created on Feb 12, 2015 by knime
 */
package com.knime.bigdata.spark.node.mllib.prediction.linear;

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.List;

import org.apache.spark.mllib.linalg.Vector;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.jobserver.jobs.AbstractRegularizationJob;
import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.node.mllib.MLlibSettings;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.port.model.SparkModelPortObjectSpec;
import com.knime.bigdata.spark.port.model.interpreter.SparkModelInterpreter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @param <M> the MLlib model
 */
public class LinearMethodsNodeModel<M extends Serializable> extends SparkNodeModel {

    private final LinearMethodsSettings m_settings = new LinearMethodsSettings();

    private Class<? extends AbstractRegularizationJob> m_jobClassPath;

    private SparkModelInterpreter<SparkModel<M>> m_interpreter;

    /**
     * Constructor.
     *
     * @param jobClassPath the class path to the job class
     * @param interpreter the SparkModelInterpreter
     */
    public LinearMethodsNodeModel(final Class<? extends AbstractRegularizationJob> jobClassPath,
        final SparkModelInterpreter<SparkModel<M>> interpreter) {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkModelPortObject.TYPE});
        m_jobClassPath = jobClassPath;
        m_interpreter = interpreter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1) {
            throw new InvalidSettingsException("No input data available");
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        //        final PMMLPortObjectSpec pmmlSpec = (PMMLPortObjectSpec)inSpecs[1];
        //      if (mapSpec != null && !SparkCategory2NumberNodeModel.MAP_SPEC.equals(mapSpec.getTableSpec())) {
        //          throw new InvalidSettingsException("Invalid mapping dictionary on second input port.");
        //      }
        m_settings.check(spec.getTableSpec());
        //MLlibClusterAssignerNodeModel.createSpec(tableSpec),
        return new PortObjectSpec[]{createMLSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[0];
        exec.setMessage("Starting " + m_interpreter.getModelName() + " learner");
        exec.checkCanceled();
        final MLlibSettings s = m_settings.getSettings(data);
        final double regularization = m_settings.getRegularization();
        final int noOfIterations = m_settings.getNoOfIterations();
        final LinearLearnerTask task;
        if (m_settings.getUseSGD()) {
            task = new LinearLearnerTask(data.getData(), s.getFeatueColIdxs(), s.getClassColIdx(), noOfIterations,
                regularization, m_settings.getUpdaterType(), m_settings.getValidateData(), m_settings.getAddIntercept(),
                m_settings.getUseFeatureScaling(), m_settings.getGradientType(), m_settings.getStepSize(),
                m_settings.getFraction(), m_jobClassPath);
        } else {
            task = new LinearLearnerTask(data.getData(), s.getFeatueColIdxs(), s.getClassColIdx(), noOfIterations,
                regularization, m_settings.getNoOfCorrections(), m_settings.getTolerance(), m_settings.getUpdaterType(),
                m_settings.getValidateData(), m_settings.getAddIntercept(), m_settings.getUseFeatureScaling(),
                m_settings.getGradientType(), m_jobClassPath);
        }
        @SuppressWarnings("unchecked")
        final M linearModel = (M)task.execute(exec);
        return new PortObject[]{new SparkModelPortObject<>(new SparkModel<>(linearModel, m_interpreter, s))};

    }

    /**
     * @return
     */
    private SparkModelPortObjectSpec createMLSpec() {
        return new SparkModelPortObjectSpec(m_interpreter.getModelName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
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
