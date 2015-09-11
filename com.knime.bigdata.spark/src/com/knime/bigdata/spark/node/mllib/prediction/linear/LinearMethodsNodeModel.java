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

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelDouble;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;

import com.knime.bigdata.spark.jobserver.jobs.SGDJob;
import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.node.mllib.MLlibNodeSettings;
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

    private final MLlibNodeSettings m_classCol = new MLlibNodeSettings(true);

    private final SettingsModelInteger m_noOfIterations = createNumberOfIterationsModel();

    private final SettingsModelDouble m_regularization = createRegularizationModel();

    private Class<? extends SGDJob> m_jobClassPath;

    private SparkModelInterpreter<SparkModel<M>> m_interpreter;

    /**
     * Constructor.
     * @param jobClassPath the class path to the job class
     * @param interpreter the SparkModelInterpreter
     */
    public LinearMethodsNodeModel(final Class<? extends SGDJob> jobClassPath,
        final SparkModelInterpreter<SparkModel<M>> interpreter) {
        super(new PortType[]{SparkDataPortObject.TYPE, new PortType(PMMLPortObject.class, true)},
            new PortType[]{SparkModelPortObject.TYPE});
        m_jobClassPath = jobClassPath;
        m_interpreter = interpreter;
    }

    /**
     * @return the maximum number of bins model
     */
    static SettingsModelIntegerBounded createNumberOfIterationsModel() {
        return new SettingsModelIntegerBounded("numberOfIteration", 100, 1, Integer.MAX_VALUE);
    }

    /**
     * @return the regularization model
     */
    static SettingsModelDouble createRegularizationModel() {
        return new SettingsModelDouble("regularization", 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 2) {
            throw new InvalidSettingsException("");
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
//        final PMMLPortObjectSpec pmmlSpec = (PMMLPortObjectSpec)inSpecs[1];
//      if (mapSpec != null && !SparkCategory2NumberNodeModel.MAP_SPEC.equals(mapSpec.getTableSpec())) {
//          throw new InvalidSettingsException("Invalid mapping dictionary on second input port.");
//      }
        m_classCol.check(spec.getTableSpec());
        //MLlibClusterAssignerNodeModel.createSpec(tableSpec),
        return new PortObjectSpec[]{createMLSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[0];
        final PMMLPortObject mapping = (PMMLPortObject)inObjects[1];
        exec.setMessage("Starting " + m_interpreter.getModelName() + " learner");
        exec.checkCanceled();
        final MLlibSettings s = m_classCol.getSettings(data, mapping);
        final double regularization = m_regularization.getDoubleValue();
        final int noOfIterations = m_noOfIterations.getIntValue();
        final SGDLearnerTask task = new SGDLearnerTask(data.getData(), s.getFeatueColIdxs(), s.getFatureColNames(),
            s.getClassColName(), s.getClassColIdx(), s.getNominalFeatureInfo(), noOfIterations, regularization,
            m_jobClassPath);
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
        m_classCol.saveSettingsTo(settings);
        m_noOfIterations.saveSettingsTo(settings);
        m_regularization.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_classCol.validateSettings(settings);
        m_noOfIterations.validateSettings(settings);
        m_regularization.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_classCol.loadSettingsFrom(settings);
        m_noOfIterations.loadSettingsFrom(settings);
        m_regularization.loadSettingsFrom(settings);
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
