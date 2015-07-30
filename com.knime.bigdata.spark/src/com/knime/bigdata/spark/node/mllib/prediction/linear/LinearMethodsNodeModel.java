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
import java.util.Arrays;
import java.util.List;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnFilter2;
import org.knime.core.node.defaultnodesettings.SettingsModelDouble;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;

import com.knime.bigdata.spark.jobserver.jobs.SGDJob;
import com.knime.bigdata.spark.node.AbstractSparkNodeModel;
import com.knime.bigdata.spark.node.convert.stringmapper.SparkStringMapperNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.SparkModelInterpreter;
import com.knime.bigdata.spark.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.port.model.SparkModelPortObjectSpec;
import com.knime.bigdata.spark.util.SparkUtil;

/**
 *
 * @author koetter
 * @param <M> the MLlib model
 */
public class LinearMethodsNodeModel<M extends Serializable> extends AbstractSparkNodeModel {

    private final SettingsModelString m_classCol = createClassColModel();

    private final SettingsModelColumnFilter2 m_cols = createColumnsModel();

    private final SettingsModelInteger m_noOfIterations = createNumberOfIterationsModel();

    private final SettingsModelDouble m_regularization = createRegularizationModel();

    private Class<? extends SGDJob> m_jobClassPath;

    private SparkModelInterpreter<M> m_interpreter;

    /**
     * Constructor.
     * @param jobClassPath the class path to the job class
     * @param interpreter the SparkModelInterpreter
     */
    public LinearMethodsNodeModel(final Class<? extends SGDJob> jobClassPath, final SparkModelInterpreter<M> interpreter) {
        super(new PortType[]{SparkDataPortObject.TYPE, SparkDataPortObject.TYPE_OPTIONAL},
            new PortType[]{SparkModelPortObject.TYPE});
        m_jobClassPath = jobClassPath;
        m_interpreter = interpreter;
    }

    /**
     * @return the class column model
     */
    static SettingsModelString createClassColModel() {
        return new SettingsModelString("classColumn", null);
    }

    /**
     * @return the feature column model
     */
    @SuppressWarnings("unchecked")
    static SettingsModelColumnFilter2 createColumnsModel() {
        return new SettingsModelColumnFilter2("featureColumns", DoubleValue.class);
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
        final SparkDataPortObjectSpec mapSpec = (SparkDataPortObjectSpec)inSpecs[1];
        if (mapSpec != null && !SparkStringMapperNodeModel.MAP_SPEC.equals(mapSpec.getTableSpec())) {
            throw new InvalidSettingsException("Invalid mapping dictionary on second input port.");
        }
        final DataTableSpec tableSpec = spec.getTableSpec();
        final String classColName = m_classCol.getStringValue();
        if (classColName == null) {
            throw new InvalidSettingsException("No class column selected");
        }
        int classColIdx = tableSpec.findColumnIndex(classColName);
        if (classColIdx < 0) {
            throw new InvalidSettingsException("Class column :" + classColName + " not found in input data");
        }
        final FilterResult result = m_cols.applyTo(tableSpec);
        final List<String> featureColNames =  Arrays.asList(result.getIncludes());
        Integer[] featureColIdxs = SparkUtil.getColumnIndices(tableSpec, featureColNames);
        if (Arrays.asList(featureColIdxs).contains(Integer.valueOf(classColIdx))) {
            throw new InvalidSettingsException("Class column also selected as feature column");
        }
        //MLlibClusterAssignerNodeModel.createSpec(tableSpec),
        return new PortObjectSpec[]{createMLSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[0];
        final SparkDataPortObject mapping = (SparkDataPortObject)inObjects[1];
        exec.setMessage("Starting " + m_interpreter.getModelName() + " learner");
        exec.checkCanceled();
        final DataTableSpec tableSpec = data.getTableSpec();
        final String classColName = m_classCol.getStringValue();
        int classColIdx = tableSpec.findColumnIndex(classColName);
        if (classColIdx < 0) {
            throw new InvalidSettingsException("Class column :" + classColName + " not found in input data");
        }
        final FilterResult filterResult = m_cols.applyTo(tableSpec);
        final List<String> featureColNames = Arrays.asList(filterResult.getIncludes());
        final Integer[] featureColIdxs = SparkUtil.getColumnIndices(tableSpec, featureColNames);
        if (Arrays.asList(featureColIdxs).contains(Integer.valueOf(classColIdx))) {
            throw new InvalidSettingsException("Class column also selected as feature column");
        }
        final double regularization = m_regularization.getDoubleValue();
        final int noOfIterations = m_noOfIterations.getIntValue();
        final SGDLearnerTask task = new SGDLearnerTask(data.getData(), featureColIdxs, featureColNames, classColName,
            classColIdx, mapping == null ? null : mapping.getData(), noOfIterations, regularization, m_jobClassPath);
        @SuppressWarnings("unchecked")
        final M linearModel = (M)task.execute(exec);
        return new PortObject[]{new SparkModelPortObject<>(new SparkModel<>(linearModel,
                m_interpreter, tableSpec, classColName, featureColNames))};

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
        m_cols.saveSettingsTo(settings);
        m_noOfIterations.saveSettingsTo(settings);
        m_regularization.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_classCol.validateSettings(settings);
        m_cols.validateSettings(settings);
        m_noOfIterations.validateSettings(settings);
        m_regularization.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_classCol.loadSettingsFrom(settings);
        m_cols.loadSettingsFrom(settings);
        m_noOfIterations.loadSettingsFrom(settings);
        m_regularization.loadSettingsFrom(settings);
    }

}
