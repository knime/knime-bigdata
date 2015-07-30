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
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnFilter2;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;

import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.node.AbstractSparkNodeModel;
import com.knime.bigdata.spark.node.preproc.convert.stringmapper.SparkStringMapperNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.port.model.SparkModelPortObjectSpec;
import com.knime.bigdata.spark.util.SparkUtil;

/**
 *
 * @author knime
 */
public class MLlibDecisionTreeNodeModel extends AbstractSparkNodeModel {

    private final SettingsModelString m_classCol = createClassColModel();

    private final SettingsModelString m_qualityMeasure = createQualityMeasureModel();

    private final SettingsModelColumnFilter2 m_cols = createColumnsModel();

    private final SettingsModelInteger m_maxNumberOfBins = createMaxNumberBinsModel();

    private final SettingsModelInteger m_maxDepth = createMaxDepthModel();

    /**
     * Constructor.
     */
    MLlibDecisionTreeNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE, SparkDataPortObject.TYPE_OPTIONAL},
            new PortType[]{SparkModelPortObject.TYPE});
    }

    /**
     * @return the quality measure model
     */
    static SettingsModelString createQualityMeasureModel() {
        return new SettingsModelString("qualityMeasure", ParameterConstants.VALUE_GINI);
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
    static SettingsModelIntegerBounded createMaxNumberBinsModel() {
        return new SettingsModelIntegerBounded("maxNumBins", 10, 1, Integer.MAX_VALUE);
    }

    /**
     * @return the maximum depth model
     */
    static SettingsModelIntegerBounded createMaxDepthModel() {
        return new SettingsModelIntegerBounded("maxDepth", 25, 1, Integer.MAX_VALUE);
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
        exec.setMessage("Starting Decision Tree (SPARK) Learner");
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
        final int maxDepth = m_maxDepth.getIntValue();
        final int maxNoOfBins = m_maxNumberOfBins.getIntValue();
        final String qualityMeasure = m_qualityMeasure.getStringValue();
        final DecisionTreeTask task = new DecisionTreeTask(data.getData(), featureColIdxs, featureColNames, classColName,
            classColIdx, mapping == null ? null : mapping.getData(), maxDepth, maxNoOfBins, qualityMeasure);
        final DecisionTreeModel treeModel = task.execute(exec);
        final MLlibDecisionTreeInterpreter interpreter = MLlibDecisionTreeInterpreter.getInstance();
        return new PortObject[]{new SparkModelPortObject<>(
                new SparkModel<>(treeModel, interpreter, tableSpec, classColName, featureColNames))};

    }

    /**
     * @return
     */
    private SparkModelPortObjectSpec createMLSpec() {
        return new SparkModelPortObjectSpec(MLlibDecisionTreeInterpreter.getInstance().getModelName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_classCol.saveSettingsTo(settings);
        m_cols.saveSettingsTo(settings);
        m_maxNumberOfBins.saveSettingsTo(settings);
        m_maxDepth.saveSettingsTo(settings);
        m_qualityMeasure.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_classCol.validateSettings(settings);
        m_cols.validateSettings(settings);
        m_maxNumberOfBins.validateSettings(settings);
        m_maxDepth.validateSettings(settings);
        m_qualityMeasure.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_classCol.loadSettingsFrom(settings);
        m_cols.loadSettingsFrom(settings);
        m_maxNumberOfBins.loadSettingsFrom(settings);
        m_maxDepth.loadSettingsFrom(settings);
        m_qualityMeasure.loadSettingsFrom(settings);
    }

}
