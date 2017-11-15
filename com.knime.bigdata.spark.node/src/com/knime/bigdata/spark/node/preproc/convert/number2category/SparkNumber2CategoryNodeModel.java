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
 *   Created on 21.08.2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.convert.number2category;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.port.pmml.PMMLPortObjectSpec;

import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.job.SimpleJobRunFactory;
import com.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;
import com.knime.bigdata.spark.core.job.util.ColumnBasedValueMappings;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;
import com.knime.bigdata.spark.core.util.SparkIDs;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkNumber2CategoryNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkNumber2CategoryNodeModel.class.getCanonicalName();

    private final SettingsModelString m_colSuffix = createColSuffixModel();

    private final SettingsModelBoolean m_keepOriginalCols = createKeepOriginalColsModel();

    SparkNumber2CategoryNodeModel() {
        super(new PortType[]{PMMLPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * @return the keep original columns model
     */
    static SettingsModelBoolean createKeepOriginalColsModel() {
        return new SettingsModelBoolean("keepOriginalColumns", false);
    }

    /**
     * @return the column name suffix model
     */
    static SettingsModelString createColSuffixModel() {
        SettingsModelString model = new SettingsModelString("columnSuffix", " (to category)");
        model.setEnabled(false);
        return model;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final PMMLPortObjectSpec pmml = (PMMLPortObjectSpec)inSpecs[0];
        final SparkDataPortObjectSpec rdd = (SparkDataPortObjectSpec)inSpecs[1];
        final DataTableSpec resultSpec = createResultSpec(rdd.getTableSpec(), pmml, m_colSuffix.getStringValue(),
            m_keepOriginalCols.getBooleanValue());
        return new PortObjectSpec[]{
            new SparkDataPortObjectSpec(rdd.getContextID(), resultSpec)};
    }

    private static DataTableSpec createResultSpec(final DataTableSpec tableSpec, final PMMLPortObjectSpec pmml,
        final String nameSuffix, final boolean keepOriginal) {
        final Set<String> preprocessingFields = new HashSet<>(pmml.getPreprocessingFields());
        final List<DataColumnSpec> resultCols = new LinkedList<>();
        final List<DataColumnSpec> appendedCols = new LinkedList<>();
        final DataColumnSpecCreator creator = new DataColumnSpecCreator("DUMMY", StringCell.TYPE);
        for (final DataColumnSpec colSpec : tableSpec) {
            final String colName = colSpec.getName();
            if (preprocessingFields.contains(colName)) {
                final String newName;
                if (keepOriginal) {
                    //keep the original column and add a new column with a new unique name
                    resultCols.add(colSpec);
                    newName = DataTableSpec.getUniqueColumnName(tableSpec, colName + nameSuffix);
                } else {
                    newName = colName;
                }
                creator.setName(newName);
                appendedCols.add(creator.createSpec());
            } else {
                resultCols.add(colSpec);
            }
        }
        Collections.addAll(resultCols, appendedCols.toArray(new DataColumnSpec[0]));
        return new DataTableSpec(resultCols.toArray(new DataColumnSpec[0]));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final PMMLPortObject pmml = (PMMLPortObject)inData[0];
        final SparkDataPortObject rdd = (SparkDataPortObject)inData[1];
        final ColumnBasedValueMapping map = ColumnBasedValueMappings.fromPMMLPortObject(pmml, rdd);
        final SparkDataTable resultTable;
        if (map.getColumnIndices().isEmpty()) {
            setWarningMessage("No mapping found return input RDD");
            setDeleteOnReset(false);
            resultTable = rdd.getData();
        } else {
            final String outputTableName = SparkIDs.createSparkDataObjectID();
          //TODO_TK
            final boolean keepOriginalColumns = m_keepOriginalCols.getBooleanValue();

            final Number2CategoryJobInput jobInput = new Number2CategoryJobInput(rdd.getTableName(),
                    map, keepOriginalColumns, m_colSuffix.getStringValue(), outputTableName);
            final SimpleJobRunFactory<Number2CategoryJobInput> runFactory = SparkContextUtil.getSimpleRunFactory(rdd.getContextID(), JOB_ID);
            runFactory.createRun(jobInput).run(rdd.getContextID(), exec);

            setDeleteOnReset(true);
            final DataTableSpec resultSpec = createResultSpec(rdd.getTableSpec(), pmml.getSpec(),
                m_colSuffix.getStringValue(), keepOriginalColumns);
            resultTable =
                new SparkDataTable(rdd.getContextID(), outputTableName, resultSpec);
        }
        return new PortObject[] {new SparkDataPortObject(resultTable)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_colSuffix.saveSettingsTo(settings);
        m_keepOriginalCols.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_colSuffix.validateSettings(settings);
        m_keepOriginalCols.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_colSuffix.loadSettingsFrom(settings);
        m_keepOriginalCols.loadSettingsFrom(settings);
    }

}
