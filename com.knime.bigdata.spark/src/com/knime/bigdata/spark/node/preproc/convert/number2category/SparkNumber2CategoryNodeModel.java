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
 *   Created on 21.08.2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.convert.number2category;

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
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.port.pmml.PMMLPortObjectSpec;

import com.knime.bigdata.spark.jobserver.server.ColumnBasedValueMapping;
import com.knime.bigdata.spark.node.AbstractSparkNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.util.SparkIDs;
import com.knime.bigdata.spark.util.SparkUtil;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkNumber2CategoryNodeModel extends AbstractSparkNodeModel {

    private final SettingsModelString m_colSuffix = createColSuffixModel();

    SparkNumber2CategoryNodeModel() {
        super(new PortType[]{PMMLPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkModelPortObject.TYPE});
    }

    /**
     * @return the column name suffix model
     */
    static SettingsModelString createColSuffixModel() {
        return new SettingsModelString("columnSuffix", "_nom");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final PMMLPortObjectSpec pmml = (PMMLPortObjectSpec)inSpecs[0];
        final SparkDataPortObjectSpec rdd = (SparkDataPortObjectSpec)inSpecs[1];
        final DataTableSpec resultSpec = createResultSpec(rdd.getTableSpec(), pmml, m_colSuffix.getStringValue());
        return new PortObjectSpec[] {new SparkDataPortObjectSpec(rdd.getContext(), resultSpec)};
    }

    private static DataTableSpec createResultSpec(final DataTableSpec tableSpec, final PMMLPortObjectSpec pmml,
        final String nameSuffix) {
        final Set<String> preprocessingFields = new HashSet<>(pmml.getPreprocessingFields());
        List<DataColumnSpec> newCols = new LinkedList<DataColumnSpec>();
        DataColumnSpecCreator creator = new DataColumnSpecCreator("DUMMY", StringCell.TYPE);
        for (final DataColumnSpec colSpec : tableSpec) {
            final String colName = colSpec.getName();
            if (preprocessingFields.contains(colName)) {
                final String newName = colName + nameSuffix;
                creator.setName(DataTableSpec.getUniqueColumnName(tableSpec, newName));
                newCols.add(creator.createSpec());
            }
        }
        return new DataTableSpec(tableSpec, new DataTableSpec(newCols.toArray(new DataColumnSpec[0])));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final PMMLPortObject pmml = (PMMLPortObject)inData[0];
        final SparkDataPortObject rdd = (SparkDataPortObject)inData[1];
        final ColumnBasedValueMapping map = SparkUtil.getCategory2NumberMap(pmml, rdd);
        final String outputTableName = SparkIDs.createRDDID();
        final Number2CategoryConverterTask task = new Number2CategoryConverterTask(rdd.getData(), map, outputTableName);
        exec.checkCanceled();
        task.execute(exec);
        final SparkDataTable resultTable = new SparkDataTable(rdd.getContext(), outputTableName,
            createResultSpec(rdd.getTableSpec(), pmml.getSpec(), outputTableName));
        return new PortObject[] {new SparkDataPortObject(resultTable)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_colSuffix.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_colSuffix.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_colSuffix.loadSettingsFrom(settings);
    }

}
