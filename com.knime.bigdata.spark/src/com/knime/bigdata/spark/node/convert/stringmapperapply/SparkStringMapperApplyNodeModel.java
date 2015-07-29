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
 *   Created on 06.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.convert.stringmapperapply;

import java.util.Map;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.StringValue;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnFilter2;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;

import com.knime.bigdata.spark.jobserver.server.MappedRDDContainer;
import com.knime.bigdata.spark.node.AbstractSparkNodeModel;
import com.knime.bigdata.spark.node.convert.stringmapper.SparkStringMapperNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDs;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkStringMapperApplyNodeModel extends AbstractSparkNodeModel {

    private final SettingsModelColumnFilter2 m_cols = createColumnsModel();

    SparkStringMapperApplyNodeModel() {
        super(new PortType[] {SparkDataPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[] {SparkDataPortObject.TYPE});
    }

    /**
     * @return
     */
    @SuppressWarnings("unchecked")
    static SettingsModelColumnFilter2 createColumnsModel() {
        return new SettingsModelColumnFilter2("columns", StringValue.class);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("No input spec available");
        }
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec)inSpecs[0];
        final DataTableSpec spec = sparkSpec.getTableSpec();
        FilterResult filterResult = m_cols.applyTo(spec);
        final String[] includedCols = filterResult.getIncludes();
        if (includedCols == null || includedCols.length < 1) {
            throw new InvalidSettingsException("No nominal columns selected");
        }
        final SparkDataPortObjectSpec mappingSparkSpec = (SparkDataPortObjectSpec)inSpecs[1];
        if (!SparkStringMapperNodeModel.MAP_SPEC.equals(mappingSparkSpec.getTableSpec())) {
            throw new InvalidSettingsException("Invalid mapping RDD found.");
        }
        return new PortObjectSpec[] {null};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rdd = (SparkDataPortObject)inData[0];
        final SparkDataPortObject mappingRdd = (SparkDataPortObject)inData[1];
        final DataTableSpec spec = rdd.getTableSpec();
        exec.checkCanceled();
        final FilterResult result = m_cols.applyTo(spec);
        final String[] includedCols = result.getIncludes();
        int[] includeColIdxs = new int[includedCols.length];

        for (int i = 0, length = includedCols.length; i < length; i++) {
            includeColIdxs[i] = spec.findColumnIndex(includedCols[i]);
        }

        final String outputTableName = SparkIDs.createRDDID();
        final SparkStringMapperApplyTask task = new SparkStringMapperApplyTask(rdd.getData(),
            mappingRdd.getTableName(), includeColIdxs, includedCols, outputTableName);

        //create port object from mapping
        final MappedRDDContainer mapping = task.execute(exec);
        //TODO: Use the MappedRDDContainer to also ouput a PMML of the mapping

        //these are all the column names of the original (selected) and the mapped columns
        // (original columns that were not selected are not included, but the index of the new
        //  columns is still correct)
        final Map<Integer, String> names = mapping.getColumnNames();

        //we have two output RDDs - the mapped data and the RDD with the mappings
        exec.setMessage("Nominal to Number mapping done.");
        final DataColumnSpec[] mappingSpecs = SparkStringMapperNodeModel.createMappingSpecs(spec, names);
        final DataTableSpec firstSpec = new DataTableSpec(spec, new DataTableSpec(mappingSpecs));
        final SparkDataTable firstRDD = new SparkDataTable(rdd.getContext(), outputTableName, firstSpec);
        return new PortObject[]{new SparkDataPortObject(firstRDD)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_cols.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_cols.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_cols.loadSettingsFrom(settings);
    }
}
