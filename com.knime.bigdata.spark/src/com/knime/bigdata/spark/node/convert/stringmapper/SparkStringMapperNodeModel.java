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
package com.knime.bigdata.spark.node.convert.stringmapper;

import java.util.Map;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.StringValue;
import org.knime.core.data.container.CellFactory;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnFilter2;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;

import com.knime.bigdata.spark.jobserver.server.MappedRDDContainer;
import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.node.AbstractSparkNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.util.SparkIDGenerator;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class SparkStringMapperNodeModel extends AbstractSparkNodeModel {

    private static final DataType MAP_TYPE = DoubleCell.TYPE;

    private final SettingsModelString m_mappingType = createMappingTypeModel();

    private final SettingsModelColumnFilter2 m_cols = createColumnsModel();

    SparkStringMapperNodeModel() {
        super(new PortType[] {SparkDataPortObject.TYPE},
            new PortType[] {SparkDataPortObject.TYPE, SparkDataPortObject.TYPE});
    }

    /**
     * @return
     */
    @SuppressWarnings("unchecked")
    static SettingsModelColumnFilter2 createColumnsModel() {
        return new SettingsModelColumnFilter2("columns", StringValue.class);
    }

    static SettingsModelString createMappingTypeModel() {
        return new SettingsModelString("mappingType", MappingType.COLUMN.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input spec available");
        }
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec)inSpecs[0];
        final DataTableSpec spec = sparkSpec.getTableSpec();
//        final String colName = "TODO"; //m_col.getStringValue();
//        DataColumnSpec origColSpec = null;
//        if (colName == null) {
//            //preset it with the first nominal column
//            for (DataColumnSpec colSpec : spec) {
//                if (colSpec.getType().isCompatible(StringValue.class)) {
//                    //TODO m_col.setStringValue(colSpec.getName());
//                    setWarningMessage("Column name preset to " + colSpec.getName());
//                    origColSpec = colSpec;
//                    break;
//                }
//            }
//        } else {
//            origColSpec = spec.getColumnSpec(colName);
//        }
//        if (origColSpec == null) {
//            throw new InvalidSettingsException("Column " + colName + " not found in input spec");
//        }
        final DataTableSpec firstSpec = createAppendSpec(spec);
//        final DataTableSpec secondSpec = createMapSpec(spec, origColSpec);
        return new PortObjectSpec[] {new SparkDataPortObjectSpec(sparkSpec.getContext(), firstSpec),
//            new SparkDataPortObjectSpec(sparkSpec.getContext(), secondSpec)};
           //TODO - this should be the map with the mappings
            new SparkDataPortObjectSpec(sparkSpec.getContext(), firstSpec)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rdd = (SparkDataPortObject)inData[0];
        final DataTableSpec spec = rdd.getTableSpec();

        final DataTableSpec firstSpec = createAppendSpec(spec);

        exec.checkCanceled();
        final FilterResult result = m_cols.applyTo(firstSpec);
        final String[] includedCols = result.getIncludes();
        int[] includeColIdxs = new int[includedCols.length];

        for (int i = 0, length = includedCols.length; i < length; i++) {
            includeColIdxs[i] = firstSpec.findColumnIndex(includedCols[i]);
        }


//        final DataTableSpec secondSpec = createMapSpec(spec, spec.getColumnSpec(colIdx));
        final String outputTableName = SparkIDGenerator.createID();
        final String outputMappingTableName = SparkIDGenerator.createID();

        final ValueConverterTask task = new ValueConverterTask(rdd.getData(), includeColIdxs,includedCols,
            MappingType.valueOf(m_mappingType.getStringValue()), outputTableName, outputMappingTableName);
        //TODO - create port object from mapping
        final MappedRDDContainer mapping = task.execute(exec);

        //these are all the column names of the original (selected) and the mapped columns
        // (original columns that were not selected are not included, but the index of the new
        //  columns is still correct)
        Map<Integer, String> names = mapping.getColumnNames();

        // TODO - we have two output RDDs - the mapped data and the RDD with the mappings
        exec.setMessage("Nominal to Number mapping done.");

        return null; //new PortObject[]{new SparkDataPortObject(resultRDD)};
    }

    private DataTableSpec createMapSpec(final DataTableSpec spec, final DataColumnSpec origColSpec) {
        final DataColumnSpec[] specs = new DataColumnSpec[2];
        final DataColumnSpecCreator creator = new DataColumnSpecCreator(origColSpec);
        specs[0] = creator.createSpec();
        specs[1] = createMapColSpec(spec);
        return new DataTableSpec(specs);
    }

    /**
     * @param spec
     * @param creator
     * @return
     */
    private DataColumnSpec createMapColSpec(final DataTableSpec spec) {
        return new DataColumnSpecCreator(DataTableSpec.getUniqueColumnName(spec, "TODO" /*m_columnName.getStringValue()*/),
            MAP_TYPE).createSpec();
    }

    private DataTableSpec createAppendSpec(final DataTableSpec spec) {
        final ColumnRearranger rearranger = new ColumnRearranger(spec);
        rearranger.append(new CellFactory() {

            @Override
            public void setProgress(final int curRowNr, final int rowCount, final RowKey lastKey, final ExecutionMonitor exec) {
                //nothing to do
            }

            @Override
            public DataColumnSpec[] getColumnSpecs() {
                return new DataColumnSpec[] {createMapColSpec(spec)};
            }

            @Override
            public DataCell[] getCells(final DataRow row) {
                return null;
            }
        });
        return rearranger.createSpec();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_cols.saveSettingsTo(settings);
        m_mappingType.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_cols.validateSettings(settings);
        //TODO - need to validate that selected columns are non-numeric!
       // m_mappingType.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_cols.loadSettingsFrom(settings);
        //m_mappingType.loadSettingsFrom(settings);
    }
}
