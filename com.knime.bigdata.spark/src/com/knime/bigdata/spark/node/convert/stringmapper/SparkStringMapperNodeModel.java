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

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.StringValue;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
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
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDs;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkStringMapperNodeModel extends AbstractSparkNodeModel {

    private static final DataType MAP_TYPE = DoubleCell.TYPE;

    private final SettingsModelString m_mappingType = createMappingTypeModel();

    private final SettingsModelColumnFilter2 m_cols = createColumnsModel();

    /**
     * {@link DataTableSpec} of the mapping RDD.
     */
    public static final DataTableSpec MAP_SPEC = createMapSpec();

    SparkStringMapperNodeModel() {
        //TODO: Also implement the PMML mapping
        super(new PortType[] {SparkDataPortObject.TYPE},
//                , new PortType(PMMLPortObject.class, true)},
            new PortType[] {SparkDataPortObject.TYPE, SparkDataPortObject.TYPE});
//        , PMMLPortObject.TYPE});
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
        FilterResult filterResult = m_cols.applyTo(spec);
        final String[] includedCols = filterResult.getIncludes();
        if (includedCols == null || includedCols.length < 1) {
            throw new InvalidSettingsException("No nominal columns selected");
        }
        return new PortObjectSpec[] {null, new SparkDataPortObjectSpec(sparkSpec.getContext(), MAP_SPEC)};
//        //PMML section
//        final PMMLPortObjectSpec pmmlSpec = (PMMLPortObjectSpec)inSpecs[2];
//        final PMMLPortObjectSpecCreator pmmlSpecCreator = new PMMLPortObjectSpecCreator(pmmlSpec, spec);
//        pmmlSpecCreator.addPreprocColNames(Arrays.asList(includedCols));
//        return new PortObjectSpec[] {null, new SparkDataPortObjectSpec(sparkSpec.getContext(), MAP_SPEC),
//            pmmlSpecCreator.createSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rdd = (SparkDataPortObject)inData[0];
        final KNIMESparkContext context = rdd.getContext();
        final DataTableSpec spec = rdd.getTableSpec();
        final MappingType mappingType = MappingType.valueOf(m_mappingType.getStringValue());

        exec.checkCanceled();
        final FilterResult result = m_cols.applyTo(spec);
        final String[] includedCols = result.getIncludes();
        int[] includeColIdxs = new int[includedCols.length];

        for (int i = 0, length = includedCols.length; i < length; i++) {
            includeColIdxs[i] = spec.findColumnIndex(includedCols[i]);
        }

        final String outputTableName = SparkIDs.createRDDID();
        final String outputMappingTableName = SparkIDs.createRDDID();

        final ValueConverterTask task = new ValueConverterTask(rdd.getData(), includeColIdxs, includedCols,
            mappingType, outputTableName, outputMappingTableName);
        //create port object from mapping
        final MappedRDDContainer mapping = task.execute(exec);
        //TODO: Use the MappedRDDContainer to also ouput a PMML of the mapping

        //these are all the column names of the original (selected) and the mapped columns
        // (original columns that were not selected are not included, but the index of the new
        //  columns is still correct)
        final Map<Integer, String> names = mapping.getColumnNames();

        //we have two output RDDs - the mapped data and the RDD with the mappings
        exec.setMessage("Nominal to Number mapping done.");
        final DataColumnSpec[] mappingSpecs = createMappingSpecs(spec, names);
        final DataTableSpec firstSpec = new DataTableSpec(spec, new DataTableSpec(mappingSpecs));
        SparkDataTable firstRDD = new SparkDataTable(context, outputTableName, firstSpec);
        SparkDataTable secondRDD = new SparkDataTable(context, outputMappingTableName, MAP_SPEC);

     // the optional PMML in port (can be null)
//        final PMMLPortObject inPMMLPort = (PMMLPortObject)inData[2];
//        final PMMLPortObjectSpecCreator creator = new PMMLPortObjectSpecCreator(inPMMLPort, firstSpec);
//
//        final PMMLPortObject outPMMLPort = new PMMLPortObject(creator.createSpec(), inPMMLPort);

//        int colIdx = spec.getNumColumns();
//        for (DataColumnSpec col : mappingSpecs) {
//            PMMLMapValuesTranslator trans = new PMMLMapValuesTranslator(
//                    factory.getConfig(), new DerivedFieldMapper(inPMMLPort));
//            outPMMLPort.addGlobalTransformations(trans.exportToTransDict());
//        }

        return new PortObject[]{new SparkDataPortObject(firstRDD), new SparkDataPortObject(secondRDD)};
    }

    /**
     * @param spec input table spec
     * @param names mapping column names from the MappedRDDContainer
     * @return the appended mapped value columns
     */
    public static DataColumnSpec[] createMappingSpecs(final DataTableSpec spec, final Map<Integer, String> names) {
        System.out.println(names);
        final DataColumnSpec[] specList = new DataColumnSpec[names.size()];
        final DataColumnSpecCreator specCreator = new DataColumnSpecCreator("Dummy", MAP_TYPE);
        int count = 0;
        for (final Entry<Integer, String> entry : names.entrySet()) {
            final String colName = entry.getValue();
            if (!spec.containsName(colName)) {
                final int mapColIdx = entry.getKey().intValue() - spec.getNumColumns();
                specCreator.setName(DataTableSpec.getUniqueColumnName(spec, colName));
                specList[mapColIdx] = specCreator.createSpec();
                count++;
            }
        }
        final DataColumnSpec[] specs = Arrays.copyOfRange(specList, 0, count);
        return specs;
    }

    private static DataTableSpec createMapSpec() {
        final DataColumnSpec[] specs = new DataColumnSpec[4];
        final DataColumnSpecCreator creator = new DataColumnSpecCreator("Column name", StringCell.TYPE);
        specs[0] = creator.createSpec();
        creator.setName("Column index");
        creator.setType(IntCell.TYPE);
        specs[1] = creator.createSpec();
        creator.setName("Column value");
        creator.setType(StringCell.TYPE);
        specs[2] = creator.createSpec();
        creator.setName("Mapping value");
        creator.setType(IntCell.TYPE);
        specs[3] = creator.createSpec();
        return new DataTableSpec(specs);
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
        final String mappingType =
                ((SettingsModelString)m_mappingType.createCloneWithValidatedValue(settings)).getStringValue();
        if (MappingType.valueOf(mappingType) == null) {
            throw new InvalidSettingsException("Invalid mapping type: " + mappingType);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_cols.loadSettingsFrom(settings);
        m_mappingType.loadSettingsFrom(settings);
    }
}
