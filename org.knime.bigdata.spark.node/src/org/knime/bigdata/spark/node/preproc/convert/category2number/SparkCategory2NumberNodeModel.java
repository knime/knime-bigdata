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
 *   Created on 06.07.2015 by koetter
 */
package org.knime.bigdata.spark.node.preproc.convert.category2number;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.dmg.pmml.DATATYPE;
import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.dmg.pmml.NormDiscreteDocument.NormDiscrete;
import org.dmg.pmml.OPTYPE;
import org.dmg.pmml.OPTYPE.Enum;
import org.dmg.pmml.TransformationDictionaryDocument.TransformationDictionary;
import org.knime.base.node.preproc.colconvert.categorytonumber.MapValuesConfiguration;
import org.knime.base.node.preproc.colconvert.categorytonumber.PMMLMapValuesTranslator;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.job.JobRunFactory;
import org.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.core.util.SparkUtil;
import org.knime.bigdata.spark.node.preproc.convert.MyRecord;
import org.knime.bigdata.spark.node.preproc.convert.NominalValueMapping;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.StringValue;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnFilter2;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.port.pmml.PMMLPortObjectSpecCreator;
import org.knime.core.node.port.pmml.preproc.DerivedFieldMapper;
import org.knime.core.node.port.pmml.preproc.PMMLPreprocTranslator;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;
import org.knime.core.util.Pair;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkCategory2NumberNodeModel extends SparkNodeModel {
    //TODO add support for column suffix and improve keep original columns option.
    //The problem with the keep original columns is the pmml generation.
    //The problem with the column suffix is that some functions check for the column name ending _num.
    private static final DataType MAP_TYPE = DoubleCell.TYPE;

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkCategory2NumberNodeModel.class.getCanonicalName();

    private final SettingsModelString m_mappingType = createMappingTypeModel();

    private final SettingsModelColumnFilter2 m_cols = createColumnsModel();

//    private final SettingsModelString m_colSuffix = createSuffixModel();
    private final SettingsModelBoolean m_keepOriginalCols = createKeepOriginalColsModel();

    /**
     * {@link DataTableSpec} of the mapping RDD.
     */
    public static final DataTableSpec MAP_SPEC = createMapSpec();

    SparkCategory2NumberNodeModel() {
        super(new PortType[] {SparkDataPortObject.TYPE},
            new PortType[] {SparkDataPortObject.TYPE, PMMLPortObject.TYPE});
    }

//    /**
//     * @return the column suffix model
//     */
//    static SettingsModelString createSuffixModel() {
//        return new SettingsModelString("columnSuffix", "_num");
//    }
//
    /**
     * @return the keep original columns model
     */
    static SettingsModelBoolean createKeepOriginalColsModel() {
        return new SettingsModelBoolean("keepOriginalColumns", false);
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
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
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
        final Integer[] includeColIdxs = SparkUtil.getColumnIndices(spec, includedCols);
        final MappingType mappingType = MappingType.valueOf(m_mappingType.getStringValue());
        final boolean keepOriginalColumns = m_keepOriginalCols.getBooleanValue();
//        final String suffix = m_colSuffix.getStringValue();
        final String suffix = NominalValueMapping.NUMERIC_COLUMN_NAME_POSTFIX;
        final DataTableSpec tableSpec =
                createResultSpec(spec, includeColIdxs, null, keepOriginalColumns, mappingType, suffix);
        final SparkDataPortObjectSpec resultSpec;
        if (tableSpec != null) {
            resultSpec =
                new SparkDataPortObjectSpec(sparkSpec.getContextID(), tableSpec);
        } else {
            resultSpec = null;
        }
        //PMML section
        final List<String> inputCols = new ArrayList<>();
        for (DataColumnSpec column : spec) {
            if (column.getType().isCompatible(StringValue.class)) {
                inputCols.add(column.getName());
            }
        }
        final PMMLPortObjectSpecCreator pmmlSpecCreator = new PMMLPortObjectSpecCreator(spec);
        pmmlSpecCreator.addPreprocColNames(inputCols);
        return new PortObjectSpec[] {resultSpec, pmmlSpecCreator.createSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rdd = (SparkDataPortObject)inData[0];
        final SparkContextID contextID = rdd.getContextID();
        final DataTableSpec inputTableSpec = rdd.getTableSpec();
        final MappingType mappingType = MappingType.valueOf(m_mappingType.getStringValue());
        final boolean keepOriginalColumns = m_keepOriginalCols.getBooleanValue();
//        final String suffix = m_colSuffix.getStringValue();
        final String suffix = NominalValueMapping.NUMERIC_COLUMN_NAME_POSTFIX;
        final FilterResult filterResult = m_cols.applyTo(inputTableSpec);
        final String[] includedCols = filterResult.getIncludes();
        final Integer[] includeColIdxs = SparkUtil.getColumnIndices(inputTableSpec, includedCols);
        final String resultTableName = SparkIDs.createSparkDataObjectID();
        final Category2NumberJobInput jobInput =  new Category2NumberJobInput(rdd.getData().getID(), includeColIdxs,
            includedCols, mappingType, keepOriginalColumns, suffix, resultTableName);

        final JobRunFactory<Category2NumberJobInput, Category2NumberJobOutput> runFactory =
                SparkContextUtil.getJobRunFactory(contextID, JOB_ID);
        final Category2NumberJobOutput jobOutput = runFactory.createRun(jobInput).run(contextID, exec);

        //we have two output RDDs - the mapped data and the RDD with the mappings
        exec.setMessage("Nominal to Number mapping done.");
        final String appendedColumns[] = jobOutput.getAppendedColumnsNames();
        final DataColumnSpec[] appendedColumnsSpecs = createMappingSpecs(inputTableSpec, appendedColumns);
        final DataTableSpec resultSpec =
                createResultSpec(inputTableSpec, includeColIdxs, appendedColumnsSpecs, keepOriginalColumns, mappingType, suffix);
        //we have to create a spec for pmml which contains all input columns AND the transformed columns
        final DataTableSpec pmmlSpec =
                createResultSpec(inputTableSpec, includeColIdxs, appendedColumnsSpecs, true, mappingType, suffix);
        // the optional PMML in port (can be null)
        exec.setMessage("Create PMML model");
        final PMMLPortObjectSpecCreator creator = new PMMLPortObjectSpecCreator(pmmlSpec);
        final PMMLPortObject outPMMLPort = new PMMLPortObject(creator.createSpec());
        final Collection<TransformationDictionary> dicts =
                getTransformations(inputTableSpec, jobOutput.getMappings(), keepOriginalColumns, suffix);
        for (final TransformationDictionary dict : dicts) {
            outPMMLPort.addGlobalTransformations(dict);
        }
        final SparkDataTable resultTable =
            new SparkDataTable(contextID, resultTableName, resultSpec);
        return new PortObject[]{new SparkDataPortObject(resultTable), outPMMLPort};
    }

    private static DataTableSpec createResultSpec(final DataTableSpec inputTableSpec, final Integer[] includeColIdxs,
        final DataColumnSpec[] mappingSpecs, final boolean keepOriginalColumns, final MappingType mappingType,
        final String suffix) {
        if (MappingType.BINARY.equals(mappingType)) {
            if (mappingSpecs == null) {
                return null;
            }
            if (keepOriginalColumns) {
                //simply append the new mapping specs
                return new DataTableSpec(inputTableSpec, new DataTableSpec(mappingSpecs));
            }
            final ColumnRearranger rearranger = new ColumnRearranger(inputTableSpec);
            rearranger.remove(SparkUtil.convert(includeColIdxs));
            return new DataTableSpec(rearranger.createSpec(), new DataTableSpec(mappingSpecs));
        }
        final Set<Integer> col2filter = new HashSet<>(Arrays.asList(includeColIdxs));
        final List<DataColumnSpec> resultCols = new LinkedList<>();
        final List<DataColumnSpec> appendCols = new LinkedList<>();
        DataColumnSpecCreator creator = new DataColumnSpecCreator("Dummy", DoubleCell.TYPE);
        for (int i = 0; i < inputTableSpec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = inputTableSpec.getColumnSpec(i);
            if (col2filter.contains(Integer.valueOf(i))) {
                final String origColName = colSpec.getName();
                final String colName;
                if (keepOriginalColumns) {
                    resultCols.add(colSpec);
                    colName = DataTableSpec.getUniqueColumnName(inputTableSpec, origColName + suffix);
                } else {
                    colName = DataTableSpec.getUniqueColumnName(inputTableSpec, origColName + suffix);
                    //We have to use a different column name otherwise we get problems with the pmml generation
//                    colName = origColName;
                }
                creator.setName(colName);
                appendCols.add(creator.createSpec());
            } else {
                resultCols.add(colSpec);
            }
        }
        Collections.addAll(resultCols, appendCols.toArray(new DataColumnSpec[0]));
        return new DataTableSpec(resultCols.toArray(new DataColumnSpec[0]));
    }

    private Collection<TransformationDictionary> getTransformations(final DataTableSpec inputTableSpec,
        final NominalValueMapping mappings, final boolean keepOriginalCols, final String suffix) {
        final Collection<TransformationDictionary> dicts = new LinkedList<>();
        final Iterator<MyRecord> records = mappings.iterator();
        if (MappingType.BINARY.equals(mappings.getType())) {
            final Map<String, List<Pair<String, String>>> columnMapping = new LinkedHashMap<>();
            while (records.hasNext()) {
                MyRecord record = records.next();
                int colIdx = record.m_nominalColumnIndex;
                String origVal = record.m_nominalValue;
                final String colName= inputTableSpec.getColumnSpec(colIdx).getName();
                List<Pair<String, String>> valMap = columnMapping.get(colName);
                if (valMap == null) {
                    valMap = new LinkedList<>();
                    columnMapping.put(colName, valMap);
                }
                //this is the new column name of the mapped value
                final String newColumnName = colName + "_" + origVal;
                valMap.add(new Pair<>(newColumnName, origVal));
            }
            final DerivedFieldMapper mapper = new DerivedFieldMapper((PMMLPortObject)null);
            final List<DerivedField> derivedFields = new ArrayList<>();
            for (Map.Entry<String, List<Pair<String, String>>> entry : columnMapping.entrySet()) {
                final String columnName = entry.getKey();
                final String derivedName = mapper.getDerivedFieldName(columnName);
                for (Pair<String, String> nameValue : entry.getValue()) {
                    final DerivedField derivedField = DerivedField.Factory.newInstance();
                    derivedField.setName(nameValue.getFirst());
                    derivedField.setOptype(OPTYPE.ORDINAL);
                    derivedField.setDataType(DATATYPE.DOUBLE);
                    final NormDiscrete normDiscrete = derivedField.addNewNormDiscrete();
                    normDiscrete.setField(derivedName);
                    normDiscrete.setValue(nameValue.getSecond());
                    normDiscrete.setMapMissingTo(0);
                    derivedFields.add(derivedField);
                }
            }
            final TransformationDictionary dictionary = TransformationDictionary.Factory.newInstance();
            dictionary.setDerivedFieldArray(derivedFields.toArray(new DerivedField[0]));
            dicts.add(dictionary);
        } else {
            final Map<String, Map<DataCell, DoubleCell>> colValMap = new LinkedHashMap<>();
            while (records.hasNext()) {
                MyRecord record = records.next();
                int colIdx = record.m_nominalColumnIndex;
                String origVal = record.m_nominalValue;
                int mappedVal = record.m_numberValue;
                final String colName= inputTableSpec.getColumnSpec(colIdx).getName();
                Map<DataCell, DoubleCell> valMap = colValMap.get(colName);
                if (valMap == null) {
                    valMap = new LinkedHashMap<>();
                    colValMap.put(colName, valMap);
                }
                valMap.put(new StringCell(origVal), new DoubleCell(mappedVal));
            }
            for (Entry<String, Map<DataCell, DoubleCell>> col : colValMap.entrySet()) {
                final String origColName = col.getKey();
                final String mapColName;
//                if (keepOriginalCols) {
                    mapColName = DataTableSpec.getUniqueColumnName(inputTableSpec, origColName + suffix);
//                } else {
//                    mapColName = origColName;
//                }
                final MapValuesConfiguration config =
                        new MapValuesConfiguration(origColName, mapColName, col.getValue()) {
                    /**{@inheritDoc}*/
                    @Override
                    public String getSummary() {
                        return "Generated by KNIME - Spark Category to Number node";
                    }

                    /**{@inheritDoc}*/
                    @Override
                    public DATATYPE.Enum getOutDataType() {
                        return DATATYPE.DOUBLE;
                    }

                    /**{@inheritDoc}*/
                    @Override
                    public Enum getOpType() {
                        return OPTYPE.CONTINUOUS;
                    }
                };
                final PMMLPreprocTranslator trans =
                        new PMMLMapValuesTranslator(config, new DerivedFieldMapper((PMMLPortObject)null));
                dicts.add(trans.exportToTransDict());
            }
        }
        return dicts;
    }

    /**
     * @param inputTableSpec input table spec
     * @param appendedColumns column names from the MappedRDDContainer
     * @return the appended mapped value columns
     */
    public static DataColumnSpec[] createMappingSpecs(final DataTableSpec inputTableSpec, final String appendedColumns[]) {
        final DataColumnSpec[] specList = new DataColumnSpec[appendedColumns.length];
        final DataColumnSpecCreator specCreator = new DataColumnSpecCreator("Dummy", MAP_TYPE);
        for (int i = 0; i < appendedColumns.length; i++) {
            String colName = appendedColumns[i];
            specCreator.setName(DataTableSpec.getUniqueColumnName(inputTableSpec, colName));
            specList[i] = specCreator.createSpec();
        }
        return specList;
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
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_cols.saveSettingsTo(settings);
        m_mappingType.saveSettingsTo(settings);
        m_keepOriginalCols.saveSettingsTo(settings);
//        m_colSuffix.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_cols.validateSettings(settings);
        final String mappingType =
                ((SettingsModelString)m_mappingType.createCloneWithValidatedValue(settings)).getStringValue();
        if (MappingType.valueOf(mappingType) == null) {
            throw new InvalidSettingsException("Invalid mapping type: " + mappingType);
        }
        m_keepOriginalCols.validateSettings(settings);
//        m_colSuffix.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_cols.loadSettingsFrom(settings);
        m_mappingType.loadSettingsFrom(settings);
        m_keepOriginalCols.loadSettingsFrom(settings);
//        m_colSuffix.loadSettingsFrom(settings);
    }
}
