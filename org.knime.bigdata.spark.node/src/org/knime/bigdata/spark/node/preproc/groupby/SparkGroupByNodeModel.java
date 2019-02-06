/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * -------------------------------------------------------------------
 *
 */
package org.knime.bigdata.spark.node.preproc.groupby;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.node.preproc.groupby.ColumnNamePolicy;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import org.knime.bigdata.spark.core.sql_function.SparkSQLFunctionJobInput;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.preproc.groupby.dialog.PivotSettings;
import org.knime.bigdata.spark.node.preproc.groupby.dialog.column.ColumnAggregationFunctionRow;
import org.knime.bigdata.spark.node.sql_function.SparkSQLAggregationFunction;
import org.knime.bigdata.spark.node.sql_function.SparkSQLFunctionCombinationProvider;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Node model of the Spark GroupBy and Spark Pivot nodes.
 *
 * @author Tobias Koetter, KNIME GmbH
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkGroupByNodeModel extends SparkNodeModel {
    /** The unique Spark job id. */
    public static final String JOB_ID = SparkGroupByNodeModel.class.getCanonicalName();

    /**
     * Enum indicating whether type based matching should match strictly columns of the same type or also columns
     * containing sub types.
     *
     * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
     */
    enum TypeMatch {

            /** Strict type matching. */
            STRICT("Strict", true, (byte)0),

            /** Sub type matching. */
            SUB_TYPE("Include sub-types", false, (byte)1);

        /** Configuration key for the exact type match flag. */
        private static final String CFG_TYPE_MATCH = "typeMatch";

        /** The options UI name. */
        private final String m_name;

        /** The strict type matching flag. */
        private final boolean m_strictTypeMatch;

        /** The byte used to store the option. */
        private final byte m_persistByte;

        TypeMatch(final String name, final boolean strictTypeMatch, final byte persistByte) {
            m_name = name;
            m_strictTypeMatch = strictTypeMatch;
            m_persistByte = persistByte;
        }

        boolean useStrictTypeMatching() {
            return m_strictTypeMatch;
        }

        @Override
        public String toString() {
            return m_name;
        }

        /**
         * Save the selected strategy.
         *
         * @param settings the settings to save to
         */
        void saveSettingsTo(final NodeSettingsWO settings) {
            settings.addByte(CFG_TYPE_MATCH, m_persistByte);
        }

        /**
         * Loads a strategy.
         *
         * @param settings the settings to load the strategy from
         * @return the loaded strategy
         * @throws InvalidSettingsException if the selection strategy couldn't be loaded
         */
        static TypeMatch loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
            if (settings.containsKey(CFG_TYPE_MATCH)) {
                final byte persistByte = settings.getByte(CFG_TYPE_MATCH);
                for (final TypeMatch strategy : values()) {
                    if (persistByte == strategy.m_persistByte) {
                        return strategy;
                    }
                }
                throw new InvalidSettingsException("The selection type matching strategy could not be loaded.");

            } else {
                return TypeMatch.SUB_TYPE;
            }
        }
    }

    /**
     * Config key for the add count star option.
     */
    static final String CFG_ADD_COUNT_STAR = "addCountStar";

    /**
     * Config key for the columns that will be grouped.
     */
    static final String CFG_GROUP_BY_COLUMNS = "groupByColumns";

    /**
     * Config key for the name policy of aggregated columns.
     */
    static final String CFG_COLUMN_NAME_POLICY = "columnNamePolicy";

    static final PortType GROUP_BY_INPUT_PORTS[] = new PortType[] { SparkDataPortObject.TYPE };

    static final PortType PIVOT_INPUT_PORTS[] = new PortType[] { SparkDataPortObject.TYPE, BufferedDataTable.TYPE_OPTIONAL };

//    private final WindowFunctionSettings m_windowSettings = new WindowFunctionSettings();

    private final SettingsModelBoolean m_addCountStar = new SettingsModelBoolean(CFG_ADD_COUNT_STAR, false);

    private final SettingsModelString m_countStarColName = createCountStarColNameModel();

    private final SettingsModelFilterString m_groupByCols = new SettingsModelFilterString(CFG_GROUP_BY_COLUMNS);

    private final SettingsModelString m_columnNamePolicy =
        new SettingsModelString(CFG_COLUMN_NAME_POLICY, ColumnNamePolicy.getDefault().getLabel());

    /** Enum indicating whether type based aggregation should use strict or sub-type matching. */
    private TypeMatch m_typeMatch = TypeMatch.STRICT;

    private final AggregationFunctionSettings m_aggregationFunctionSettings = new AggregationFunctionSettings();

    private List<ColumnAggregationFunctionRow> m_aggregationFunction2Use = null;

    /** Indicates that this node is the pivot node */
    private final boolean m_pivotNodeMode;

    private final PivotSettings m_pivotSettings = new PivotSettings();

    /**
     * Creates a new database group by.
     *
     * @param pivotNodeMode Whether to do pivoting or just grouping.
     */
    public SparkGroupByNodeModel(final boolean pivotNodeMode) {
        super(pivotNodeMode ? PIVOT_INPUT_PORTS : GROUP_BY_INPUT_PORTS,
              new PortType[]{ SparkDataPortObject.TYPE });
        m_pivotNodeMode = pivotNodeMode;
    }

    /**
     * @return the count star result column name
     */
    static SettingsModelString createCountStarColNameModel() {
        final SettingsModelString model = new SettingsModelString("countStarColName", "COUNT(*)");
        model.setEnabled(false);
        return model;
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 0 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Spark RDD available");
        }

        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[0];
        final DataTableSpec tableSpec = sparkSpec.getTableSpec();
        final SparkVersion sparkVersion = SparkContextUtil.getSparkVersion(sparkSpec.getContextID());

        if (SparkVersion.V_2_0.compareTo(sparkVersion) > 0) {
            throw new InvalidSettingsException("Unsupported Spark version. This node requires at least Spark 2.0.");
        }

        final SparkSQLFunctionCombinationProvider functionProvider = new SparkSQLFunctionCombinationProvider(sparkVersion);
        final ArrayList<ColumnAggregationFunctionRow> invalidColAggrs = new ArrayList<>(1);

        m_aggregationFunction2Use = m_aggregationFunctionSettings.getAggregationFunctions(tableSpec, functionProvider,
            m_groupByCols.getIncludeList(), invalidColAggrs, m_typeMatch.useStrictTypeMatching());

        if (m_addCountStar.getBooleanValue() && !functionProvider.hasCountFunction()) {
            setWarningMessage("No Spark count(*) function provider exists.");
        }

//        if (m_windowSettings.isEnabled() && !functionProvider.hasWindowFunction()) {
//            setWarningMessage("No Spark window function provider exists.");
//        }

        if (m_pivotNodeMode && StringUtils.isBlank(m_pivotSettings.getColumn())) {
            throw new InvalidSettingsException("No pivot column selected.");
        }

        if (m_pivotNodeMode && tableSpec.findColumnIndex(m_pivotSettings.getColumn()) == -1) {
            throw new InvalidSettingsException("Can't find pivot column '" + m_pivotSettings.getColumn() + "' in input table.");
        }

        if (m_pivotNodeMode && m_pivotSettings.isManualValuesMode() && m_pivotSettings.getValues().length == 0) {
            throw new InvalidSettingsException("No pivot values were specified.");
        }

        if (!invalidColAggrs.isEmpty()) {
            setWarningMessage(invalidColAggrs.size() + " aggregation functions ignored due to incompatible columns.");
        }

        final IntermediateSpec inputSpec = SparkDataTableUtil.toIntermediateSpec(tableSpec);
        final List<SparkSQLFunctionJobInput> aggFunctions = createAggJobInput(tableSpec, functionProvider);
        if (aggFunctions == null || aggFunctions.isEmpty()) {
            throw new InvalidSettingsException("No aggregation function defined");
        }

        if (m_pivotNodeMode && inSpecs.length == 2 && inSpecs[1] != null) {
            if (StringUtils.isBlank(m_pivotSettings.getInputValuesColumn())) {
                m_pivotSettings.guessInputValuesColumn(new DataTableSpec[] { tableSpec, (DataTableSpec)inSpecs[1] });
            }

            if (StringUtils.isBlank(m_pivotSettings.getInputValuesColumn())) {
                throw new InvalidSettingsException("No pivot values column selected.");
            }

            if (!((DataTableSpec) inSpecs[1]).containsName(m_pivotSettings.getInputValuesColumn())) {
                throw new InvalidSettingsException("Can't find pivot values column '" + m_pivotSettings.getInputValuesColumn() + "' in input table.");
            }

            return new PortObjectSpec[] { null };

        } else if (m_pivotNodeMode && m_pivotSettings.isAutoValuesMode()) {
            return new PortObjectSpec[] { null }; // wait for result spec from spark

        } else if (m_pivotNodeMode && m_pivotSettings.isManualValuesMode()) {
            final DataTableSpec outputSpec = createPivotOutputSpec(sparkVersion, inputSpec, null,
                createGroupByJobInput(tableSpec, functionProvider), aggFunctions, functionProvider);
            return new PortObjectSpec[] { new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec) };

        } else {
            final DataTableSpec outputSpec = createGroupByOutputSpec(sparkVersion, inputSpec,
                createGroupByJobInput(tableSpec, functionProvider), aggFunctions, functionProvider);
            return new PortObjectSpec[] { new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec) };
        }
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final DataTableSpec tableSpec = sparkPort.getTableSpec();
        final IntermediateSpec inputSpec = SparkDataTableUtil.toIntermediateSpec(tableSpec);
        final SparkVersion sparkVersion = SparkContextUtil.getSparkVersion(sparkPort.getContextID());
        final SparkSQLFunctionCombinationProvider functionProvider = new SparkSQLFunctionCombinationProvider(sparkVersion);
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();
        final List<SparkSQLFunctionJobInput> groupByFunctions = createGroupByJobInput(tableSpec, functionProvider);
        final List<SparkSQLFunctionJobInput> aggFunctions = createAggJobInput(tableSpec, functionProvider);
        final SparkGroupByJobInput jobInput = new SparkGroupByJobInput(inputObject, outputObject,
            groupByFunctions.toArray(new SparkSQLFunctionJobInput[0]),
            aggFunctions.toArray(new SparkSQLFunctionJobInput[0]));
        if (m_pivotNodeMode) {
            if (inData.length == 2 && inData[1] != null) {
                boolean allUsed =
                    m_pivotSettings.addJobConfig(jobInput, (BufferedDataTable) inData[1]);
                if (!allUsed) {
                    setWarningMessage(
                        "One or more values in the pivot column were ignored, because the configured limit was reached.\n"
                        + "Increase the limit in the node configuration if required.");
                }
            } else {
                m_pivotSettings.addJobConfig(jobInput);
            }
        }

        exec.setMessage("Executing spark job...");
        final SparkGroupByJobOutput jobOutput = SparkContextUtil
                .<SparkGroupByJobInput, SparkGroupByJobOutput>getJobRunFactory(contextID, JOB_ID)
                .createRun(jobInput).run(contextID, exec);
        final DataTableSpec outputSpec;

        if (m_pivotNodeMode && inData.length == 2 && inData[1] != null) {
            outputSpec = createPivotOutputSpec(sparkVersion, inputSpec, (BufferedDataTable)inData[1], groupByFunctions,
                aggFunctions, functionProvider);

        } else if (m_pivotNodeMode && m_pivotSettings.isAutoValuesMode()) {
            if (jobOutput.getPivotValuesDropped()) {
                setWarningMessage(
                    "One or more values in the pivot column were ignored, because the configured limit was reached.\n"
                    + "Increase the limit in the node configuration if required.");
            }
            outputSpec = KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(outputObject));

        } else if (m_pivotNodeMode && m_pivotSettings.isManualValuesMode()) {
            outputSpec =
                createPivotOutputSpec(sparkVersion, inputSpec, null, groupByFunctions, aggFunctions, functionProvider);

        } else {
            outputSpec =
                createGroupByOutputSpec(sparkVersion, inputSpec, groupByFunctions, aggFunctions, functionProvider);
        }

        final SparkDataTable resultTable = new SparkDataTable(contextID, outputObject, outputSpec);
        return new PortObject[] { new SparkDataPortObject(resultTable) };
    }

    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        // initialize the node settings
        m_addCountStar.saveSettingsTo(settings);
        m_countStarColName.saveSettingsTo(settings);
        m_groupByCols.saveSettingsTo(settings);
//        m_windowSettings.saveSettingsTo(settings);
        m_columnNamePolicy.saveSettingsTo(settings);
        m_aggregationFunctionSettings.saveSettingsTo(settings);

        if (m_pivotNodeMode) {
            m_pivotSettings.saveSettingsTo(settings);
        }
        m_typeMatch.saveSettingsTo(settings);
    }

    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_groupByCols.loadSettingsFrom(settings);
//        m_windowSettings.loadSettingsFrom(settings);
        m_addCountStar.loadSettingsFrom(settings);
        m_countStarColName.loadSettingsFrom(settings);
        m_columnNamePolicy.loadSettingsFrom(settings);
        m_aggregationFunctionSettings.loadSettingsFrom(settings);

        if (m_pivotNodeMode) {
            m_pivotSettings.loadSettingsFrom(settings);
        }
        // AP-7020: the default value false ensures backwards compatibility (KNIME 3.8)
        m_typeMatch = TypeMatch.loadSettingsFrom(settings);
    }

    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_groupByCols.validateSettings(settings);
//        m_windowSettings.validateSettings(settings);

        final boolean addCountStar =
                ((SettingsModelBoolean)m_addCountStar.createCloneWithValidatedValue(settings)).getBooleanValue();
        final String colName =
                ((SettingsModelString)m_countStarColName.createCloneWithValidatedValue(settings)).getStringValue();

        if (addCountStar && (colName == null || colName.isEmpty())) {
            throw new IllegalArgumentException("Please specify the count(*) column name");
        }

        if (ColumnNamePolicy.getPolicy4Label(((SettingsModelString)m_columnNamePolicy.createCloneWithValidatedValue(
            settings)).getStringValue()) == null) {
            throw new InvalidSettingsException("Invalid column name policy");
        }

        m_aggregationFunctionSettings.validateSettings(settings);

        if (m_pivotNodeMode) {
            m_pivotSettings.validateSettings(settings);
        }
    }

    /** @return output spec of the Spark GroupBy job */
    private DataTableSpec createGroupByOutputSpec(final SparkVersion sparkVersion, final IntermediateSpec inputSpec,
        final List<SparkSQLFunctionJobInput> groupByFunctions, final List<SparkSQLFunctionJobInput> aggFunctions,
        final SparkSQLFunctionCombinationProvider functionProvider) throws InvalidSettingsException {

        final List<SparkSQLFunctionJobInput> allFunctions = new ArrayList<>();
        allFunctions.addAll(groupByFunctions);
        allFunctions.addAll(aggFunctions);
        final SparkSQLFunctionJobInput functionInput[] = allFunctions.toArray(new SparkSQLFunctionJobInput[0]);
        final ArrayList<IntermediateField> outputFields = new ArrayList<>(functionInput.length);
        for (int i = 0; i < functionInput.length; i++) {
            final SparkSQLFunctionJobInput funcInput = functionInput[i];
            outputFields.add(functionProvider
                    .getFunctionResultField(sparkVersion, inputSpec, funcInput));
        }

        // check uniqueness of output column names
        final HashMap<String, Integer> outputColumns = new HashMap<>();
        for (int i = 0; i < outputFields.size(); i++) {
            final String colName = outputFields.get(i).getName();

            if (outputColumns.containsKey(colName)) {
                throw new InvalidSettingsException(
                    String.format("Duplicate output column '%s' at %d and %d detected. Rename input column or remove manual aggregations.",
                        colName, outputColumns.get(colName), i));
            } else {
                outputColumns.put(colName, i);
            }
        }

        return KNIMEToIntermediateConverterRegistry.convertSpec(new IntermediateSpec(outputFields.toArray(new IntermediateField[0])));
    }

    /**
     * @return output spec of the Spark Pivot job with manually defined values
     * @throws InvalidSettingsException
     */
    private DataTableSpec createPivotOutputSpec(final SparkVersion sparkVersion, final IntermediateSpec inputSpec,
        final BufferedDataTable valuesTable,
        final List<SparkSQLFunctionJobInput> groupByFunctions, final List<SparkSQLFunctionJobInput> aggFunctions,
        final SparkSQLFunctionCombinationProvider functionProvider) throws InvalidSettingsException {


        final ArrayList<IntermediateField> outputFields = new ArrayList<>();
        final HashSet<String> outputColNames = new HashSet<>(outputFields.size());

        // first add the columns we group over
        for (SparkSQLFunctionJobInput config : groupByFunctions) {
            outputFields.add(functionProvider.getFunctionResultField(sparkVersion, inputSpec, config));
            outputColNames.add(config.getOutputName());
        }

        // collect aggregation names
        final IntermediateDataType aggResult[] = new IntermediateDataType[aggFunctions.size()];
        final String aggName[] = new String[aggFunctions.size()];
        for (int i = 0; i < aggFunctions.size(); i++) {
            final SparkSQLFunctionJobInput aggIn = aggFunctions.get(i);
            aggResult[i] = functionProvider.getFunctionResultField(sparkVersion, inputSpec, aggIn).getType();
            aggName[i] = aggIn.getOutputName();
        }

        // collect pivot values
        final String[] pivotValues;
        if (valuesTable != null) {
            final String[] valuesFromTable = m_pivotSettings.getValues(valuesTable);
            if (valuesFromTable.length > m_pivotSettings.getValuesLimit()) {
                pivotValues = Arrays.copyOf(valuesFromTable, m_pivotSettings.getValuesLimit());
            } else {
                pivotValues = valuesFromTable;
            }
        } else {
            pivotValues = m_pivotSettings.getValues();
        }

        // combine pivot values with aggregation names
        for (String pivotValue : pivotValues) {
            final String pivotString = (pivotValue != null) ? pivotValue : "?";

            for (int i = 0; i < aggResult.length; i++) {
                final String pivotColumnName = String.format("%s+%s", pivotString, aggName[i]);

                if (outputColNames.add(pivotColumnName)) {
                    outputFields.add(new IntermediateField(pivotColumnName, aggResult[i], true));
                } else {
                    String msg = String.format("Duplicate column '%s' in resulting table.\n", pivotColumnName);
                    if (pivotValue == null || pivotString.equals("?")) {
                        msg +=
                            " Please adjust the column naming scheme and/or 'Ignore missing values' to prevent this.";
                    } else {
                        // conflict is unrelated to missing values -> conflict is between a group column and pivot value -> solvable via column naming scheme.
                        msg +=
                            " Please adjust the column naming strategy, or rename the grouping column before pivoting.";
                    }
                    throw new InvalidSettingsException(msg);
                }
            }
        }

        return KNIMEToIntermediateConverterRegistry.convertSpec(new IntermediateSpec(outputFields.toArray(new IntermediateField[0])));
    }

    /** @return group by column function input */
    private List<SparkSQLFunctionJobInput> createGroupByJobInput(final DataTableSpec inSpec,
        final SparkSQLFunctionCombinationProvider functionProvider) throws InvalidSettingsException {

        final List<SparkSQLFunctionJobInput> groupBy = new ArrayList<>();
        final ColumnNamePolicy columnNamePolicy = ColumnNamePolicy.getPolicy4Label(m_columnNamePolicy.getStringValue());

        // Add all group by columns
        for (String col : m_groupByCols.getIncludeList()) {
            final DataColumnSpec columnSpec = inSpec.getColumnSpec(col);
            if (columnSpec == null) {
                throw new InvalidSettingsException("Group column '" + col + "' not found in input table");
            }

            groupBy.add(getColumnFuncInput(functionProvider, columnSpec.getName()));
        }

        // Window over Time
//        if (m_windowSettings.isEnabled()) {
//            final String columnName = m_windowSettings.getColumnName(columnNamePolicy);
//            final String factoryName = functionProvider.getSparkSideFactory("window");
//            groupBy.add(m_windowSettings.getSparkJobInput(factoryName, columnName));
//        }

        return groupBy;
    }

    /** @return aggregation column function input */
    private List<SparkSQLFunctionJobInput> createAggJobInput(final DataTableSpec inSpec,
        final SparkSQLFunctionCombinationProvider functionProvider) throws InvalidSettingsException {

        final List<SparkSQLFunctionJobInput> aggFunc = new ArrayList<>();
        final ColumnNamePolicy columnNamePolicy = ColumnNamePolicy.getPolicy4Label(m_columnNamePolicy.getStringValue());

        // Add count(*) aggregations
        if (functionProvider.hasCountFunction() && m_addCountStar.getBooleanValue()) {
            final String columnName = m_countStarColName.getStringValue();
            final String factoryName = functionProvider.getSparkSideFactory("count");
            aggFunc.add(new SparkSQLFunctionJobInput("count", factoryName, columnName,
                new Serializable[] { "*" },
                new IntermediateDataType[] { IntermediateDataTypes.STRING }));
        }

        // Add aggregated columns
        for (int i = 0; i < m_aggregationFunction2Use.size(); i++) {
            final ColumnAggregationFunctionRow row = m_aggregationFunction2Use.get(i);
            final String columnName = row.getColumnSpec().getName();
            final SparkSQLAggregationFunction function = row.getFunction();
            final String factory = functionProvider.getSparkSideFactory(function.getId());

            if (inSpec.getColumnSpec(columnName) == null) {
                throw new InvalidSettingsException("Column '" + columnName + "' for aggregation function "
                        + row.getFunction().getLabel() + " does not exist");
            }

            final String outputName = generateColumnName(columnNamePolicy, inSpec, row);
            aggFunc.add(function.getSparkJobInput(factory, columnName, outputName, inSpec));
        }

        return aggFunc;
    }

    private SparkSQLFunctionJobInput getColumnFuncInput(final SparkSQLFunctionCombinationProvider functionProvider, final String columnName) {
        final String factoryName = functionProvider.getSparkSideFactory("column");
        return new SparkSQLFunctionJobInput("column", factoryName, columnName,
            new Serializable[] { columnName }, new IntermediateDataType[] { IntermediateDataTypes.STRING });
    }

    /**
     * @param policy the {@link ColumnNamePolicy}
     * @param inSpec input table spec
     * @param row {@link ColumnAggregationFunctionRow}
     * @return New column name based on the naming policy
     */
    public static String generateColumnName(final ColumnNamePolicy policy, final DataTableSpec inSpec,
        final ColumnAggregationFunctionRow row) {

        final String columnName = row.getColumnSpec().getName();
        final SparkSQLAggregationFunction method = row.getFunction();

        switch (policy) {
            case KEEP_ORIGINAL_NAME:
                return columnName;
            case AGGREGATION_METHOD_COLUMN_NAME:
                return method.getFuncNameColNameLabel(columnName, inSpec);
            case COLUMN_NAME_AGGREGATION_METHOD:
                return method.getColNameFuncNameLabel(columnName, inSpec);
            default:
                return columnName;
        }
    }

}
