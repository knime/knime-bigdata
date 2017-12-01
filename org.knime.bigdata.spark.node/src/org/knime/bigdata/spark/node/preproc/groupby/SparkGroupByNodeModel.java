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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
import java.util.List;

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
import org.knime.bigdata.spark.node.preproc.groupby.dialog.column.ColumnAggregationFunctionRow;
import org.knime.bigdata.spark.node.sql_function.SparkSQLAggregationFunction;
import org.knime.bigdata.spark.node.sql_function.SparkSQLFunctionCombinationProvider;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
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
 * Node model of the Spark GroupBy node.
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkGroupByNodeModel extends SparkNodeModel {
    /** The unique Spark job id. */
    public static final String JOB_ID = SparkGroupByNodeModel.class.getCanonicalName();

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

//    private final WindowFunctionSettings m_windowSettings = new WindowFunctionSettings();

    private final SettingsModelBoolean m_addCountStar = new SettingsModelBoolean(CFG_ADD_COUNT_STAR, false);

    private final SettingsModelString m_countStarColName = createCountStarColNameModel();

    private final SettingsModelFilterString m_groupByCols = new SettingsModelFilterString(CFG_GROUP_BY_COLUMNS);

    private final SettingsModelString m_columnNamePolicy = new SettingsModelString(CFG_COLUMN_NAME_POLICY,
        ColumnNamePolicy.getDefault().getLabel());

    private final AggregationFunctionSettings m_aggregationFunctionSettings = new AggregationFunctionSettings();

    private List<ColumnAggregationFunctionRow> m_aggregationFunction2Use = null;

    /**
     * Creates a new database group by.
     */
    SparkGroupByNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{SparkDataPortObject.TYPE});
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
            throw new InvalidSettingsException("Unsupported Spark Version! This node requires at least Spark 2.0.");
        }

        final SparkSQLFunctionCombinationProvider functionProvider = new SparkSQLFunctionCombinationProvider(sparkVersion);
        final ArrayList<ColumnAggregationFunctionRow> invalidColAggrs = new ArrayList<>(1);

        m_aggregationFunction2Use = m_aggregationFunctionSettings.getAggregationFunctions(tableSpec, functionProvider,
            m_groupByCols.getIncludeList(), invalidColAggrs);


        if (m_addCountStar.getBooleanValue() && !functionProvider.hasCountFunction()) {
            setWarningMessage("No Spark count(*) function provider exists.");
        }

//        if (m_windowSettings.isEnabled() && !functionProvider.hasWindowFunction()) {
//            setWarningMessage("No Spark window function provider exists.");
//        }

        if (!invalidColAggrs.isEmpty()) {
            setWarningMessage(invalidColAggrs.size() + " aggregation functions ignored due to incompatible columns.");
        }

        final IntermediateSpec inputSpec = SparkDataTableUtil.toIntermediateSpec(tableSpec);
        final List<SparkSQLFunctionJobInput> aggFunctions = createAggJobInput(tableSpec, functionProvider);
        if (aggFunctions == null || aggFunctions.isEmpty()) {
            throw new InvalidSettingsException("No aggregation function defined");
        }
        final DataTableSpec outputSpec = createOutputSpec(sparkVersion, inputSpec,
            createGroupByJobInput(tableSpec, functionProvider), aggFunctions, functionProvider);

        return new PortObjectSpec[] { new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec) };
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
        final DataTableSpec outputSpec = createOutputSpec(sparkVersion, inputSpec,
            groupByFunctions, aggFunctions, functionProvider);
        final SparkGroupByJobInput jobInput = new SparkGroupByJobInput(inputObject, outputObject,
            groupByFunctions.toArray(new SparkSQLFunctionJobInput[0]),
            aggFunctions.toArray(new SparkSQLFunctionJobInput[0]));

        exec.setMessage("Executing spark job...");
        SparkContextUtil.getJobRunFactory(contextID, JOB_ID).createRun(jobInput).run(contextID, exec);
        final SparkDataTable resultTable = new SparkDataTable(contextID, outputObject, outputSpec);
        final SparkDataPortObject sparkObject = new SparkDataPortObject(resultTable);

        return new PortObject[] { sparkObject };
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
    }

    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_groupByCols.loadSettingsFrom(settings);
//        m_windowSettings.loadSettingsFrom(settings);

        m_addCountStar.loadSettingsFrom(settings);
        m_countStarColName.loadSettingsFrom(settings);

        m_columnNamePolicy.loadSettingsFrom(settings);

        m_aggregationFunctionSettings.loadSettingsFrom(settings);
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
    }

    /**
     * @return output spec of the Spark GroupBy job
     */
    private DataTableSpec createOutputSpec(final SparkVersion sparkVersion, final IntermediateSpec inputSpec,
        final List<SparkSQLFunctionJobInput> groupByFunctions, final List<SparkSQLFunctionJobInput> aggFunctions,
        final SparkSQLFunctionCombinationProvider functionProvider) throws InvalidSettingsException {

        final List<SparkSQLFunctionJobInput> allFunctions = new ArrayList<>();
        allFunctions.addAll(groupByFunctions);
        allFunctions.addAll(aggFunctions);
        final SparkSQLFunctionJobInput functionInput[] = allFunctions.toArray(new SparkSQLFunctionJobInput[0]);
        final IntermediateField outputFields[] = new IntermediateField[functionInput.length];
        for (int i = 0; i < functionInput.length; i++) {
            final SparkSQLFunctionJobInput funcInput = functionInput[i];
            outputFields[i] = functionProvider
                    .getFunctionResultField(sparkVersion, inputSpec, funcInput);
        }

        return KNIMEToIntermediateConverterRegistry.convertSpec(new IntermediateSpec(outputFields));
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
