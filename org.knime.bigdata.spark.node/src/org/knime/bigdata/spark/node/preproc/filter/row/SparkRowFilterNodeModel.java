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
 *   Created on Nov 5, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.preproc.filter.row;

import java.util.Iterator;
import java.util.Optional;

import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.preproc.filter.row.operator.SparkOperatorFunction;
import org.knime.bigdata.spark.node.preproc.filter.row.operator.SparkOperatorRegistry;
import org.knime.bigdata.spark.node.sql.SparkSQLJobInput;
import org.knime.bigdata.spark.node.sql.SparkSQLNodeModel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.rowfilter.OperatorParameters;
import org.knime.core.node.rowfilter.model.AbstractElement;
import org.knime.core.node.rowfilter.model.ColumnSpec;
import org.knime.core.node.rowfilter.model.Condition;
import org.knime.core.node.rowfilter.model.Group;
import org.knime.core.node.rowfilter.model.GroupType;
import org.knime.core.node.rowfilter.model.Node;
import org.knime.core.node.rowfilter.model.Operation;
import org.knime.core.node.rowfilter.model.Operator;
import org.knime.core.node.rowfilter.registry.OperatorKey;
import org.knime.core.node.rowfilter.registry.OperatorRegistry;
import org.knime.database.agent.rowfilter.DBGroupTypes;
import org.knime.database.agent.rowfilter.impl.DefaultDBRowFilter;

/**
 * Spark row filter node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkRowFilterNodeModel extends  SparkNodeModel {

    /** Supported condition group types */
    static final GroupType[] GROUP_TYPES = new GroupType[]{DBGroupTypes.AND, DBGroupTypes.OR};

    /** Settings model */
    private final SparkRowFilterSettings m_settings = new SparkRowFilterSettings();

    private DataTableSpec m_dataTableSpec;

    /** Default constructor */
    SparkRowFilterNodeModel() {
        super(new PortType[]{ SparkDataPortObject.TYPE }, new PortType[]{ SparkDataPortObject.TYPE });
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 0 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Spark data available");
        }

        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[0];
        final SparkVersion sparkVersion = SparkContextUtil.getSparkVersion(sparkSpec.getContextID());

        // To use Spark native API in a later implementation instead of a Spark SQL query, we do not support Spark 1.x
        if (SparkVersion.V_2_0.compareTo(sparkVersion) > 0) {
            throw new InvalidSettingsException("Unsupported Spark version. This node requires at least Spark 2.0.");
        }

        final DataTableSpec tableSpec = sparkSpec.getTableSpec();
        m_dataTableSpec = tableSpec;
        m_settings.validate(tableSpec);
        return new PortObjectSpec[] { new SparkDataPortObjectSpec(sparkSpec.getContextID(), tableSpec) };
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        return SparkSQLNodeModel.executeSQLQuery((SparkDataPortObject) inData[0], exec, generateQuery());
    }

    private String generateQuery() throws InvalidSettingsException {
        final Node root = m_settings.getRowFilterConfig().getRoot();
        final StringBuilder sb = new StringBuilder("SELECT * FROM " + SparkSQLJobInput.TABLE_PLACEHOLDER);
        if (root != null) {
            final SparkOperatorRegistry operatorRegistry = SparkOperatorRegistry.getInstance();
            sb.append(" WHERE ");
            consumeNode(root, sb, operatorRegistry);
        } else {
            setWarningMessage("Filters were not specified. Returning input data.");
        }
        return sb.toString();
    }

    /**
     * Based on {@link DefaultDBRowFilter#consumeNode}.
     *
     * TODO: replace this with intermediate placeholders and converted intermediate values.
     *
     * @param node
     * @param sb
     * @param operatorRegistry
     * @throws InvalidSettingsException
     */
    private void consumeNode(final Node node, final StringBuilder sb,
        final OperatorRegistry<SparkOperatorFunction> operatorRegistry) throws InvalidSettingsException {
        final AbstractElement value = node.getElement();

        if (value instanceof Condition) {
            final Condition condition = (Condition)value;
            final ColumnSpec columnSpec = condition.getColumnSpec();
            final Operation operation = condition.getOperation();
            final Operator operator = operation.getOperator();
            final OperatorKey key = OperatorKey.key(columnSpec.getType(), operator);
            final Optional<SparkOperatorFunction> function = operatorRegistry.findFunction(key);

            if (!function.isPresent()) {
                throw new InvalidSettingsException("Cann't find operator function by Key " + key);
            } else {
                sb.append(" ")
                    .append(function.get().apply(new OperatorParameters(columnSpec, operator, operation.getValues())));
            }

        } else if (value instanceof Group) {
            final String name = ((Group)value).getType().getName();

            sb.append(" (");

            final Iterator<Node> iterator = node.getChildren().iterator();
            while (iterator.hasNext()) {
                consumeNode(iterator.next(), sb, operatorRegistry);

                if (iterator.hasNext()) {
                    sb.append(" ").append(name);
                }
            }

            sb.append(" )");
        }
    }

    @Override
    protected void resetInternal() {
       m_dataTableSpec = null;
    }

    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveAdditionalSettingsTo(settings);
    }

    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadAdditionalValidatedSettingsFrom(settings);
    }

    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateAdditionalSettings(settings, m_dataTableSpec);
    }
}
