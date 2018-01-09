/*
 * ------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------
 */
package org.knime.bigdata.spark.node.preproc.groupby.dialog.type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.knime.bigdata.spark.node.preproc.groupby.dialog.AbstractAggregationFunctionRow;
import org.knime.bigdata.spark.node.sql_function.SparkSQLAggregationFunction;
import org.knime.bigdata.spark.node.sql_function.SparkSQLFunctionCombinationProvider;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 *
 * @author Tobias Koetter, KNIME GmbH
 */
public class DataTypeAggregationFunctionRow extends AbstractAggregationFunctionRow<SparkSQLAggregationFunction> {

    private static final String CNFG_DATA_TYPE = "dataType";

    private final DataType m_type;

    /**
     * @param type the {@link DataType} of the column to aggregate
     * @param function the {@link SparkSQLAggregationFunction} to use
     *
     */
    public DataTypeAggregationFunctionRow(final DataType type, final SparkSQLAggregationFunction function) {
        super(function);
        m_type = type;
    }

    /**
     * @return the {@link DataType} of the column to aggregate
     */
    public DataType getDataType() {
        return m_type;
    }

    /**
     * @param dataType {@link DataType} to check
     * @return <code>true</code> if the given {@link DataType} is compatible to the user selected {@link DataType}
     * of this aggregator
     */
    public boolean isCompatibleType(final DataType dataType) {
        return m_type.equals(dataType) || m_type.isASuperTypeOf(dataType);
    }

    /**
     * @param settings {@link NodeSettingsWO}
     * @param rows the {@link DataTypeAggregationFunctionRow}s to save
     */
    public static void saveFunctions(final NodeSettingsWO settings,
        final List<DataTypeAggregationFunctionRow> rows) {

        if (settings == null) {
            throw new NullPointerException("settings must not be null");
        }
        if (rows == null) {
            return;
        }
        for (int i = 0, length = rows.size(); i < length; i++) {
            final NodeSettingsWO cfg = settings.addNodeSettings("f_" + i);
            final DataTypeAggregationFunctionRow row = rows.get(i);
            cfg.addDataType(CNFG_DATA_TYPE, row.getDataType());
            saveFunction(cfg, row.getFunction());
        }
    }

    /**
     * Loads the functions and handles invalid aggregation functions graceful.
     * @param settings {@link NodeSettingsRO}
     * @param functionProvider the {@link SparkSQLFunctionCombinationProvider}
     * @param tableSpec the input {@link DataTableSpec}
     * @return {@link List} of {@link DataTypeAggregationFunctionRow}s
     * @throws InvalidSettingsException if the settings are invalid
     */
    public static List<DataTypeAggregationFunctionRow> loadFunctions(final NodeSettingsRO settings,
        final SparkSQLFunctionCombinationProvider functionProvider, final DataTableSpec tableSpec) throws InvalidSettingsException {

        final Set<String> settingsKeys = settings.keySet();
        if (settingsKeys.isEmpty()) {
            return Collections.emptyList();
        }

        final List<DataTypeAggregationFunctionRow> colAggrList = new ArrayList<>(settingsKeys.size());
        for (String settingsKey : settingsKeys) {
            final NodeSettingsRO cfg = settings.getNodeSettings(settingsKey);
            final DataType dataType = cfg.getDataType(CNFG_DATA_TYPE);
            SparkSQLAggregationFunction function =
                    loadFunction(tableSpec, functionProvider, cfg);
            final DataTypeAggregationFunctionRow aggrFunctionRow =
                    new DataTypeAggregationFunctionRow(dataType, function);
            colAggrList.add(aggrFunctionRow);
        }
        return colAggrList;
    }
}
