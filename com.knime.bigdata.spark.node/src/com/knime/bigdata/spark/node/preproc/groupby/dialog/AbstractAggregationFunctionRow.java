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
 * ---------------------------------------------------------------------
 */
package com.knime.bigdata.spark.node.preproc.groupby.dialog;

import org.knime.base.data.aggregation.dialogutil.AggregationFunctionRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.database.aggregation.AggregationFunction;

import com.knime.bigdata.spark.node.sql_function.SparkSQLAggregationFunction;
import com.knime.bigdata.spark.node.sql_function.SparkSQLFunctionCombinationProvider;

/**
 * Spark aggregation function table row. Based on AbstractDBAggregationFunctionRow.
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 * @param <F> the {@link SparkSQLAggregationFunction}
 */
public abstract class AbstractAggregationFunctionRow<F extends SparkSQLAggregationFunction>
implements AggregationFunctionRow<F> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(AbstractAggregationFunctionRow.class);

    private static final String CNFG_AGGR_COL_SECTION = "aggregationFunction";

    private final F m_function;

    private boolean m_valid = true;

    /**
     * @param function the {@link SparkSQLAggregationFunction} to use
     *
     */
    public AbstractAggregationFunctionRow(final F function) {
        m_function = function;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public F getFunction() {
        return m_function;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid() {
        return m_valid;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setValid(final boolean valid) {
        m_valid = valid;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsMissingValueOption() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean inclMissingCells() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setInclMissingCells(final boolean inclMissingCells) {
        // nothing to do
    }

    /**
     * @param cfg {@link NodeSettingsWO} to write to
     * @param function the {@link AggregationFunction} to save
     */
    public static void saveFunction(final NodeSettingsWO cfg, final AggregationFunction function) {
        cfg.addString(CNFG_AGGR_COL_SECTION, function.getId());
        if (function.hasOptionalSettings()) {
            try {
                final NodeSettingsWO subConfig = cfg.addNodeSettings("functionSettings");
                function.saveSettingsTo(subConfig);
            } catch (Exception e) {
                LOGGER.error("Exception while saving settings for aggreation function '"
                    + function.getId() + "', reason: " + e.getMessage());
            }
        }
    }

    /**
     * @param tableSpec optional input {@link DataTableSpec}
     * @param functionProvider the {@link SparkSQLFunctionCombinationProvider}
     * @param cfg {@link NodeSettingsRO} to read from
     * @return the {@link SparkSQLAggregationFunction}
     * @throws InvalidSettingsException if the settings of the function are invalid
     */
    public static SparkSQLAggregationFunction loadFunction(final DataTableSpec tableSpec,
        final SparkSQLFunctionCombinationProvider functionProvider, final NodeSettingsRO cfg)
        throws InvalidSettingsException {

        final String functionId = cfg.getString(CNFG_AGGR_COL_SECTION);
        SparkSQLAggregationFunction function = functionProvider.getFunction(functionId);
        if (function.hasOptionalSettings()) {
            try {
                final NodeSettingsRO subSettings = cfg.getNodeSettings("functionSettings");
                if (tableSpec != null) {
                    //this method is called from the dialog
                    function.loadSettingsFrom(subSettings, tableSpec);
                } else {
                    //this method is called from the node model where we do not
                    //have the DataTableSpec
                    function.loadValidatedSettings(subSettings);
                }
            } catch (Exception e) {
                final String errMsg = "Failed to load settings for aggreation function '"
                    + function.getId() + "', reason: " + e.getMessage();
                throw new InvalidSettingsException(errMsg, e);
            }
        }
        return function;
    }
}
