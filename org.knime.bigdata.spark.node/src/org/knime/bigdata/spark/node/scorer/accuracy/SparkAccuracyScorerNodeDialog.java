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
 *   Created on 30.09.2015 by Bjoern Lohrmann
 */
package org.knime.bigdata.spark.node.scorer.accuracy;

import org.knime.base.node.mine.scorer.accuracy.AccuracyScorerNodeDialog;
import org.knime.base.util.SortingStrategy;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;

/**
 * Extends the {@link AccuracyScorerNodeDialog} for the {@link SparkAccuracyScorerNodeModel}.
 *
 * @author Ole Ostergaard
 */
public class SparkAccuracyScorerNodeDialog extends AccuracyScorerNodeDialog {

    private static final SortingStrategy[] SUPPORTED_NUMBER_SORT_STRATEGIES = new SortingStrategy[] {
        SortingStrategy.Numeric, SortingStrategy.Lexical
    };

    private static final SortingStrategy[] SUPPORTED_STRING_SORT_STRATEGIES = new SortingStrategy[] {
        SortingStrategy.Lexical
    };

    /**
     * Creates a new {@link NodeDialogPane} for scoring in order to set the two
     * columns to compare.
     */
    public SparkAccuracyScorerNodeDialog() {
        super(false);
    } // SparkScorerNodeDialog(NodeModel)

    /**
     * {@inheritDoc}
     */
    @Override
    protected SortingStrategy[] getSupportedNumberSortStrategies() {
        return SUPPORTED_NUMBER_SORT_STRATEGIES;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SortingStrategy[] getSupportedStringSortStrategies() {
        return SUPPORTED_STRING_SORT_STRATEGIES;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected   SortingStrategy getFallbackStrategy() {
        return SortingStrategy.Lexical;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int getDataInputPortIndex() {
        return SparkAccuracyScorerNodeModel.INPORT;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getFirstCompID() {
        return SparkAccuracyScorerNodeModel.FIRST_COMP_ID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getSecondCompID() {
        return SparkAccuracyScorerNodeModel.SECOND_COMP_ID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getFlowVarPrefix() {
        return SparkAccuracyScorerNodeModel.FLOW_VAR_PREFIX;
    }

    /**
     * We need the {@link DataTableSpec} from the {@link SparkDataPortObjectSpec}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        if (specs == null || specs.length <= 0 || specs[SparkAccuracyScorerNodeModel.INPORT] == null) {
            throw new NotConfigurableException("No input Spark RDD available");
        }
        final DataTableSpec spec = ((SparkDataPortObjectSpec) specs[SparkAccuracyScorerNodeModel.INPORT]).getTableSpec();

        if ((spec == null) || (spec.getNumColumns() < 2)) {
            throw new NotConfigurableException("Scorer needs an input table "
                    + "with at least two columns");
        }

        DataTableSpec[] dataTableSpecs = {((SparkDataPortObjectSpec) specs[SparkAccuracyScorerNodeModel.INPORT]).getTableSpec()};
        loadSettingsFrom(settings, dataTableSpecs);
    }
}