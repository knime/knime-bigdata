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
 */
package org.knime.bigdata.spark.node.preproc.missingval;

import java.io.Serializable;
import java.util.Map;

import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.dmg.pmml.ExtensionDocument.Extension;
import org.knime.base.data.statistics.Statistic;
import org.knime.base.node.preproc.pmml.missingval.DataColumnWindow;
import org.knime.base.node.preproc.pmml.missingval.MissingCellHandler;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverter;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.RowKey;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * The base class of all missing value handlers for Spark. Since the actual handling
 * of data does not happen in KNIME Analytics Platform, but in the remote Spark cluster,
 * many of the methods of {@link MissingCellHandler} do not apply.
 *
 * @author Sascha Wolke, KNIME GmbH
 * @since 3.5
 */
public abstract class SparkMissingValueHandler extends MissingCellHandler {

    private final boolean m_isPMML4_2Compatible;

    /**
     * Constructor for a PMML 4.2 compatible Spark missing value handler.
     *
     * @param col the column spec this handler is used for
     */
    public SparkMissingValueHandler(final DataColumnSpec col) {
        this(col, true);
    }

    /**
     * Constructor for the missing value handler.
     *
     * @param col the column spec this handler is used for.
     * @param isPMML4_2Compatible whether the strategy implemented by this handler is PMML 4.2 compatible or not.
     */
    public SparkMissingValueHandler(final DataColumnSpec col, final boolean isPMML4_2Compatible) {
        super(col);
        m_isPMML4_2Compatible = isPMML4_2Compatible;
    }

    /**
     * Creates a column configuration to be used with {@link SparkMissingValueJobInput}.
     *
     * @param converter The {@link KNIMEToIntermediateConverter} to use for this column.
     * @return the column configuration.
     * @throws InvalidSettingsException if the job contains invalid configurations
     */
    public abstract Map<String, Serializable> getJobInputColumnConfig(final KNIMEToIntermediateConverter converter) throws InvalidSettingsException;

    /**
     * Creates a derived field for the documentation of the operation in PMML.
     *
     * @param aggResult result of aggregation or null
     * @return the derived field
     */
    public abstract DerivedField getPMMLDerivedField(final Object aggResult);

    /**
     * Creates a missing value handler from an extension that is inside of a PMML derived field.
     *
     * @param column the column this handler is used for
     * @param ext the extension containing the necessary information
     * @return a missing cell handler that was initialized from the extension
     * @throws InvalidSettingsException if the the factory from the extension is not applicable for the column
     */
    public static SparkMissingValueHandler fromPMMLExtension(final DataColumnSpec column, final Extension ext)
        throws InvalidSettingsException {

        final SparkMissingValueHandlerFactoryManager manager = SparkMissingValueHandlerFactoryManager.getInstance();
        return (SparkMissingValueHandler)fromPMMLExtension(column, manager, ext);
    }

    /**
     *
     * @return whether the missing value handler is PMML 4.2 compatible.
     */
    public boolean isPMML4_2Compatible() {
        return m_isPMML4_2Compatible;
    }

    @Override
    public Statistic getStatistic() {
        return null;
    }

    @Override
    public final DataCell getCell(final RowKey key, final DataColumnWindow window) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final DerivedField getPMMLDerivedField() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getPreviousCellsWindowSize() {
        return 0;
    }

    @Override
    public int getNextCellsWindowSize() {
        return 0;
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        // default implementation does nothing
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        // default implementation does nothing
    }
}
