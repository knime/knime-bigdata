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
package org.knime.bigdata.spark.node.preproc.missingval.handler;

import java.io.Serializable;
import java.util.Map;

import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelDouble;

import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverter;
import org.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandler;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput;

/**
 * Replaces missing values with a fixed double value.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class FixedDoubleMissingValueHandler extends SparkMissingValueHandler {

    private static final String FIX_VAL_CFG = "fixDoubleValue";

    private final SettingsModelDouble m_fixVal = createDoubleValueSettingsModel();

    /**
     * @param col the column this handler is configured for
     */
    public FixedDoubleMissingValueHandler(final DataColumnSpec col) {
        super(col);
    }

    /**
     * @return a new SettingsModel for the fix value the user can select
     */
    public static SettingsModelDouble createDoubleValueSettingsModel() {
        return new SettingsModelDouble(FIX_VAL_CFG, 0d);
    }

    @Override
    public Map<String, Serializable> getJobInputColumnConfig(final KNIMEToIntermediateConverter converter) {
        return SparkMissingValueJobInput
            .createFixedValueConfig(converter.convert(new DoubleCell(m_fixVal.getDoubleValue())));
    }

    @Override
    public DerivedField getPMMLDerivedField(final Object aggResult) {
        return createValueReplacingDerivedField(org.dmg.pmml.DATATYPE.DOUBLE,
            Double.toString(m_fixVal.getDoubleValue()));
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fixVal.loadSettingsFrom(settings);
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_fixVal.saveSettingsTo(settings);
    }
}
