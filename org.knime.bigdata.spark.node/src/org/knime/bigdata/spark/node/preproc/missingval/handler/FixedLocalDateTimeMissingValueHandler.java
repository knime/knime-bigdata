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
import org.knime.core.data.time.localdatetime.LocalDateTimeCellFactory;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.time.util.SettingsModelDateTime;

import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverter;
import org.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandler;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput;

/**
 * Replaces missing values with a fixed local date time.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class FixedLocalDateTimeMissingValueHandler extends SparkMissingValueHandler {

    private static final String FIX_VAL_CFG = "fixLocalDateTimeValue";

    private final SettingsModelDateTime m_fixVal = createDateTimeValueSettingsModel();

    /**
     * @param col the column this handler is configured for
     */
    public FixedLocalDateTimeMissingValueHandler(final DataColumnSpec col) {
        super(col);
    }

    /**
     * @return a new SettingsModel for the fix value the user can select
     */
    public static SettingsModelDateTime createDateTimeValueSettingsModel() {
        return new SettingsModelDateTime(FIX_VAL_CFG, null);
    }

    @Override
    public Map<String, Serializable> getJobInputColumnConfig(final KNIMEToIntermediateConverter converter) {
        return SparkMissingValueJobInput
            .createFixedValueConfig(converter.convert(LocalDateTimeCellFactory.create(m_fixVal.getLocalDateTime())));
    }

    @Override
    public DerivedField getPMMLDerivedField(final Object aggResult) {
        return createValueReplacingDerivedField(org.dmg.pmml.DATATYPE.STRING, m_fixVal.getLocalDateTime().toString());
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
