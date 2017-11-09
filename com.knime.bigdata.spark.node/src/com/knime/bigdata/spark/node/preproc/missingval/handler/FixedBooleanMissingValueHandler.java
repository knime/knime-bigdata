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
 *
 * History
 *   Created on Nov 3, 2017 by bjoern
 */
package com.knime.bigdata.spark.node.preproc.missingval.handler;

import java.io.Serializable;
import java.util.Map;

import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.def.BooleanCell.BooleanCellFactory;

import com.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverter;
import com.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandler;
import com.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput;

/**
 * Replaces missing values with a fixed boolean false.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class FixedBooleanMissingValueHandler extends SparkMissingValueHandler {

    private final boolean m_fixedValue;

    /**
     * Creates a new instance.
     *
     * @param col the column this handler is configured for
     * @param fixedValue the fixed boolean value
     */
    public FixedBooleanMissingValueHandler(final DataColumnSpec col, final boolean fixedValue) {
        super(col);
        m_fixedValue = fixedValue;
    }

    @Override
    public Map<String, Serializable> getJobInputColumnConfig(final KNIMEToIntermediateConverter converter) {
        return SparkMissingValueJobInput.createFixedValueConfig(converter.convert(BooleanCellFactory.create(m_fixedValue)));
    }

    @Override
    public DerivedField getPMMLDerivedField(final Object aggResult) {
        return createValueReplacingDerivedField(org.dmg.pmml.DATATYPE.BOOLEAN, Boolean.toString(m_fixedValue));
    }
}
