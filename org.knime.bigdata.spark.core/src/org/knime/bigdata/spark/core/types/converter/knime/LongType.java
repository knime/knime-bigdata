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
 *   Created on 05.07.2015 by koetter
 */
package org.knime.bigdata.spark.core.types.converter.knime;

import java.io.Serializable;

import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import org.knime.core.data.DataCell;
import org.knime.core.data.LongValue;
import org.knime.core.data.def.LongCell;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class LongType extends AbstractKNIMEToIntermediateConverter {

    /**The only instance.*/
    public static final LongType INSTANCE = new LongType();

    private LongType() {
        super("Long", LongCell.TYPE, IntermediateDataTypes.LONG);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataCell convertNotNullSerializable(final Serializable intermediateTypeObject) {
        if (intermediateTypeObject instanceof Number) {
            Number val = (Number) intermediateTypeObject;
            return new LongCell(val.longValue());
        }
        throw incompatibleSerializableException(intermediateTypeObject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Serializable convertNoneMissingCell(final DataCell cell) {
        if (cell instanceof LongValue) {
            return ((LongValue)cell).getLongValue();
        }
        throw incompatibleCellException(cell);
    }
}
