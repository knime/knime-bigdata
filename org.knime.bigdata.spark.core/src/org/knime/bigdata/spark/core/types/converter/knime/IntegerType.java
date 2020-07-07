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

import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import org.knime.core.data.DataCell;
import org.knime.core.data.IntValue;
import org.knime.core.data.def.IntCell;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class IntegerType extends AbstractKNIMEToIntermediateConverter {

    /**The only instance.*/
    public static final IntegerType INSTANCE = new IntegerType();

    private IntegerType() {
        super("Integer", IntCell.TYPE, IntermediateDataTypes.INTEGER,
            new IntermediateDataType[] {IntermediateDataTypes.INTEGER, IntermediateDataTypes.SHORT, IntermediateDataTypes.BYTE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataCell convertNotNullSerializable(final Serializable intermediateTypeObject,
        final KNIMEToIntermediateConverterParameter parameter) {

        if (intermediateTypeObject instanceof Number) {
            final Number val = (Number) intermediateTypeObject;
            return new IntCell(val.intValue());
        }

        throw incompatibleSerializableException(intermediateTypeObject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Serializable convertNoneMissingCell(final DataCell cell,
        final KNIMEToIntermediateConverterParameter parameter) {

        if (cell instanceof IntValue) {
            return ((IntValue)cell).getIntValue();
        }

        throw incompatibleCellException(cell);
    }

}
