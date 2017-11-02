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
package com.knime.bigdata.spark.core.types.converter.knime;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;

import org.knime.core.data.DataCell;
import org.knime.core.data.date.DateAndTimeCell;
import org.knime.core.data.date.DateAndTimeValue;

import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;

/**
 * Intermediate type for KNIMES {@link DateAndTimeType}.
 *
 * @author Tobias Koetter, KNIME.com
 * @deprecated Deprecated since 2.1.0 in favor of {@link LocalDateTimeType} and {@link LocalDateType}.
 */
@Deprecated
public class DateAndTimeType extends AbstractKNIMEToIntermediateConverter {

    /**The only instance.*/
    public static final DateAndTimeType INSTANCE = new DateAndTimeType();

    private DateAndTimeType() {
        super("Date and time", DateAndTimeCell.TYPE, IntermediateDataTypes.TIMESTAMP,
            new IntermediateDataType[] {IntermediateDataTypes.TIMESTAMP, IntermediateDataTypes.DATE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Serializable convertNoneMissingCell(final DataCell cell) {
        if (cell instanceof DateAndTimeValue) {
            return new Timestamp(((DateAndTimeValue)cell).getUTCTimeInMillis());
        }
        throw incompatibleCellException(cell);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataCell convertNotNullSerializable(final Serializable intermediateTypeObject) {
        if (intermediateTypeObject instanceof Timestamp) {
            Timestamp val = (Timestamp) intermediateTypeObject;
            return new DateAndTimeCell(val.getTime(), true, true, true);
        } else if (intermediateTypeObject instanceof Date) {
            Date val = (Date) intermediateTypeObject;
            return new DateAndTimeCell(val.getTime(), true, false, false);
        }
        throw incompatibleSerializableException(intermediateTypeObject);
    }
}
