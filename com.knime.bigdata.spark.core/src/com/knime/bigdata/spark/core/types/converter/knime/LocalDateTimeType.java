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
package com.knime.bigdata.spark.core.types.converter.knime;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.time.localdatetime.LocalDateTimeCell;
import org.knime.core.data.time.localdatetime.LocalDateTimeCellFactory;
import org.knime.core.data.time.localdatetime.LocalDateTimeValue;

import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;

/**
 * Converts between LocalDateTime and Timestamps without time shifts.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class LocalDateTimeType extends AbstractKNIMEToIntermediateConverter {

    /** The only instance. */
    public static final LocalDateTimeType INSTANCE = new LocalDateTimeType();

    private LocalDateTimeType() {
        super("Local date and time", DataType.getType(LocalDateTimeCell.class), IntermediateDataTypes.TIMESTAMP,
            new IntermediateDataType[] { IntermediateDataTypes.TIMESTAMP });
    }

    @Override
    protected Serializable convertNoneMissingCell(final DataCell cell) {
        if (cell instanceof LocalDateTimeValue) {
            LocalDateTime utcDateTime = ((LocalDateTimeValue)cell).getLocalDateTime().atOffset(ZoneOffset.UTC).toLocalDateTime();
            return Timestamp.valueOf(utcDateTime);
        }

        throw incompatibleCellException(cell);
    }

    @Override
    protected DataCell convertNotNullSerializable(final Serializable intermediateTypeObject) {
        if (intermediateTypeObject instanceof Timestamp) {
            return LocalDateTimeCellFactory.create(((Timestamp) intermediateTypeObject).toLocalDateTime());
        }

        throw incompatibleSerializableException(intermediateTypeObject);
    }
}
