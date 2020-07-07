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
 */
package org.knime.bigdata.spark.core.types.converter.knime;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.time.localdate.LocalDateCell;
import org.knime.core.data.time.localdate.LocalDateCellFactory;
import org.knime.core.data.time.localdate.LocalDateValue;

/**
 * Converts between LocalDate and Date without time shifts.
 *
 * NOTE: This converter is currently unused becaused LocalDateTime support has been postponed.
 *
 * @author Sascha Wolke, KNIME GmbH
 * @since 2.2.0
 */
public class LocalDateType extends AbstractKNIMEToIntermediateConverter {

    /** The only instance. */
    public static final LocalDateType INSTANCE = new LocalDateType();

    private LocalDateType() {
        super("Local date", DataType.getType(LocalDateCell.class), IntermediateDataTypes.DATE,
            new IntermediateDataType[] { IntermediateDataTypes.DATE });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Serializable convertNoneMissingCell(final DataCell cell,
        final KNIMEToIntermediateConverterParameter parameter) {

        if (cell instanceof LocalDateValue) {
            // Convert local date into a UTC date (internal represented as long since 1970-01-01)
            LocalDate localDate = ((LocalDateValue)cell).getLocalDate();
            GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
            calendar.set(localDate.getYear(), localDate.getMonthValue() - 1, localDate.getDayOfMonth());
            return new Date(calendar.getTimeInMillis());
        }

        throw incompatibleCellException(cell);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataCell convertNotNullSerializable(final Serializable intermediateTypeObject,
        final KNIMEToIntermediateConverterParameter parameter) {

        if (intermediateTypeObject instanceof Date) {
            return LocalDateCellFactory.create(((Date) intermediateTypeObject).toLocalDate());
        }

        throw incompatibleSerializableException(intermediateTypeObject);
    }
}
