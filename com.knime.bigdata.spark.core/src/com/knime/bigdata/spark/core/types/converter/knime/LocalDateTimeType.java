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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.time.localdatetime.LocalDateTimeCell;
import org.knime.core.data.time.localdatetime.LocalDateTimeCellFactory;
import org.knime.core.data.time.localdatetime.LocalDateTimeValue;
import org.osgi.framework.Version;

import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import com.knime.bigdata.spark.core.version.SparkPluginVersion;

/**
 * Converts between LocalDateTime and Timestamps without time shifts.
 *
 * @author Sascha Wolke, KNIME.com
 * @since 2.1.0
 */
public class LocalDateTimeType extends AbstractKNIMEToIntermediateConverter {

    /** The only instance. */
    public static final LocalDateTimeType INSTANCE = new LocalDateTimeType();

    private LocalDateTimeType() {
        super("Local date and time", DataType.getType(LocalDateTimeCell.class), IntermediateDataTypes.TIMESTAMP,
            new IntermediateDataType[] { IntermediateDataTypes.TIMESTAMP });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Serializable convertNoneMissingCell(final DataCell cell) {
        if (cell instanceof LocalDateTimeValue) {
            // This interprets the given LocalDateTime in the UTC timezone. For example,
            // 2017-01-02 01:02:03 is interpreted as the instant 2017-01-02 01:02:03 UTC.
            final LocalDateTime localDateTime = ((LocalDateTimeValue)cell).getLocalDateTime();
            final Instant instant = Instant.from(localDateTime.atZone(ZoneOffset.UTC));
            return new Timestamp(instant.toEpochMilli());
        }

        throw incompatibleCellException(cell);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataCell convertNotNullSerializable(final Serializable intermediateTypeObject) {
        if (intermediateTypeObject instanceof Timestamp) {
            final Timestamp sqlTimestamp = (Timestamp) intermediateTypeObject;

            return LocalDateTimeCellFactory.create(
                LocalDateTime.ofEpochSecond(sqlTimestamp.getTime() / 1000, sqlTimestamp.getNanos(), ZoneOffset.UTC));
        }

        throw incompatibleSerializableException(intermediateTypeObject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Version getLowestSupportedVersion() {
        return SparkPluginVersion.VERSION_2_1_0;
    }
}
