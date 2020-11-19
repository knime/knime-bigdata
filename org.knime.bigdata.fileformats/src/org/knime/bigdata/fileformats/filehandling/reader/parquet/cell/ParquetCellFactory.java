/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Nov 13, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet.cell;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;

import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.ComposedParquetCell.Builder;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.BinaryContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.BooleanContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.DoubleContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.FloatContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.IntContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.LongContainer;
import org.knime.core.node.util.CheckUtils;

/**
 * Static factory class for {@link ParquetCell} instances.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ParquetCellFactory {

    private ParquetCellFactory() {
        // static factory class
    }

    /**
     * Creates the matching {@link ParquetCell} for the provided {@link Type}.
     *
     * @param type {@link Type} to create the {@link ParquetCell} for
     * @return a {@link ParquetCell} that can read {@link Type type}
     */
    public static ParquetCell create(final Type type) {
        if (type.isPrimitive()) {
            return createForPrimitiveType(type.asPrimitiveType());
        } else {
            return createForGroupType(type.asGroupType());
        }
    }

    private static ParquetCell createForPrimitiveType(final PrimitiveType type) {//NOSONAR
        switch (type.getPrimitiveTypeName()) {
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                return createForBinary(type);
            case BOOLEAN:
                return createForBoolean();
            case DOUBLE:
                return createForDouble();
            case FLOAT:
                return createForFloat();
            case INT32:
                return createForInt(type);
            case INT64:
                return createForLong(type);
            case INT96:
                return createForInt96();
            default:
                throw new IllegalArgumentException("Unsupported primitive type: " + type.getPrimitiveTypeName());
        }
    }

    private static ComposedParquetCell<BinaryContainer> createForInt96() {
        return ComposedParquetCell.builder(new BinaryContainer())//
            .withObjAccess(LocalDateTime.class, Accesses::getInt96LocalDateTime)//
            .withObjAccess(ZonedDateTime.class, Accesses::getInt96ZonedDateTime)//
            .withObjAccess(LocalDate.class, Accesses::getInt96LocalDate)//
            .withObjAccess(String.class, Accesses::getInt96ZonedDateTimeString)//
            .build();
    }

    private static ComposedParquetCell<BinaryContainer> createForBinary(final PrimitiveType type) {
        final Builder<BinaryContainer> builder = ComposedParquetCell.builder(new BinaryContainer());
        final OriginalType ot = type.getOriginalType();
        if (ot == null) {
            return builder//
                .withObjAccess(InputStream.class, Accesses::getInputStream)//
                .build();
        } else {
            switch (ot) {
                case DECIMAL:
                    return configureForBinaryDecimal(type, builder);
                case ENUM:
                case JSON:
                case UTF8:
                    return builder//
                        .withObjAccess(String.class, Accesses::getString)//
                        .build();
                default:
                    throw unsupportedOriginalType(ot, type.getPrimitiveTypeName());

            }
        }
    }

    private static ComposedParquetCell<BinaryContainer> configureForBinaryDecimal(final PrimitiveType type,
        final Builder<BinaryContainer> builder) {
        final DecimalMetadata dm = type.getDecimalMetadata();
        final DecimalAccess da = new DecimalAccess(dm);
        if (dm.getScale() == 0 && dm.getPrecision() <= 18) {
            // treat as long
            builder.withLongAccess(da::getLong);
        }
        return builder//
            .withDoubleAccess(da::getDouble)//
            .withObjAccess(String.class, da::getString)//
            .build();
    }

    private static ComposedParquetCell<LongContainer> createForLong(final PrimitiveType type) {//NOSONAR
        final Builder<LongContainer> builder = ComposedParquetCell.builder(new LongContainer());
        final OriginalType ot = type.getOriginalType();
        if (ot == null) {
            return configureForLong(builder);
        } else {
            switch (ot) {
                case DECIMAL:
                    return configureForLongDecimal(type, builder);
                case INT_64:
                    return configureForLong(builder);
                case TIMESTAMP_MICROS://NOSONAR
                    return builder//
                        .withObjAccess(LocalDateTime.class, Accesses::getLocalDateTimeMicros)//
                        .withObjAccess(ZonedDateTime.class, Accesses::getZonedDateTimeMicros)//
                        .withObjAccess(LocalDate.class, Accesses::getLocalDateMicros)//
                        .withObjAccess(String.class, Accesses::getZonedDateTimeStringMicros)//
                        .build();
                case TIMESTAMP_MILLIS://NOSONAR
                    return builder//
                        .withObjAccess(LocalDateTime.class, Accesses::getLocalDateTimeMillis)//
                        .withObjAccess(ZonedDateTime.class, Accesses::getZonedDateTimeMillis)//
                        .withObjAccess(LocalDate.class, Accesses::getLocalDateMillis)//
                        .withObjAccess(String.class, Accesses::getZonedDateTimeStringMillis)//
                        .build();
                case TIME_MICROS:
                    return builder//
                        .withObjAccess(LocalTime.class, Accesses::getLocalTime)//
                        .withObjAccess(String.class, Accesses::getLocalTimeString)//
                        .build();
                case UINT_64:
                    return builder//
                        .withObjAccess(String.class, Accesses::getUnsignedLongString)//
                        .build();
                default:
                    throw unsupportedOriginalType(ot, type.getPrimitiveTypeName());
            }
        }
    }

    private static IllegalArgumentException unsupportedOriginalType(final OriginalType originalType,
        final PrimitiveTypeName primitiveType) {
        return new IllegalArgumentException(String
            .format("The original type %s is not supported for the primitive type %s.", originalType, primitiveType));
    }

    private static ComposedParquetCell<LongContainer> configureForLong(final Builder<LongContainer> builder) {
        return builder.withLongAccess(Accesses::getLong)//
            .withDoubleAccess(Accesses::getDouble)//
            .withObjAccess(String.class, Accesses::getString)//
            .build();
    }

    private static ComposedParquetCell<LongContainer> configureForLongDecimal(final PrimitiveType type,
        final Builder<LongContainer> builder) {
        final DecimalAccess da = new DecimalAccess(type, 18);
        DecimalMetadata dm = type.getDecimalMetadata();
        if (dm.getScale() == 0) {
            builder//
                .withLongAccess(da::getLong);
        }
        return builder.withDoubleAccess(da::getDouble)//
            .withObjAccess(String.class, da::getString)//
            .build();
    }

    private static ComposedParquetCell<FloatContainer> createForFloat() {
        return ComposedParquetCell.builder(new FloatContainer())//
            .withDoubleAccess(Accesses::getDouble)//
            .withObjAccess(String.class, Accesses::getString)//
            .build();
    }

    private static ComposedParquetCell<DoubleContainer> createForDouble() {
        return ComposedParquetCell.builder(new DoubleContainer())//
            .withDoubleAccess(Accesses::getDouble)//
            .withObjAccess(String.class, Accesses::getString)//
            .build();
    }

    private static ComposedParquetCell<IntContainer> createForInt(final PrimitiveType type) {//NOSONAR
        final Builder<IntContainer> builder = ComposedParquetCell.builder(new IntContainer());
        final OriginalType ot = type.getOriginalType();
        if (ot == null) {
            return configureForInt(builder);
        } else {
            switch (ot) {
                case INT_8:
                case UINT_8:
                case INT_16:
                case INT_32:
                case UINT_16:
                    return configureForInt(builder);
                case UINT_32:
                    return builder//
                        .withLongAccess(Accesses::getUInt32)//
                        .withDoubleAccess(Accesses::getUInt32AsDouble)//
                        .withObjAccess(String.class, Accesses::getUInt32AsString)//
                        .build();
                case DATE:
                    return builder//
                        .withObjAccess(LocalDate.class, Accesses::getLocalDate)//
                        .withObjAccess(LocalDateTime.class, Accesses::getLocalDateTime)//
                        .withObjAccess(String.class, Accesses::getLocalDateString)//
                        .build();
                case DECIMAL:
                    return configureForIntDecimal(type, builder);
                case TIME_MILLIS:
                    return builder//
                        .withObjAccess(LocalTime.class, Accesses::getLocalTime)//
                        .withObjAccess(String.class, Accesses::getLocalTimeString)//
                        .build();
                default:
                    throw unsupportedOriginalType(ot, type.getPrimitiveTypeName());
            }
        }
    }

    private static ComposedParquetCell<IntContainer> configureForIntDecimal(final PrimitiveType type,
        final Builder<IntContainer> builder) {
        final DecimalAccess da = new DecimalAccess(type, 9);
        if (type.getDecimalMetadata().getScale() == 0) {
            builder.withLongAccess(da::getLong);
        }
        return builder//
            .withDoubleAccess(da::getDouble)//
            .withObjAccess(String.class, da::getString)//
            .build();
    }

    private static ComposedParquetCell<IntContainer> configureForInt(final Builder<IntContainer> builder) {
        return builder.withIntAccess(Accesses::getInt)//
            .withLongAccess(Accesses::getLong)//
            .withDoubleAccess(Accesses::getDouble)//
            .withObjAccess(String.class, Accesses::getString)//
            .build();
    }

    private static ComposedParquetCell<BooleanContainer> createForBoolean() {
        return ComposedParquetCell.builder(new BooleanContainer())//
            .withBooleanAccess(Accesses::getBoolean)//
            .withObjAccess(String.class, Accesses::getString)//
            .build();
    }

    private static ParquetCell createForGroupType(final GroupType type) {
        CheckUtils.checkArgument(type.getOriginalType() == OriginalType.LIST, "Unsupported group type %s.",
            type.getOriginalType());
        final Type subtype = type.getType(0).asGroupType().getType(0);
        CheckUtils.checkArgument(subtype.isPrimitive(),
            "The field %s is a nested list, which is not supported at the moment.", type.getName());
        final PrimitiveType elementType = subtype.asPrimitiveType();
        return new ListParquetCell(create(elementType));
    }

}
