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
 *   Sep 24, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet.cell;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.knime.core.node.util.CheckUtils;

/**
 * Static factory class for creating {@link ParquetCell} instances.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ParquetCellFactory {

    private ParquetCellFactory() {
        // Factory class
    }

    /**
     * Creates a {@link ParquetCell} for reading a column of {@link Type} type
     *
     * @param type {@link Type} of the column to read
     * @return a {@link ParquetCell} that can read from {@link Type type}
     */
    public static ParquetCell createFor(final Type type) {
        if (type.isPrimitive()) {
            return getForType(type.asPrimitiveType());
        } else {
            return createListCell(type.asGroupType());
        }
    }

    private static ParquetCell createListCell(final GroupType groupType) {
        final Type subtype = groupType.getType(0).asGroupType().getType(0);
        CheckUtils.checkArgument(subtype.isPrimitive(),
            "The field %s is a nested list, which is not supported at the moment.", groupType.getName());
        final PrimitiveType elementType = subtype.asPrimitiveType();
        return new ListParquetCell(getForType(elementType));
    }

    private static ParquetCell getForType(final PrimitiveType type) {// NOSONAR, stupid rule
        final OriginalType ot = type.getOriginalType();
        switch (type.getPrimitiveTypeName()) {
            case BINARY:
                return createCellForBinary(type, ot);
            case BOOLEAN:
                return new BooleanParquetCell();
            case DOUBLE:
                return new DoubleParquetCell();
            case FIXED_LEN_BYTE_ARRAY:
                return createCellForFixedLenByteArray(type, ot);
            case FLOAT:
                return new FloatParquetCell();
            case INT32:
                return new IntParquetCell();
            case INT64:
                return new LongParquetCell();
            case INT96:
                return new Int96ParquetCell();
            default:
                throw new IllegalArgumentException();
        }
    }

    private static ParquetCell createCellForFixedLenByteArray(final PrimitiveType type, final OriginalType ot) {
        if (ot == OriginalType.UTF8) {
            return new StringParquetCell();
        } else if (ot == OriginalType.DECIMAL) {
            return new DecimalParquetCell(type.getDecimalMetadata());
        } else {
            throw new IllegalArgumentException();
        }
    }

    private static ParquetCell createCellForBinary(final PrimitiveType type, final OriginalType ot) {
        if (ot == OriginalType.UTF8 || ot == OriginalType.ENUM) {
            return new StringParquetCell();
        } else if (ot == OriginalType.DECIMAL) {
            return new DecimalParquetCell(type.getDecimalMetadata());
        } else {
            throw new IllegalArgumentException();
        }
    }
}
