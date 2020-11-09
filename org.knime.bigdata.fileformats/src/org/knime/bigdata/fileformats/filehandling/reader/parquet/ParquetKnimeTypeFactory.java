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
 *   Nov 6, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.ListKnimeType;
import org.knime.bigdata.fileformats.filehandling.reader.type.PrimitiveKnimeType;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.core.node.util.CheckUtils;

/**
 * Factory class for create {@link KnimeType} instances for Parquet types.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ParquetKnimeTypeFactory {

    public static KnimeType fromType(final Type type) {
        if (type.isPrimitive()) {
            return fromPrimitiveType(type.asPrimitiveType());
        } else {
            return fromGroupType(type.asGroupType());
        }
    }

    private static KnimeType fromGroupType(final GroupType type) {
        if (type.getOriginalType() == OriginalType.LIST) {
            final Type subtype = type.getType(0).asGroupType().getType(0);
            CheckUtils.checkArgument(subtype.isPrimitive(),
                "The field %s is a nested list, which is not supported at the moment.", type.getName());
            final KnimeType primitiveElementType = fromPrimitiveType(subtype.asPrimitiveType());
            return new ListKnimeType(primitiveElementType);
        } else {
            // TODO We could identify lists based on the repetitions of the fields
            throw new BigDataFileFormatException(String.format(
                "Found unsupported group type in column '%s', only supported group type is LIST.", type.getName()));
        }
    }

    private static PrimitiveKnimeType fromPrimitiveType(final PrimitiveType field) {
        if (field.getOriginalType() != null) {
            return fromOriginalType(field.getOriginalType());
        } else {
            return fromPrimitiveType(field.getPrimitiveTypeName());
        }
    }

    private static PrimitiveKnimeType fromPrimitiveType(final PrimitiveTypeName primitiveType) {
        switch (primitiveType) {
            case BOOLEAN:
                return PrimitiveKnimeType.BOOLEAN;
            case FLOAT:
            case DOUBLE:
                return PrimitiveKnimeType.DOUBLE;
            case INT32:
                return PrimitiveKnimeType.INTEGER;
            case INT64:
                return PrimitiveKnimeType.LONG;
            case INT96: // we can't represent a 96 bit integer in KNIME
                return PrimitiveKnimeType.STRING;
            case FIXED_LEN_BYTE_ARRAY:
            case BINARY:
            default:
                throw new IllegalArgumentException("Unsupported primitive type: " + primitiveType);
        }
    }

    private static PrimitiveKnimeType fromOriginalType(final OriginalType originalType) {//NOSONAR, the rule is nonsense for switches
        switch (originalType) {
            case DECIMAL:
                return PrimitiveKnimeType.DOUBLE;
            case UINT_8:
            case INT_8:
            case INT_16:
            case UINT_16:
            case INT_32:
                return PrimitiveKnimeType.INTEGER;
            case INT_64:
            case UINT_32:
                return PrimitiveKnimeType.LONG;
            case BSON:
            case DATE:
            case TIMESTAMP_MICROS:
            case TIMESTAMP_MILLIS:
            case TIME_MICROS:
            case TIME_MILLIS:
            case JSON:
            case UINT_64:
            case ENUM:
            case UTF8:
                return PrimitiveKnimeType.STRING;
            case LIST:
            case INTERVAL:
            case MAP:
            case MAP_KEY_VALUE:
            default:
                throw new IllegalArgumentException("Unsupported original type: " + originalType);
        }
    }

}
