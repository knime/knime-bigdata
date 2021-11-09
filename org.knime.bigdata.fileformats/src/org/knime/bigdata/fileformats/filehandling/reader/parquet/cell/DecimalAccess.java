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
 *   Nov 14, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet.cell;

import java.math.BigDecimal;

import org.apache.parquet.pig.convert.DecimalUtils;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.BinaryContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.IntContainer;
import org.knime.bigdata.fileformats.filehandling.reader.parquet.cell.Containers.LongContainer;
import org.knime.core.node.util.CheckUtils;

/**
 * Provides methods to read decimal values from various containers.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class DecimalAccess {

    private final int m_precision;

    private final int m_scale;

    DecimalAccess(final int precision, final int scale) {
        m_precision = precision;
        m_scale = scale;
    }

    @Deprecated
    DecimalAccess(final DecimalMetadata decimalMetaData) {
        this(decimalMetaData.getPrecision(), decimalMetaData.getScale());
    }

    @Deprecated
    DecimalAccess(final PrimitiveType type, final int maxSupportedPrecision) {
        this(type.getDecimalMetadata().getPrecision(), type.getDecimalMetadata().getScale(),
            type.getPrimitiveTypeName(), maxSupportedPrecision);
    }

    DecimalAccess(final int precision, final int scale, final PrimitiveTypeName typeName, final int maxSupportedPrecision) {
        this(precision, scale);
        CheckUtils.checkArgument(m_precision <= maxSupportedPrecision,
            "The precision %s is too high for the primitive type %s which only supports a precision up to %s.",
            m_precision, typeName, maxSupportedPrecision);
    }

    int getInt(final IntContainer container) {
        assert m_scale == 0 : "The scale must be 0 if the value should be read as int.";
        return container.getInt();
    }

    long getLong(final IntContainer container) {
        assert m_scale == 0 : "The scale must be 0 if the value should be read as long.";
        return container.getInt();
    }

    String getString(final IntContainer container) {
        if (m_scale == 0) {
            return Long.toString(getLong(container));
        } else {
            return Double.toString(getDouble(container));
        }
    }

    long getLong(final LongContainer container) {
        assert m_scale == 0 : "The scale must be 0 if the value should be read as long.";
        return container.getLong();
    }

    String getString(final LongContainer container) {
        if (m_scale == 0) {
            return Long.toString(getLong(container));
        } else {
            return Double.toString(getDouble(container));
        }
    }

    double getDouble(final IntContainer container) {
        return BigDecimal.valueOf(container.getInt(), m_scale).doubleValue();
    }

    double getDouble(final LongContainer container) {
        return BigDecimal.valueOf(container.getLong(), m_scale).doubleValue();
    }

    int getInt(final BinaryContainer container) {
        assert m_scale == 0
            && m_precision <= 9 : "The scale must be 0 and the precision <= 9 if the value should be read as int";
        return getBigDecimal(container).intValue();
    }

    long getLong(final BinaryContainer container) {
        assert m_scale == 0
            && m_precision <= 18 : "The scale must be 0 and the precision <= 18 if the value should be read as long";
        return getBigDecimal(container).longValue();
    }

    double getDouble(final BinaryContainer container) {
        return getBigDecimal(container).doubleValue();
    }

    String getString(final BinaryContainer container) {
        final BigDecimal bigDecimal = getBigDecimal(container);
        if (m_scale == 0 && m_precision <= 18) {
            return Long.toString(bigDecimal.longValue());
        } else {
            return bigDecimal.toPlainString();
        }
    }

    private BigDecimal getBigDecimal(final BinaryContainer container) {
        return DecimalUtils.binaryToDecimal(container.getBinary(), m_precision, m_scale);
    }

}