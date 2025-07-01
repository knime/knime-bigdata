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
 *   2025-05-27 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.iceberg.types;

import java.util.Arrays;

import org.knime.core.data.DataType;
import org.knime.core.data.blob.BinaryObjectDataCell;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.time.localdate.LocalDateCellFactory;
import org.knime.core.data.time.localdatetime.LocalDateTimeCellFactory;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCellFactory;

/**
 * Enum of primitive types supported in KNIME.
 *
 * Based on {@code PrimitiveKnimeType} of the Big Data Reader.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public enum IcebergPrimitiveType implements IcebergDataType {

        /**
         * String type.
         */
        STRING("String", "string", StringCell.TYPE),

        /**
         * Boolean type.
         */
        BOOLEAN("Boolean", "boolean", BooleanCell.TYPE),

        /**
         * Integer type.
         */
        INTEGER("Integer", "integer", IntCell.TYPE),

        /**
         * Long type.
         */
        LONG("Long", "long", LongCell.TYPE),

        /**
         * Double type.
         */
        DOUBLE("Double", "double", DoubleCell.TYPE),

        /**
         * Binary type.
         */
        BINARY("Binary", "binary", BinaryObjectDataCell.TYPE),

        /**
         * Local date type.
         */
        DATE("Date", "date", LocalDateCellFactory.TYPE),

        /**
         * Instant date&time type
         */
        INSTANT_DATE_TIME("Instant Date & Time", "instantDateAndTime", ZonedDateTimeCellFactory.TYPE),

        /**
         * Local date&time type i.e. no instant semantics
         */
        LOCAL_DATE_TIME("Local Date & Time", "localDateAndTime", LocalDateTimeCellFactory.TYPE);

    private final String m_label;

    private final String m_serializableType;

    private final DataType m_defaultDataType;

    IcebergPrimitiveType(final String label, final String serializableType, final DataType defaultDataType) {
        m_label = label;
        m_serializableType = serializableType;
        m_defaultDataType = defaultDataType;
    }

    @Override
    public DataType getDefaultDataType() {
        return m_defaultDataType;
    }

    @Override
    public String toString() {
        return m_label;
    }

    static IcebergDataType toExternalType(final String serializedType) {
        return Arrays.stream(values()) //
            .filter(type -> type.toSerializableType().equals(serializedType)) //
            .findAny()
            .orElseThrow(() -> new IllegalArgumentException("Unknown type: '" + serializedType + "'"));
    }

    @Override
    public String toSerializableType() {
        return m_serializableType;
    }

}
