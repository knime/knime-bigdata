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
 *   25 Apr 2018 (Marc Bux, KNIME AG, Zurich, Switzerland): created
 */
package org.knime.bigdata.fileformats.parquet.type;

import java.nio.charset.StandardCharsets;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.knime.bigdata.fileformats.parquet.type.AbstractParquetType.DataCellConverter;
import org.knime.core.data.DataCell;
import org.knime.core.data.StringValue;
import org.knime.core.data.def.StringCell;

/**
 * The factory class for creating {@link ParquetType} instances for converting between KNIME {@link StringCell} and
 * Parquet binary values.
 *
 * @author Marc Bux, KNIME AG, Zurich, Switzerland
 */
public final class StringParquetTypeFactory implements ParquetTypeFactory {
    /**
     * {@inheritDoc}
     */
    @Override
    public ParquetType create(final String columnName) {
        return new StringParquetType(columnName);
    }

    /**
     * The class for converting between a specific column of KNIME {@link StringCell} and binary double values.
     */
    final static class StringParquetType extends AbstractParquetType {
        StringParquetType(final String columnName) {
            super(new StringCellConverter(),
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, 0, columnName));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        void writeValueNonNull(final RecordConsumer consumer, final DataCell cell) {
            consumer.addBinary(Binary.fromString(((StringValue)cell).getStringValue()));
        }

    }

    /**
     * The {@link DataCellConverter} corresponding to the class {@link StringParquetType}.
     */
    final static class StringCellConverter extends DataCellConverter {
        /**
         * {@inheritDoc}
         */
        @Override
        public void addBinary(final Binary value) {
            m_dataCell = new StringCell(new String(value.getBytes(), StandardCharsets.UTF_8));
        }
    }
}
