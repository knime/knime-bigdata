/*
 * ------------------------------------------------------------------------
 *
 * Copyright by KNIME AG, Zurich, Switzerland Website: http://www.knime.com;
 * Email: contact@knime.com
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, Version 3, as published by the
 * Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, see <http://www.gnu.org/licenses>.
 *
 * Additional permission under GNU GPL version 3 section 7:
 *
 * KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs. Hence,
 * KNIME and ECLIPSE are both independent programs and are not derived from each
 * other. Should, however, the interpretation of the GNU GPL Version 3
 * ("License") under any applicable laws result in KNIME and ECLIPSE being a
 * combined program, KNIME AG herewith grants you the additional permission to
 * use and propagate KNIME together with ECLIPSE with only the license terms in
 * place for ECLIPSE applying to ECLIPSE and the GNU GPL Version 3 applying for
 * KNIME, provided the license terms of ECLIPSE themselves allow for the
 * respective use and propagation of ECLIPSE together with KNIME.
 *
 * Additional permission relating to nodes for KNIME that extend the Node
 * Extension (and in particular that are based on subclasses of NodeModel,
 * NodeDialog, and NodeView) and that only interoperate with KNIME through
 * standard APIs ("Nodes"): Nodes are deemed to be separate and independent
 * programs and to not be covered works. Notwithstanding anything to the
 * contrary in the License, the License does not apply to Nodes, you are not
 * required to license Nodes under the License, and you are granted a license to
 * prepare and propagate Nodes, in each case even if such Nodes are propagated
 * with or for interoperation with KNIME. The owner of a Node may freely choose
 * the license terms applicable to such Node, including when such Node is
 * propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History 24 Apr 2018 (Marc Bux, KNIME AG, Zurich, Switzerland): created
 */
package org.knime.bigdata.fileformats.parquet.type;

import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.Type;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;

/**
 * This class provides a skeletal implementation of the {@link ParquetType}
 * interface.
 *
 * @author Marc Bux, KNIME AG, Zurich, Switzerland
 */
abstract class AbstractParquetType implements ParquetType {
    private final DataCellConverter m_converter;

    private final Type m_parquetType;

    AbstractParquetType(final DataCellConverter converter, final Type parquetType) {
        m_converter = converter;
        m_parquetType = parquetType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataCellConverter getConverter() {
        return m_converter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Type getParquetType() {
        return m_parquetType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataCell readValue() {
        return m_converter.fetchAndResetDataCell();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeValue(final RecordConsumer consumer, final DataCell cell, final int index) {
        if (!cell.isMissing()) {
            consumer.startField(m_parquetType.getName(), index);
            writeValueNonNull(consumer, cell);
            consumer.endField(m_parquetType.getName(), index);
        }
    }

    abstract void writeValueNonNull(final RecordConsumer consumer, final DataCell cell);

    /**
     * An abstract class that provides functionality common to all
     * DataCell-specific converters.
     */
    abstract static class DataCellConverter extends PrimitiveConverter {
        DataCell m_dataCell;

        DataCell fetchAndResetDataCell() {
            if (m_dataCell == null) {
                return DataType.getMissingCell();
            }
            final DataCell cell = m_dataCell;
            m_dataCell = null;
            return cell;
        }
    }
}
