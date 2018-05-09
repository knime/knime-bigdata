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
 *   21.02.2018 (Mareike Hoeger): created
 */
package org.knime.bigdata.parquet.utility;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type.Repetition;
import org.knime.bigdata.parquet.node.writer.ParquetWriteException;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataCellDataOutput;
import org.knime.core.data.DataCellSerializer;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DataTypeRegistry;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.NodeLogger;

/**
 * Will be replaced with Parquet functionality in org.knime.parquet!
 * WriteSupport implementation for KNIME DataRow
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class KNIMEWriteSupport extends WriteSupport<DataRow> {

    private final NodeLogger LOGGER = NodeLogger.getLogger(KNIMEWriteSupport.class);
    private RecordConsumer m_recordConsumer;
    private final MessageType m_schema;
    private final DataTableSpec m_datatablespec;
    private final boolean m_writeRowKey;

    /**
     * Creates a org.apache.parquet.hadoop.api.WriteSupport for Knime tables
     *
     * @param parqSchema the Parquet schema, that is used
     * @param tablespec the KNIME table spec
     * @param writeRowKey whether the row key should be written
     */
    public KNIMEWriteSupport(final MessageType parqSchema, final DataTableSpec tablespec, final boolean writeRowKey) {
        this.m_schema = parqSchema;
        this.m_datatablespec = tablespec;
        m_writeRowKey = writeRowKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WriteContext init(final Configuration configuration) {

        final Map<String, String> metaData = new HashMap<>();
        DataColumnSpec columnspec;

        for (int i = 0; i < m_datatablespec.getNumColumns(); i++) {
            columnspec = m_datatablespec.getColumnSpec(i);
            metaData.put("Column_" + columnspec.getName(), columnspec.getType().getCellClass().getName());
        }

        return new WriteContext(m_schema, metaData);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepareForWrite(final RecordConsumer consumer) {
        m_recordConsumer = consumer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(final DataRow row) {

        m_recordConsumer.startMessage();
        int index = 0;
        if (m_writeRowKey) {
            m_recordConsumer.startField("rowKey", index);
            m_recordConsumer.addBinary(Binary.fromString(row.getKey().getString()));
            m_recordConsumer.endField("rowKey", index);
            index++;
        }

        for (int i = 0; i < row.getNumCells(); i++) {
            final DataCell cell = row.getCell(i);
            if (cell.isMissing() && m_schema.getType(index).isRepetition(Repetition.REQUIRED)) {
                throw new ParquetWriteException(String.format("Null in non null field in column %s row %s.",
                        m_schema.getFieldName(index), row.getKey()));
            }
            if (!cell.isMissing()) {
                m_recordConsumer.startField(m_schema.getFieldName(i), index);
                writeCell(m_datatablespec.getColumnSpec(i), cell);
                m_recordConsumer.endField(m_schema.getFieldName(i), index);
            }
            index++;
        }
        m_recordConsumer.endMessage();
    }

    /**
     * @param dataColumnSpec
     * @param cell
     * @throws UnsupportedEncodingException
     */
    private void writeCell(DataColumnSpec dataColumnSpec, final DataCell cell) {
        final DataType columnType = dataColumnSpec.getType();
        if (columnType == IntCell.TYPE) {
            m_recordConsumer.addInteger(((IntValue) cell).getIntValue());
        } else if (cell.getType() == StringCell.TYPE) {

            try {
                m_recordConsumer.addBinary(
                        Binary.fromConstantByteArray(((StringCell) cell).getStringValue().getBytes("UTF-8")));
            } catch (final UnsupportedEncodingException ex) {
                LOGGER.error("Could not encode StringCell.", ex);
                throw new ParquetWriteException("Could not encode StringCell.", ex);
            }
        } else if (columnType == LongCell.TYPE) {
            m_recordConsumer.addLong(((LongValue) cell).getLongValue());
        } else if (columnType == BooleanCell.TYPE) {
            m_recordConsumer.addBoolean(((BooleanCell) cell).getBooleanValue());
        } else if (columnType == DoubleCell.TYPE) {
            m_recordConsumer.addDouble(((DoubleValue) cell).getDoubleValue());
        } else {

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                final DataCelltoByteArrayObjectOutputStream cellOutput = new DataCelltoByteArrayObjectOutputStream(out);
                cellOutput.writeDataCell(cell);
                m_recordConsumer.addBinary(Binary.fromConstantByteArray(out.toByteArray()));
                cellOutput.close();
            } catch (final IOException e) {
                LOGGER.error("Failed to write cell.", e);
                throw new ParquetWriteException("Could not encode non-primitv cell.", e);
            }
        }
    }

    /** Output stream used for cloning a data cell. */
    private static final class DataCelltoByteArrayObjectOutputStream extends ObjectOutputStream
            implements DataCellDataOutput {
        ByteArrayOutputStream m_out;

        /**
         * Call super.
         *
         * @param out To delegate
         * @throws IOException If super throws it.
         */
        DataCelltoByteArrayObjectOutputStream(ByteArrayOutputStream out) throws IOException {
            super(out);
            m_out = out;
        }

        /** {@inheritDoc} */
        @Override
        public void writeDataCell(final DataCell cell) throws IOException {
            writeUTF(cell.getClass().getName());
            final Optional<DataCellSerializer<DataCell>> cellSerializer = DataTypeRegistry.getInstance()
                    .getSerializer(cell.getClass());
            if (cellSerializer.isPresent()) {
                cellSerializer.get().serialize(cell, this);
            } else {
                writeObject(cell);
            }
        }

        @Override
        public void writeUTF(final String s) throws IOException {
            m_out.write(s.getBytes("UTF-8"));
        }
    }

}
