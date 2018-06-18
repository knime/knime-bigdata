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
 * History 2 May 2018 (Marc Bux, KNIME AG, Zurich, Switzerland): created
 */
package org.knime.bigdata.fileformats.parquet.writer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.knime.bigdata.fileformats.parquet.ParquetTableStoreFormat;
import org.knime.bigdata.fileformats.parquet.type.ParquetType;
import org.knime.core.data.DataRow;

/**
 * A class that specifies how instances of {@link DataRow} are converted and
 * consumed by Parquet's {@link RecordConsumer}.
 *
 * @author Mareike Hoeger, KNIME AG, Zurich, Switzerland
 * @author Marc Bux, KNIME AG, Zurich, Switzerland
 */
public final class DataRowWriteSupport extends WriteSupport<DataRow> {
    private final String m_name;

    private final ParquetType[] m_columnTypes;

    private final boolean m_writeRowKey;

    private RecordConsumer m_recordConsumer;

    /**
     * Constructs a Write Support instance with the given name and column types
     *
     * @param name name of the table
     * @param columnTypes the column types
     * @param writeRowKey whether to write the row key
     */
    public DataRowWriteSupport(final String name, final ParquetType[] columnTypes, final boolean writeRowKey) {
        m_name = name;
        m_columnTypes = columnTypes.clone();
        m_writeRowKey = writeRowKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WriteContext init(final Configuration configuration) {
        final List<Type> fields = Arrays.stream(m_columnTypes).map(ParquetType::getParquetType)
                .collect(Collectors.toList());
        if (m_writeRowKey) {
            fields.add(new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY,
                    ParquetTableStoreFormat.PARQUET_SCHEMA_ROWKEY));
        }

        return new WriteContext(new MessageType(m_name, fields), new HashMap<>());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepareForWrite(final RecordConsumer recordConsumer) {
        m_recordConsumer = recordConsumer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(final DataRow record) {
        m_recordConsumer.startMessage();

        int i = 0;
        // write column values
        for (; i < m_columnTypes.length; i++) {
            m_columnTypes[i].writeValue(m_recordConsumer, record.getCell(i), i);
        }
        // then write row keys
        if (m_writeRowKey) {
            m_recordConsumer.startField(ParquetTableStoreFormat.PARQUET_SCHEMA_ROWKEY, i);
            m_recordConsumer.addBinary(Binary.fromString(record.getKey().getString()));
            m_recordConsumer.endField(ParquetTableStoreFormat.PARQUET_SCHEMA_ROWKEY, i);
        }

        m_recordConsumer.endMessage();
    }
}
