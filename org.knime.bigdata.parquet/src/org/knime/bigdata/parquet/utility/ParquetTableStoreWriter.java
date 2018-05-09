/*
 * ------------------------------------------------------------------------
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
 * -------------------------------------------------------------------
 * History
 *   21.02.2018 (Mareike Hoeger): created
 */
package org.knime.bigdata.parquet.utility;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.storage.AbstractTableStoreWriter;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsWO;

/**
 * Will be replaced with Parquet functionality in org.knime.parquet WriteSupport
 * Store Writer for the Parquet file format
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class ParquetTableStoreWriter extends AbstractTableStoreWriter {

    private ParquetWriter<DataRow> m_parquetWriter;

    private final MessageType m_schema;

    private final NodeLogger LOGGER = NodeLogger.getLogger(ParquetTableStoreWriter.class);

    /**
     * Creates a TableStoreWriter that writes a KNIME table into a Parquet file.
     *
     * @param spec the table spec of the table
     * @param writeRowKey Whether the rowKey should be written
     * @param file The target file
     */
    public ParquetTableStoreWriter(final DataTableSpec spec, final boolean writeRowKey, final File file) {
        super(spec, writeRowKey);
        final Configuration conf = new Configuration();
        final List<Type> types = createTypes(spec, writeRowKey);
        m_schema = new MessageType(spec.getName(), types);

        try {
            conf.setAllowNullValueProperties(false);
            conf.set("schema", m_schema.toString());
            conf.set("spec", spec.toString());

            final KNIMEWriteSupport writesupport = new KNIMEWriteSupport(m_schema, spec, writeRowKey);
            writesupport.init(conf);

            m_parquetWriter = new KNIMEParquetWriterBuilder(new Path(file.getAbsolutePath()), writesupport).build();
        } catch (final IOException ex) {
            LOGGER.error("Could not create ParquetWriter.", ex);
        }
    }


    /**
     * Creates a TableStoreWriter that writes a KNIME table into a remote
     * Parquet file.
     *
     * @param spec the table spec of the table
     * @param writeRowKey Whether the rowKey should be written
     * @param filepath The target file path
     * @param compressionCodecName The codec name for the compression
     * @param chunkSize
     * @throws IOException thrown is writer can not be build
     */
    public ParquetTableStoreWriter(final DataTableSpec spec, final boolean writeRowKey,
            final Path filepath, CompressionCodecName compressionCodecName, int chunkSize)
                    throws IOException {
        super(spec, writeRowKey);
        final Configuration conf = new Configuration();
        final List<Type> types = createTypes(spec, writeRowKey);
        m_schema = new MessageType(spec.getName(), types);

        conf.setAllowNullValueProperties(false);
        conf.set("schema", m_schema.toString());
        conf.set("spec", spec.toString());

        final KNIMEWriteSupport writesupport = new KNIMEWriteSupport(m_schema, spec, writeRowKey);

        writesupport.init(conf);
        m_parquetWriter = new KNIMEParquetWriterBuilder(filepath, writesupport)
                .withCompressionCodec(compressionCodecName).withRowGroupSize(chunkSize).build();
    }

    /**
     * @param spec
     * @param writeRowKey
     * @return
     */
    private List<Type> createTypes(final DataTableSpec spec, final boolean writeRowKey) {
        final List<Type> types = new ArrayList<>();
        if (writeRowKey) {
            types.add(new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "rowKey"));
        }

        for (int i = 0; i < spec.getNumColumns(); i++) {

            final Type coltype = getType(spec.getColumnSpec(i));
            types.add(coltype);
        }
        return types;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeRow(final DataRow row) throws IOException {
        m_parquetWriter.write(row);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeMetaInfoAfterWrite(final NodeSettingsWO settings) {
        // No metaData to write
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        m_parquetWriter.close();
    }

    private static Type getType(final DataColumnSpec columnSpec) {

        if (columnSpec.getType() == StringCell.TYPE) {
            return new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, 0, columnSpec.getName(),
                    OriginalType.UTF8, null, null);
        } else if (columnSpec.getType() == IntCell.TYPE) {
            return new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, columnSpec.getName());
        } else if (columnSpec.getType() == DoubleCell.TYPE) {
            return new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, columnSpec.getName());
        } else if (columnSpec.getType() == BooleanCell.TYPE) {
            return new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BOOLEAN, columnSpec.getName());
        } else if (columnSpec.getType() == LongCell.TYPE) {
            return new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, columnSpec.getName());
        }
        return new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, columnSpec.getName());
    }
}
