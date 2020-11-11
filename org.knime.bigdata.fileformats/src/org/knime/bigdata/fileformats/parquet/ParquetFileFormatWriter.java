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
 *   Nov 11, 2020 (Tobias): created
 */
package org.knime.bigdata.fileformats.parquet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.knime.bigdata.fileformats.node.writer2.FileFormatWriter;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetParameter;
import org.knime.bigdata.fileformats.parquet.writer.DataRowWriteSupport;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.bigdata.hadoop.filesystem.NioFileSystemUtil;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.filehandling.core.connections.FSPath;

/**
 * Parquet writer that writes {@link DataRow}s into a parquet file.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public final class ParquetFileFormatWriter implements FileFormatWriter {

    private final ParquetWriter<DataRow> m_writer;

    private final DataTypeMappingConfiguration<?> m_mappingConfig;

    private final ParquetParameter[] m_params;

    private final long m_fileSize;

    /**
     * @param path the {@link FSPath} to write to
     * @param writeMode the write {@link Mode}
     * @param spec the {@link DataTableSpec} of the rows that will be written
     * @param codec the {@link CompressionCodecName}
     * @param fileSize the maximum file size in bytes
     * @param rowGroupSize the row group size in bytes
     * @param typeMappingConfiguration the DataTypeMappingConfiguration to use
     * @throws IOException if the parquet writer can not be initialized
     */
    public ParquetFileFormatWriter(final FSPath path, final Mode writeMode, final DataTableSpec spec,
        final CompressionCodecName codec, final long fileSize, final int rowGroupSize,
        final DataTypeMappingConfiguration<?> typeMappingConfiguration)
        throws IOException {
        m_fileSize = fileSize;
        m_mappingConfig = typeMappingConfiguration;
        m_params = new ParquetParameter[spec.getNumColumns()];
        for (int i = 0; i < spec.getNumColumns(); i++) {
            m_params[i] = new ParquetParameter(i);
        }

        final Configuration hadoopFileSystemConfig = NioFileSystemUtil.getConfiguration();
        final Path hadoopPath = NioFileSystemUtil.getHadoopPath(path, hadoopFileSystemConfig);
        try {
            m_writer = new DataRowParquetWriterBuilder(hadoopPath,
                new DataRowWriteSupport(spec.getName(), spec, m_mappingConfig.getConsumptionPathsFor(spec),
                    m_params)).withCompressionCodec(codec).withDictionaryEncoding(true)
                        .withRowGroupSize(rowGroupSize).withWriteMode(writeMode).build();
        } catch (final InvalidSettingsException e) {
            throw new BigDataFileFormatException(e);
        }
    }

    @Override
    public void close() throws IOException {
        m_writer.close();
    }

    @Override
    public boolean writeRow(final DataRow row) throws IOException {
        m_writer.write(row);
        return m_writer.getDataSize() >= m_fileSize;
    }

    /* Helper class to build ParquetWriters */
    static final class DataRowParquetWriterBuilder
        extends ParquetWriter.Builder<DataRow, ParquetFileFormatWriter.DataRowParquetWriterBuilder> {
        private final WriteSupport<DataRow> m_writeSupport;

        private DataRowParquetWriterBuilder(final Path hadoop, final WriteSupport<DataRow> writeSupport) {
            super(hadoop);
            m_writeSupport = writeSupport;
        }

        @Override
        protected WriteSupport<DataRow> getWriteSupport(final Configuration conf) {
            return m_writeSupport;
        }

        @Override
        protected ParquetFileFormatWriter.DataRowParquetWriterBuilder self() {
            return this;
        }
    }

}