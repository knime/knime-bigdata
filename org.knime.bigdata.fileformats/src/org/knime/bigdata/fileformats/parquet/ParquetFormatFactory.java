/*
 * ------------------------------------------------------------------------
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
 * -------------------------------------------------------------------
 *
 * History 04.06.2018 (Mareike Hoeger): created
 */
package org.knime.bigdata.fileformats.parquet;

import static org.knime.filehandling.core.defaultnodesettings.filechooser.writer.FileOverwritePolicy.OVERWRITE;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.fileformats.node.reader.AbstractFileFormatReader;
import org.knime.bigdata.fileformats.node.writer.AbstractFileFormatWriter;
import org.knime.bigdata.fileformats.node.writer.FileFormatWriter;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetParameter;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetTypeMappingService;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.SettingsModelParquetDataTypeMapping;
import org.knime.bigdata.fileformats.parquet.reader.ParquetKNIMEReader;
import org.knime.bigdata.fileformats.parquet.writer.DataRowWriteSupport;
import org.knime.bigdata.fileformats.parquet.writer.ParquetKNIMEWriter;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.bigdata.fileformats.utility.FileFormatFactory;
import org.knime.bigdata.hadoop.filesystem.NioFileSystemUtil;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.FileOverwritePolicy;
import org.knime.node.datatype.mapping.SettingsModelDataTypeMapping;

/**
 * Factory for Parquet file format.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class ParquetFormatFactory implements FileFormatFactory<ParquetType> {

    private static final String ROW_GROUP = "Row Group";

    private static final String SUFFIX = ".parquet";

    private static final String NAME = "Parquet";

    private static final int TO_BYTE = 1024 * 1024;

    @Override
    public String getChunkSizeUnit() {
        return "MB";
    }

    @Override
    public String[] getCompressionList() {
        return Stream.of(CompressionCodecName.values()).filter(i -> i != CompressionCodecName.LZO).map(Enum::name)
            .toArray(String[]::new);
    }

    @Override
    public String getFilenameSuffix() {
        return SUFFIX;
    }

    @Override
    public Set<FileOverwritePolicy> getSupportedPolicies() {
        return EnumSet.of(FileOverwritePolicy.FAIL, FileOverwritePolicy.OVERWRITE);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public AbstractFileFormatReader getReader(final RemoteFile<Connection> file, final ExecutionContext exec,
        final DataTypeMappingConfiguration<ParquetType> outputDataTypeMappingConfiguration, final boolean useKerberos) {
        try {
            return new ParquetKNIMEReader(file, exec, outputDataTypeMappingConfiguration, useKerberos);
        } catch (final Exception e) {
            throw new BigDataFileFormatException(e);
        }
    }

    @Override
    public SettingsModelDataTypeMapping<ParquetType> getTypeMappingModel(final String key,
        final DataTypeMappingDirection mappingDirection) {
        return new SettingsModelParquetDataTypeMapping(key, mappingDirection);
    }

    @Override
    public DataTypeMappingService<ParquetType, ?, ?> getTypeMappingService() {
        return ParquetTypeMappingService.getInstance();
    }

    @Deprecated
    @Override
    public AbstractFileFormatWriter getWriter(final RemoteFile<Connection> file, final DataTableSpec spec,
        final int chunkSize, final String compression, final DataTypeMappingConfiguration<ParquetType> typeMappingConf)
        throws IOException {
        return new ParquetKNIMEWriter(file, spec, compression, chunkSize * TO_BYTE, typeMappingConf);
    }

    @Override
    public FileFormatWriter getWriter(final FSPath path, final FileOverwritePolicy overwritePolicy,
        final DataTableSpec spec, final int chunkSize, final String compression,
        final DataTypeMappingConfiguration<ParquetType> typeMappingConf) throws IOException {
        final Configuration hadoopFileSystemConfig = NioFileSystemUtil.getConfiguration();
        final Mode writeMode = OVERWRITE == overwritePolicy ? Mode.OVERWRITE : Mode.CREATE;
        return new ParquetFileFormatWriter(NioFileSystemUtil.getHadoopPath(path, hadoopFileSystemConfig), writeMode,
            spec, compression, chunkSize * TO_BYTE, typeMappingConf);
    }

    /* Parquet implemenation for FileFormatWriter */
    static final class ParquetFileFormatWriter implements FileFormatWriter {

        private final ParquetWriter<DataRow> m_writer;

        private final DataTypeMappingConfiguration<?> m_mappingConfig;

        private final ParquetParameter[] m_params;

        public ParquetFileFormatWriter(final Path outputPath, final Mode writeMode, final DataTableSpec spec,
            final String compression, final int rowGroupSize,
            final DataTypeMappingConfiguration<?> inputputDataTypeMappingConfiguration)
            throws IOException {
            final CompressionCodecName codec = CompressionCodecName.fromConf(compression);
            m_mappingConfig = inputputDataTypeMappingConfiguration;
            m_params = new ParquetParameter[spec.getNumColumns()];
            for (int i = 0; i < spec.getNumColumns(); i++) {
                m_params[i] = new ParquetParameter(i);
            }

            try {
                m_writer = new DataRowParquetWriterBuilder(outputPath,
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
        public void writeRow(final DataRow row) throws IOException {
            m_writer.write(row);
        }

        /* Helper class to build ParquetWriters */
        static final class DataRowParquetWriterBuilder
            extends ParquetWriter.Builder<DataRow, DataRowParquetWriterBuilder> {
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
            protected DataRowParquetWriterBuilder self() {
                return this;
            }
        }

    }

    @Override
    public String getChunkUnit() {
        return ROW_GROUP;
    }

}
