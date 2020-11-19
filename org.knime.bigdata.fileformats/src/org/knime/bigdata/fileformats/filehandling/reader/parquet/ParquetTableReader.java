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
 *   Sep 24, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.knime.bigdata.fileformats.filehandling.reader.BigDataReaderConfig;
import org.knime.bigdata.fileformats.filehandling.reader.cell.BigDataCell;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeType;
import org.knime.bigdata.hadoop.filesystem.NioFileSystemUtil;
import org.knime.core.node.ExecutionMonitor;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.node.table.reader.TableReader;
import org.knime.filehandling.core.node.table.reader.config.TableReadConfig;
import org.knime.filehandling.core.node.table.reader.read.Read;
import org.knime.filehandling.core.node.table.reader.spec.TypedReaderTableSpec;
import org.knime.filehandling.core.node.table.reader.spec.TypedReaderTableSpec.TypedReaderTableSpecBuilder;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ParquetTableReader implements TableReader<BigDataReaderConfig, KnimeType, BigDataCell> {

    @SuppressWarnings("resource") // It's the responsibility of the caller to close the read
    @Override
    public Read<BigDataCell> read(final Path path, final TableReadConfig<BigDataReaderConfig> config)
        throws IOException {

        final Configuration configuration = NioFileSystemUtil.getConfiguration();
        final org.apache.hadoop.fs.Path hadoopPath = getHadoopPath(path, configuration);

        final ParquetMetadata meta =
            ParquetFileReader.readFooter(configuration, hadoopPath, ParquetMetadataConverter.NO_FILTER);
        final long rowCount = meta.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();

        final ParquetRandomAccessibleReadSupport readSupport = new ParquetRandomAccessibleReadSupport();
        final ParquetReader<ParquetRandomAccessible> parquetReader =
            ParquetReader.builder(readSupport, hadoopPath).withConf(configuration).build();
        return new ParquetRead(path, parquetReader, rowCount);
    }

    @Override
    public TypedReaderTableSpec<KnimeType> readSpec(final Path path, final TableReadConfig<BigDataReaderConfig> config,
        final ExecutionMonitor exec) throws IOException {
        final MessageType schema = extractSchema(path);
        return convertToSpec(schema);
    }

    private static MessageType extractSchema(final Path path) throws IOException {
        Configuration configuration = NioFileSystemUtil.getConfiguration();
        final org.apache.hadoop.fs.Path hadoopPath = getHadoopPath(path, configuration);
        return extractSchema(configuration, hadoopPath);
    }

    private static MessageType extractSchema(final Configuration configuration,
        final org.apache.hadoop.fs.Path hadoopPath) throws IOException {
        final ParquetMetadata meta =
            ParquetFileReader.readFooter(configuration, hadoopPath, ParquetMetadataConverter.NO_FILTER);
        final FileMetaData fileMetaData = meta.getFileMetaData();
        return fileMetaData.getSchema();
    }

    private static TypedReaderTableSpec<KnimeType> convertToSpec(final MessageType schema) {
        final TypedReaderTableSpecBuilder<KnimeType> specBuilder = new TypedReaderTableSpecBuilder<>();
        for (Type field : schema.getFields()) {
            final KnimeType type = ParquetKnimeTypeFactory.fromType(field);
            specBuilder.addColumn(field.getName(), type, true);
        }
        return specBuilder.build();
    }

    private static org.apache.hadoop.fs.Path getHadoopPath(final Path path, final Configuration configuration)
        throws IOException {
        return NioFileSystemUtil.getHadoopPath((FSPath)path, configuration);
    }

}
