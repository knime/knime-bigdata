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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.knime.bigdata.fileformats.filehandling.reader.BigDataReaderConfig;
import org.knime.bigdata.fileformats.filehandling.reader.cell.BigDataCell;
import org.knime.bigdata.fileformats.filehandling.reader.type.KnimeType;
import org.knime.bigdata.hadoop.filesystem.NioFileSystemUtil;
import org.knime.core.node.ExecutionMonitor;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.node.table.reader.TableReader;
import org.knime.filehandling.core.node.table.reader.config.TableReadConfig;
import org.knime.filehandling.core.node.table.reader.spec.TypedReaderTableSpec;

/**
 * Parquet table reader using legacy {@link OriginalType}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
abstract class AbstractParquetTableReader implements TableReader<BigDataReaderConfig, KnimeType, BigDataCell> {

    @SuppressWarnings("resource") // It's the responsibility of the caller to close the read
    @Override
    public ParquetRead read(final FSPath path, final TableReadConfig<BigDataReaderConfig> config) throws IOException {

        final Configuration configuration = NioFileSystemUtil.getConfiguration();
        final org.apache.hadoop.fs.Path hadoopPath = getHadoopPath(path, configuration);
        final long rowCount;

        try (final var reader = ParquetFileReader.open(HadoopInputFile.fromPath(hadoopPath, configuration))) {
            rowCount = reader.getFooter().getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
        }

        final ParquetReader<ParquetRandomAccessible> parquetReader =
            ParquetReader.builder(createReadSupport(config), hadoopPath).withConf(configuration).build();
        return new ParquetRead(parquetReader, rowCount);
    }

    protected abstract AbstractParquetRandomAccessibleReadSupport
        createReadSupport(final TableReadConfig<BigDataReaderConfig> config);

    @Override
    public TypedReaderTableSpec<KnimeType> readSpec(final FSPath path,
        final TableReadConfig<BigDataReaderConfig> config, final ExecutionMonitor exec) throws IOException {
        final MessageType schema = extractSchema(path);
        return convertToSpec(config, schema);
    }

    private static MessageType extractSchema(final FSPath path) throws IOException {
        Configuration configuration = NioFileSystemUtil.getConfiguration();
        final Path hadoopPath = getHadoopPath(path, configuration);
        return extractSchema(configuration, hadoopPath);
    }

    private static MessageType extractSchema(final Configuration configuration, final Path hadoopPath)
        throws IOException {

        try (final var reader = ParquetFileReader.open(HadoopInputFile.fromPath(hadoopPath, configuration))) {
            return reader.getFileMetaData().getSchema();
        }
    }

    protected abstract TypedReaderTableSpec<KnimeType> convertToSpec(final TableReadConfig<BigDataReaderConfig> config,
        final MessageType schema);

    private static Path getHadoopPath(final FSPath path, final Configuration configuration) throws IOException {
        return NioFileSystemUtil.getHadoopPath(path, configuration);
    }

}
