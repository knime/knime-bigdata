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
 * History 28.05.2018 (Mareike Hoeger): created
 */
package org.knime.bigdata.fileformats.parquet.reader;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.fileformats.node.reader.AbstractFileFormatReader;
import org.knime.bigdata.fileformats.node.reader.FileFormatRowIterator;
import org.knime.bigdata.fileformats.parquet.ParquetTableStoreFormat;
import org.knime.bigdata.fileformats.parquet.type.ParquetType;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.cloud.aws.s3.filehandler.S3Connection;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;

/**
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class ParquetKNIMEReader extends AbstractFileFormatReader {

    private final Queue<ParquetReader<DataRow>> m_readers;

    // Use a map for now, this will be changed once the type conversion
    // framework is used
    private final Map<PrimitiveTypeName, DataType> m_types = new EnumMap<>(PrimitiveTypeName.class);

    {
        m_types.put(PrimitiveTypeName.INT64, LongCell.TYPE);
        m_types.put(PrimitiveTypeName.INT32, IntCell.TYPE);
        m_types.put(PrimitiveTypeName.DOUBLE, DoubleCell.TYPE);
        m_types.put(PrimitiveTypeName.BOOLEAN, BooleanCell.TYPE);
        m_types.put(PrimitiveTypeName.BINARY, StringCell.TYPE);
    }

    /**
     * Reader that reads Parquet files into a KNIME data table.
     *
     * @param file the file or directory to read from
     * @param exec the execution context
     * @throws Exception thrown if files can not be listed, if reader can not be
     *         created, or schemas of files in a directory do not match.
     */
    public ParquetKNIMEReader(final RemoteFile<Connection> file, final ExecutionContext exec) throws Exception {
        super(file, exec);
        m_readers = new ArrayDeque<>();
        init();
        if (m_readers.isEmpty()) {
            throw new BigDataFileFormatException("Could not create reader");
        }
    }

    /* (non-Javadoc)
     * @see org.knime.bigdata.fileformats.node.reader.AbstractFileFormatReader#createReader(org.knime.core.node.ExecutionContext, java.util.List, org.knime.base.filehandling.remote.files.RemoteFile)
     */
    @Override
    protected void createReader(final ExecutionContext exec, final List<DataTableSpec> schemas, final RemoteFile<Connection> remoteFile) {

        final Configuration conf = new Configuration();
        try {
            createConfig(remoteFile, conf);
            Path path = new Path(remoteFile.getURI());
            if (getFile().getConnection() instanceof S3Connection) {
                final CloudRemoteFile<Connection> cloudcon = (CloudRemoteFile<Connection>) remoteFile;
                path = generateS3nPath(cloudcon);
            }
            final DataTableSpec tableSpec = createTableSpec(path, conf);
            schemas.add(tableSpec);
            final ParquetType[] columnTypes = ParquetTableStoreFormat.parquetTypesFromSpec(tableSpec);

            @SuppressWarnings("resource")
            final ParquetReader<DataRow> reader = ParquetReader
                    .builder(new DataRowReadSupport(columnTypes), path).withConf(conf).build();
            m_readers.add(reader);
        } catch (InvalidKeyException | BadPaddingException | IllegalBlockSizeException | IOException e) {
            throw new BigDataFileFormatException(e);
        }
    }

    private DataTableSpec createTableSpec(final Path path, final Configuration conf) throws IOException {
        final ParquetMetadata footer = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
        final FileMetaData fileMetaData = footer.getFileMetaData();
        final MessageType schema = fileMetaData.getSchema();
        final String tableName = schema.getName();
        final ArrayList<String> columnNames = new ArrayList<>();
        final ArrayList<DataType> columnTypes = new ArrayList<>();
        for (final Type field : schema.getFields()) {
            if (!field.isPrimitive()) {
                throw new BigDataFileFormatException(
                        String.format("Non Primitve type %s is not yet supported.", field.getOriginalType()));
            }
            final DataType type = m_types.get(field.asPrimitiveType().getPrimitiveTypeName());
            if (type == null) {
                throw new BigDataFileFormatException(String.format("Type %s is not yet supported.", field));
            }
            columnNames.add(field.getName());
            columnTypes.add(type);

        }
        return new DataTableSpec(tableName, columnNames.toArray(new String[columnNames.size()]),
                columnTypes.toArray(new DataType[columnTypes.size()]));
    }

    @SuppressWarnings("resource")
    @Override
    public FileFormatRowIterator getNextIterator(final long i) throws IOException, InterruptedException {
        // Parquet inits the FS only during read() so we need another doAS here
        final ParquetReader<DataRow> reader = m_readers.poll();
        FileFormatRowIterator iterator = null;
        if (reader != null) {
            if (getUser() != null) {
                iterator = getUser().doAs(new PrivilegedExceptionAction<FileFormatRowIterator>() {
                    @Override
                    public FileFormatRowIterator run() throws Exception {
                        return new ParquetRowIterator(i, reader);
                    }
                });
            } else {
                iterator = new ParquetRowIterator(i, reader);
            }
        }
        return iterator;

    }

}
