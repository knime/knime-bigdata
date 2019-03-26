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
import java.security.PrivilegedExceptionAction;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.commons.hadoop.UserGroupUtil;
import org.knime.bigdata.fileformats.node.reader.AbstractFileFormatReader;
import org.knime.bigdata.fileformats.node.reader.FileFormatRowIterator;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetParameter;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetSource;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.cloud.aws.s3.filehandler.S3Connection;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.convert.map.ProductionPath;
import org.knime.core.data.convert.map.Source.ProducerParameters;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.ExternalDataColumnSpec;
import org.knime.datatype.mapping.ExternalDataTableSpec;

/**
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class ParquetKNIMEReader extends AbstractFileFormatReader {

    private final Queue<ParquetReader<DataRow>> m_readers;

    private final Map<OriginalType, DataType> m_orgTypes = new EnumMap<>(OriginalType.class);

    private final DataTypeMappingConfiguration<ParquetType> m_outputTypeMappingConf;

    private ProductionPath[] m_paths;

    private ProducerParameters<ParquetSource>[] m_params;

    {
        m_orgTypes.put(OriginalType.UTF8, StringCell.TYPE);
    }

    /**
     * Reader that reads Parquet files into a KNIME data table.
     *
     * @param file the file or directory to read from
     * @param exec the execution context
     * @param outputDataTypeMappingConfiguration the type mapping configuration
     * @param useKerberos whether to use kerberos for authentication
     * @throws Exception thrown if files can not be listed, if reader can not be created, or schemas of files in a
     *             directory do not match.
     */
    @SuppressWarnings("unchecked")
    public ParquetKNIMEReader(final RemoteFile<Connection> file, final ExecutionContext exec,
        final DataTypeMappingConfiguration<?> outputDataTypeMappingConfiguration, final boolean useKerberos)
        throws Exception {
        super(file, exec, useKerberos);
        m_outputTypeMappingConf = (DataTypeMappingConfiguration<ParquetType>)outputDataTypeMappingConfiguration;
        m_readers = new ArrayDeque<>();
        init();
        if (m_readers.isEmpty()) {
            throw new BigDataFileFormatException("Could not create reader");
        }
    }

    private static ProducerParameters<ParquetSource>[]
        createDefault(final ExternalDataTableSpec<ParquetType> exTableSpec) {
        final ParquetParameter[] params = new ParquetParameter[exTableSpec.getColumns().size()];
        for (int i = 0; i < exTableSpec.getColumns().size(); i++) {
            params[i] = new ParquetParameter(i);
        }
        return params;
    }

    /* (non-Javadoc)
     * @see org.knime.bigdata.fileformats.node.reader.AbstractFileFormatReader#createReader(org.knime.core.node.ExecutionContext, java.util.List, org.knime.base.filehandling.remote.files.RemoteFile)
     */
    @SuppressWarnings("resource")
    @Override
    protected void createReader(final List<DataTableSpec> schemas, final RemoteFile<Connection> remoteFile) {

        try {
            final Configuration conf = createConfig(remoteFile);
            Path path = new Path(remoteFile.getURI());
            if (getFile().getConnection() instanceof S3Connection) {
                final CloudRemoteFile<Connection> cloudcon = (CloudRemoteFile<Connection>)remoteFile;
                path = generateS3nPath(cloudcon);
            }
            final DataTableSpec tableSpec = createTableSpec(path, conf);
            schemas.add(tableSpec);

            final Path readerPath = path;

            ParquetReader<DataRow> reader;

            if (useKerberos()) {
                reader = UserGroupUtil.runWithProxyUserUGIIfNecessary(
                    (ugi) -> ugi.doAs((PrivilegedExceptionAction<ParquetReader<DataRow>>)() -> ParquetReader
                        .builder(new DataRowReadSupport(getFileStoreFactory(), m_paths, m_params), readerPath)
                        .withConf(conf).build()));
            } else {
                reader =
                    ParquetReader.builder(new DataRowReadSupport(getFileStoreFactory(), m_paths, m_params), readerPath)
                        .withConf(conf).build();
            }

            m_readers.add(reader);
        } catch (final IOException ioe) {
            if (ioe.getMessage().contains("No FileSystem for scheme")) {
                throw new BigDataFileFormatException(
                    "Protocol " + remoteFile.getConnectionInformation().getProtocol() + " is not supported.");
            }
            throw new BigDataFileFormatException(ioe);
        } catch (final Exception e) {
            throw new BigDataFileFormatException(e);
        }
    }

    private DataTableSpec createTableSpec(final Path path, final Configuration conf) throws Exception {
        ParquetMetadata footer;
        if (useKerberos()) {
            footer = UserGroupUtil.runWithProxyUserUGIIfNecessary(
                (ugi) -> ugi.doAs((PrivilegedExceptionAction<ParquetMetadata>)() -> ParquetFileReader.readFooter(conf,
                    path, ParquetMetadataConverter.NO_FILTER)));
        } else {
            footer = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
        }
        final FileMetaData fileMetaData = footer.getFileMetaData();
        final MessageType schema = fileMetaData.getSchema();
        final List<ExternalDataColumnSpec<ParquetType>> columns = new ArrayList<>(schema.getFields().size());
        for (final Type field : schema.getFields()) {

            if (field.isPrimitive()) {
                columns.add(new ExternalDataColumnSpec<>(field.getName(),
                    new ParquetType(field.asPrimitiveType().getPrimitiveTypeName(), field.getOriginalType())));
            } else {
                if (field.getOriginalType() == OriginalType.LIST) {
                    Type subtype = field.asGroupType().getType(0).asGroupType().getType(0);
                    ParquetType element =
                        new ParquetType(subtype.asPrimitiveType().getPrimitiveTypeName(), subtype.getOriginalType());
                    columns.add(new ExternalDataColumnSpec<>(field.getName(), ParquetType.createListType(element)));
                } else {
                    throw new BigDataFileFormatException("Only Supported GroupType is LIST");
                }
            }
        }
        final ExternalDataTableSpec<ParquetType> exTableSpec = new ExternalDataTableSpec<>(columns);
        setexternalTableSpec(exTableSpec);
        m_paths = m_outputTypeMappingConf.getProductionPathsFor(exTableSpec);
        m_params = createDefault(exTableSpec);
        final ArrayList<DataType> colTypes = new ArrayList<>();
        for (final ProductionPath prodPath : m_paths) {
            final DataType dataType = prodPath.getConverterFactory().getDestinationType();
            colTypes.add(dataType);
        }

        final String[] fieldNames = schema.getFields().stream().map(Type::getName).toArray(String[]::new);
        return new DataTableSpec(getFile().getName(), fieldNames, colTypes.toArray(new DataType[colTypes.size()]));
    }

    @SuppressWarnings("resource")
    @Override
    public FileFormatRowIterator getNextIterator(final long i) throws Exception {
        // Parquet inits the FS only during read() so we need the doAS here
        final ParquetReader<DataRow> reader = m_readers.poll();
        FileFormatRowIterator iterator = null;
        if (reader != null) {
            if (useKerberos()) {
                iterator = UserGroupUtil.runWithProxyUserUGIIfNecessary((ugi) -> ugi
                    .doAs((PrivilegedExceptionAction<FileFormatRowIterator>)() -> new ParquetRowIterator(i, reader)));
            } else {
                iterator = new ParquetRowIterator(i, reader);
            }
        }
        return iterator;

    }

}
