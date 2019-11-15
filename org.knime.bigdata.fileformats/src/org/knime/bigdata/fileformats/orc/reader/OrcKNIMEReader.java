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
 * History 24.04.2018 ("Mareike Hoeger, KNIME GmbH, Konstanz, Germany"): created
 */
package org.knime.bigdata.fileformats.orc.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.Reader.Options;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.commons.hadoop.UserGroupUtil;
import org.knime.bigdata.fileformats.node.reader.AbstractFileFormatReader;
import org.knime.bigdata.fileformats.node.reader.FileFormatReaderInput;
import org.knime.bigdata.fileformats.node.reader.FileFormatRowIterator;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCParameter;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCSource;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.cloud.aws.s3.filehandler.S3Connection;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.convert.map.ExternalToKnimeMapper;
import org.knime.core.data.convert.map.MappingFramework;
import org.knime.core.data.convert.map.ProductionPath;
import org.knime.core.data.convert.map.Source.ProducerParameters;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.NodeLogger;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.ExternalDataColumnSpec;
import org.knime.datatype.mapping.ExternalDataTableSpec;

/**
 * Reader that reads ORC files into a KNIME data table.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class OrcKNIMEReader extends AbstractFileFormatReader {

    class OrcRowIterator extends FileFormatRowIterator {

        private final VectorizedRowBatch m_rowBatch;

        private final RecordReader m_rows;

        private boolean m_hastNext;

        private final Reader m_orcReader;

        private long m_index;

        private ORCSource m_source;

        private final RemoteFile<Connection> m_tempInputFile;

        /**
         * Row iterator for DataRows read from a ORC file.
         *
         * @param reader the ORC Reader
         * @param batchSize the batch size for reading
         * @param tempInputFile temporary input file that should be removed on close
         * @throws IOException if file can not be read
         */
        OrcRowIterator(final Reader reader, final long index, final RemoteFile<Connection> tempInputFile)
                throws IOException {
            m_orcReader = reader;
            final Options options = m_orcReader.options();
            m_rows = m_orcReader.rows(options);
            m_rowBatch = m_orcReader.getSchema().createRowBatch();
            m_source = new ORCSource(m_rowBatch);
            m_index = index;
            m_tempInputFile = tempInputFile;
            internalNext();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() {
            try {
                m_rows.close();
            } catch (final IOException e) {
                LOGGER.info("Could not close iterator.", e);
            } finally {
                removeTempInputFile();
            }
            m_hastNext = false;
        }

        private void removeTempInputFile() {
            if (m_tempInputFile != null) {
                try {
                    m_tempInputFile.delete();
                } catch (Exception e) {
                    throw new BigDataFileFormatException("Could not delete temporary file: " + e.getMessage(), e);
                }
            }
        }

        @Override
        public long getIndex() {
            return m_index;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return m_hastNext;
        }

        private void internalNext() throws IOException {
            getExec().setProgress(String.format("Read %d rows", m_index));
            if (m_rows.nextBatch(m_rowBatch)) {
                m_source = new ORCSource(m_rowBatch);
                m_hastNext = true;
            } else {
                m_hastNext = false;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            DataRow safeRow;
            try {
                safeRow = m_externalToKnimeMapper.map(RowKey.createRowKey(m_index), m_source, m_params);
            } catch (final Exception e1) {
                throw new BigDataFileFormatException(e1);
            }
            m_index++;
            try {
                getExec().checkCanceled();
            } catch (final CanceledExecutionException ex) {
                throw new BigDataFileFormatException(ex);
            }
            m_source.next();

            if (m_source.getRowIndex() >= m_rowBatch.size) {
                try {
                    internalNext();
                } catch (final IOException e) {
                    throw new BigDataFileFormatException(e);
                }
            }
            return safeRow;
        }
    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(OrcKNIMEReader.class);

    private final Queue<FileFormatReaderInput<Reader>> m_readers;

    private ProducerParameters<ORCSource>[] m_params;

    private final DataTypeMappingConfiguration<TypeDescription> m_outputTypeMappingConf;

    private ExternalToKnimeMapper<ORCSource, ProducerParameters<ORCSource>> m_externalToKnimeMapper;

    /**
     * KNIME Reader, that reads ORC Files into DataRows.
     *
     * @param file the file or directory to read from
     * @param outputDataTypeMappingConfiguration the type mapping configuration
     * @param exec the execution context
     * @param useKerberos
     * @throws Exception thrown if files can not be listed, if reader can not be created, or schemas of files in a
     *             directory do not match.
     */
    @SuppressWarnings("unchecked")
    public OrcKNIMEReader(final RemoteFile<Connection> file,
        final DataTypeMappingConfiguration<?> outputDataTypeMappingConfiguration, final ExecutionContext exec,
        final boolean useKerberos) throws Exception {
        super(file, exec, useKerberos);
        m_outputTypeMappingConf = (DataTypeMappingConfiguration<TypeDescription>)outputDataTypeMappingConfiguration;
        m_readers = new ArrayDeque<>();
        init();
        if (m_readers.isEmpty()) {
            throw new BigDataFileFormatException("Could not Create Reader");
        }

    }

    private static ProducerParameters<ORCSource>[]
        createDefault(final ExternalDataTableSpec<TypeDescription> exTableSpec) {
        final ORCParameter[] params = new ORCParameter[exTableSpec.getColumns().size()];
        for (int i = 0; i < exTableSpec.getColumns().size(); i++) {
            params[i] = new ORCParameter(i);
        }
        return params;
    }

    private FileFormatReaderInput<Reader> createORCReader(final RemoteFile<Connection> remotefile) throws Exception {
        final Configuration conf = createConfig(remotefile);
        Path readPath = new Path(remotefile.getURI());
        Reader reader = null;
        RemoteFile<Connection> tempFile = null;

        if (getFile().getConnection() instanceof S3Connection) {
            final CloudRemoteFile<Connection> cloudcon = (CloudRemoteFile<Connection>)remotefile;
            readPath = generateS3nPath(cloudcon);
        } else if (readFromLocalCopy(remotefile)) {
            tempFile = downloadLocalCopy(remotefile);
            readPath = new Path(tempFile.getURI());
        }

        final Path path = readPath;
        if (useKerberos()) {
            reader = UserGroupUtil.runWithProxyUserUGIIfNecessary((ugi) -> ugi.doAs(
                (PrivilegedExceptionAction<Reader>)() -> OrcFile.createReader(path, OrcFile.readerOptions(conf))));
        } else {
            reader = OrcFile.createReader(readPath, OrcFile.readerOptions(conf));
        }

        return new FileFormatReaderInput<>(reader, tempFile);
    }

    @Override
    protected void createReader(final List<DataTableSpec> schemas, final RemoteFile<Connection> remotefile) {
        try {
            final FileFormatReaderInput<Reader> readerInput = createORCReader(remotefile);
            m_readers.add(readerInput);
            final ExecutionContext exec = getExec();
            if (exec != null) {
                exec.setProgress("Retrieving schema for file " + remotefile.getName());
                exec.checkCanceled();
            }
            schemas.add(createSpecFromOrcSchema(readerInput.getReader().getSchema()));
        } catch (final Exception e) {
            throw new BigDataFileFormatException(e);
        }
    }

    private DataTableSpec createSpecFromOrcSchema(final TypeDescription orcSchema) throws Exception {
        final ArrayList<DataType> colTypes = new ArrayList<>();
        final List<ExternalDataColumnSpec<TypeDescription>> columns = new ArrayList<>(orcSchema.getChildren().size());
        for (int i = 0; i < orcSchema.getChildren().size(); i++) {
            columns.add(new ExternalDataColumnSpec<>(orcSchema.getFieldNames().get(i), orcSchema.getChildren().get(i)));
        }
        final ExternalDataTableSpec<TypeDescription> exTableSpec = new ExternalDataTableSpec<>(columns);
        setExternalTableSpec(exTableSpec);
        final ProductionPath[] paths = m_outputTypeMappingConf.getProductionPathsFor(exTableSpec);
        m_params = createDefault(exTableSpec);
        m_externalToKnimeMapper = MappingFramework.createMapper(i -> getFileStoreFactory(), paths);

        for (final ProductionPath path : paths) {
            final DataType dataType = path.getConverterFactory().getDestinationType();
            colTypes.add(dataType);
        }
        final List<String> fieldNames = orcSchema.getFieldNames();
        return new DataTableSpec(getFile().getName(), fieldNames.toArray(new String[fieldNames.size()]),
            colTypes.toArray(new DataType[colTypes.size()]));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileFormatRowIterator getNextIterator(final long i) throws IOException {
        final FileFormatReaderInput<Reader> readerInput = m_readers.poll();
        FileFormatRowIterator rowIterator = null;
        if (readerInput != null) {
            rowIterator =
                new OrcRowIterator(readerInput.getReader(), i, readerInput.getTempInputFile());
        }
        return rowIterator;
    }

    /**
     * Loads metadata including the tableSpec from the file before reading starts.
     *
     * @param reader the ORC reader
     * @throws Exception thrown if types cannot be detected
     */
    public void loadMetaInfoBeforeRead(final Reader reader) throws Exception {
        final TypeDescription orcSchema = reader.getSchema();

        if (!tyrCreateFromAdditonalMetadata(orcSchema, reader)) {
            // No KNIME metadata in file. Create from ORC schema.
            createSpecFromOrcSchema(orcSchema);
        }
    }

    private boolean tyrCreateFromAdditonalMetadata(final TypeDescription orcSchema, final Reader reader)
        throws Exception {
        final ArrayList<String> columnNames = new ArrayList<>();
        final ArrayList<DataType> columnTypes = new ArrayList<>();
        final List<String> keys = reader.getMetadataKeys();
        String name = getFile().getName();

        for (final String key : keys) {
            if (key.startsWith("knime_tablename.")) {
                name = key.substring(key.indexOf('.'));
            } else if (key.startsWith("knime.")) {
                columnNames.add(key.substring(key.indexOf('.')));

                final ByteBuffer columntype = reader.getMetadataValue(key);
                final String typeClassString = String.valueOf(columntype.asCharBuffer());
                @SuppressWarnings("unchecked")
                final DataType type = DataType.getType((Class<? extends DataCell>)Class.forName(typeClassString));
                columnTypes.add(type);
            }
        }
        if (columnNames.size() == orcSchema.getFieldNames().size()) {
            setTableSpec(
                new DataTableSpec(name, columnNames.toArray(new String[0]), columnTypes.toArray(new DataType[0])));
            return true;
        }
        return false;
    }

}
