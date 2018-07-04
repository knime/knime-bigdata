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
import java.security.InvalidKeyException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.Reader.Options;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.fileformats.node.reader.AbstractFileFormatReader;
import org.knime.bigdata.fileformats.node.reader.FileFormatRowIterator;
import org.knime.bigdata.fileformats.orc.OrcTableStoreFormat;
import org.knime.bigdata.fileformats.orc.types.OrcType;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.cloud.aws.s3.filehandler.S3Connection;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.container.BlobSupportDataRow;
import org.knime.core.data.def.DefaultCellIterator;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.NodeLogger;

/**
 * Reader that reads ORC files into a KNIME data table.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class OrcKNIMEReader extends AbstractFileFormatReader {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(OrcKNIMEReader.class);

    private OrcType<?>[] m_columnReaders;

    private final Queue<Reader> m_readers;

    /**
     * KNIME Reader, that reads ORC Files into DataRows.
     *
     * @param file the file or directory to read from
     * @param exec the execution context
     * @throws Exception thrown if files can not be listed, if reader can not be
     *         created, or schemas of files in a directory do not match.
     */
    public OrcKNIMEReader(final RemoteFile<Connection> file, final ExecutionContext exec) throws Exception {
        super(file, exec);
        m_readers = new ArrayDeque<>();
        init();
        if (m_readers.isEmpty()) {
            throw new BigDataFileFormatException("Could not Create Reader");
        }
        if (m_columnReaders == null) {
            throw new BigDataFileFormatException(
                    "No information for type deserialization loaded. Most likely an implementation error.");
        }
    }

    @Override
    protected void createReader(final ExecutionContext exec, final List<DataTableSpec> schemas,
            final RemoteFile<Connection> remotefile) {
        try {
            final Reader reader = createORCReader(remotefile);
            m_readers.add(reader);
            if (exec != null) {
                exec.setProgress("Retrieving schema for file " + remotefile.getName());
                exec.checkCanceled();
            }
            schemas.add(createSpecFromOrcSchema(reader.getSchema()));

        } catch (final IOException ioe) {
            if (ioe.getMessage().contains("No FileSystem for scheme")) {
                throw new BigDataFileFormatException(
                        "Protocol " + remotefile.getConnectionInformation().getProtocol() + " is not supported.");
            }
            throw new BigDataFileFormatException(ioe);
        } catch (final Exception e) {
            throw new BigDataFileFormatException(e);
        }
    }

    private Reader createORCReader(final RemoteFile<Connection> remotefile)
            throws InvalidKeyException, BadPaddingException, IllegalBlockSizeException, IOException {
        final Configuration conf = new Configuration();
        createConfig(remotefile, conf);
        Path readPath = new Path(remotefile.getURI());
        Reader reader = null;

        if (getFile().getConnection() instanceof S3Connection) {
            final CloudRemoteFile<Connection> cloudcon = (CloudRemoteFile<Connection>) remotefile;
            readPath = generateS3nPath(cloudcon);
        }

        reader = OrcFile.createReader(readPath, OrcFile.readerOptions(conf));

        return reader;
    }

    /**
     * Loads metadata including the tableSpec from the file before reading
     * starts.
     *
     * @param reader the ORC reader
     * @throws Exception thrown if types cannot be detected
     */
    public void loadMetaInfoBeforeRead(final Reader reader) throws Exception {
        final TypeDescription orcSchema = reader.getSchema();
        final ArrayList<OrcType<?>> orcTypes = new ArrayList<>();

        if (!tyrCreateFromAdditonalMetadata(orcSchema, reader, orcTypes)) {
            // No KNIME metadata in file. Create from ORC schema.
            createSpecFromOrcSchema(orcSchema);
        }
    }

    private boolean tyrCreateFromAdditonalMetadata(final TypeDescription orcSchema, final Reader reader,
            final ArrayList<OrcType<?>> orcTypes) throws Exception {
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
                final DataType type = DataType.getType((Class<? extends DataCell>) Class.forName(typeClassString));
                columnTypes.add(type);
                orcTypes.add(OrcTableStoreFormat.createOrcType(type));
            }
        }
        if (columnNames.size() == orcSchema.getFieldNames().size()) {
            m_columnReaders = orcTypes.toArray(new OrcType[orcTypes.size()]);
            setTableSpec(
                    new DataTableSpec(name, columnNames.toArray(new String[0]), columnTypes.toArray(new DataType[0])));
            return true;
        }
        return false;
    }

    private DataTableSpec createSpecFromOrcSchema(final TypeDescription orcSchema) throws Exception {
        ArrayList<OrcType<?>> orcTypes;
        final ArrayList<DataType> colTypes = new ArrayList<>();
        orcTypes = new ArrayList<>();

        for (final TypeDescription type : orcSchema.getChildren()) {
            final Category category = type.getCategory();
            DataType dataType;
            switch (category) {
            case LIST:
                final DataType subtype = OrcTableStoreFormat.getDataType(type.getChildren().get(0).getCategory());
                dataType = DataType.getType(ListCell.class, subtype);
                break;
            case MAP:
                throw new OrcReadException("MAP is not supported yet");
            case UNION:
                throw new OrcReadException("UNION is not supported yet");
            case STRUCT:
                throw new OrcReadException("STRUCT is not supported yet");
            default:
                dataType = OrcTableStoreFormat.getDataType(category);
            }
            colTypes.add(dataType);
            orcTypes.add(OrcTableStoreFormat.createOrcType(dataType));
        }
        m_columnReaders = orcTypes.toArray(new OrcType[orcTypes.size()]);
        final List<String> fieldNames = orcSchema.getFieldNames();
        return new DataTableSpec(getFile().getName(), fieldNames.toArray(new String[fieldNames.size()]),
                colTypes.toArray(new DataType[colTypes.size()]));
    }

    class OrcRowIterator extends FileFormatRowIterator {

        private final VectorizedRowBatch m_rowBatch;

        private final RecordReader m_rows;

        private int m_rowInBatch;

        private final OrcType<?>[] m_orcTypeReaders;

        private final Reader m_orcReader;

        private long m_index;

        /**
         * Row iterator for DataRows read from a ORC file.
         *
         * @param reader the ORC Reader
         * @param batchSize the batch size for reading
         * @throws IOException if file can not be read
         */
        OrcRowIterator(final Reader reader, final OrcType<?>[] columnReaders, final long index) throws IOException {
            m_orcReader = reader;
            final Options options = m_orcReader.options();
            m_rows = m_orcReader.rows(options);
            m_rowBatch = m_orcReader.getSchema().createRowBatch();
            m_orcTypeReaders = columnReaders.clone();
            m_index = index;
            internalNext();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return m_rowInBatch >= 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            // this is fragile as updates to the batch make it invalid
            final OrcRow orcRow = new OrcRow(this, m_rowInBatch, m_index);
            try {
                getExec().checkCanceled();
            } catch (final CanceledExecutionException ex) {
                throw new OrcReadException(ex);
            }
            m_index++;
            final DataRow safeRow = new BlobSupportDataRow(orcRow.getKey(), orcRow);
            m_rowInBatch += 1;
            if (m_rowInBatch >= m_rowBatch.size) {
                try {
                    internalNext();
                } catch (final IOException e) {
                    throw new OrcReadException(e);
                }
            }
            return safeRow;
        }

        private void internalNext() throws IOException {
            getExec().setProgress(String.format("Read %d rows", m_index));
            if (m_rows.nextBatch(m_rowBatch)) {
                m_rowInBatch = 0;
            } else {
                m_rowInBatch = -1;
            }
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
            }
            m_rowInBatch = -1;
        }

        @Override
        public long getIndex() {
            return m_index;
        }
    }

    /**
     * The KNIME DataRow wrapping the VectorizedBatch, values are read lazy so a
     * reset to the batch will make this row invalid -- caller needs to cache
     * data first.
     */
    static final class OrcRow implements DataRow {

        private final OrcRowIterator m_iterator;

        private final int m_rowInBatch;

        private final long m_index;

        /**
         * Creates an ORC Row.
         *
         * @param iterator the row iterator
         * @param rowInBatch the number of the row in the batch
         * @param index the index of the row in the resulting data table
         */
        OrcRow(final OrcRowIterator iterator, final int rowInBatch, final long index) {
            m_iterator = iterator;
            m_rowInBatch = rowInBatch;
            m_index = index;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RowKey getKey() {
            return RowKey.createRowKey(m_index);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataCell getCell(final int index) {
            @SuppressWarnings("unchecked")
            final OrcType<ColumnVector> orcType = (OrcType<ColumnVector>) m_iterator.m_orcTypeReaders[index];
            return orcType.readValue(m_iterator.m_rowBatch.cols[index], m_rowInBatch);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getNumCells() {
            return m_iterator.m_orcTypeReaders.length;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Iterator<DataCell> iterator() {
            return new DefaultCellIterator(this);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileFormatRowIterator getNextIterator(final long i) throws IOException {
        final Reader reader = m_readers.poll();
        FileFormatRowIterator rowIterator = null;
        if (reader != null) {
            rowIterator = new OrcRowIterator(reader, m_columnReaders, i);
        }
        return rowIterator;
    }

}
