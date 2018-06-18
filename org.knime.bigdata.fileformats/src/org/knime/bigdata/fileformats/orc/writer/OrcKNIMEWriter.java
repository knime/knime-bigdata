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
 * History 23.04.2018 ("Mareike Höger, KNIME"): created
 */
package org.knime.bigdata.fileformats.orc.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.WriterOptions;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.fileformats.node.writer.AbstractFileFormatWriter;
import org.knime.bigdata.fileformats.orc.OrcTableStoreFormat;
import org.knime.bigdata.fileformats.orc.types.OrcStringTypeFactory.OrcStringType;
import org.knime.bigdata.fileformats.orc.types.OrcType;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.util.Pair;

/**
 * ORC Writer for KNIME {@link DataRow}. It holds the rows in batches and hands
 * them to the ORC writer once the maximum batch size is reached.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class OrcKNIMEWriter extends AbstractFileFormatWriter {

    private static final String ROW_KEY = "row-key";

    private List<Pair<String, OrcType<?>>> m_fields;

    private Writer m_writer;

    private VectorizedRowBatch m_rowBatch;

    private final OrcType<?>[] m_columnTypes;

    private final CompressionKind m_compression;

    /**
     * Constructor for ORC writer, that writes KNIME {@link DataRow}s to an ORC
     * file
     *
     * @param file the target file
     * @param spec the data table spec
     * @param isWriteRowKey whether the key should be written
     * @param batchSize size of the batch
     * @param compression the compression to use for writing
     * @throws IOException if writer cannot be initialized
     */
    public OrcKNIMEWriter(final RemoteFile<Connection> file, final DataTableSpec spec, final boolean isWriteRowKey,
            final int batchSize, final String compression) throws IOException {
        super(file, batchSize, isWriteRowKey, spec);
        m_compression = CompressionKind.valueOf(compression);
        initWriter();
        m_columnTypes = getOrcKNIMETypes();
    }

    private void initWriter() throws IOException {
        m_fields = new ArrayList<>();
        if (isWriteRowKey()) {
            m_fields.add(new Pair<>(ROW_KEY, OrcTableStoreFormat.createOrcType(StringCell.TYPE)));
        }
        final DataTableSpec tableSpec = getTableSpec();
        for (int i = 0; i < tableSpec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = tableSpec.getColumnSpec(i);
            // final String name = colSpec.getName().replaceAll("[^a-zA-Z]",
            // "-");
            m_fields.add(new Pair<>(colSpec.getName(), OrcTableStoreFormat.createOrcType(colSpec.getType())));

        }
        final Configuration conf = new Configuration();
        final TypeDescription schema = deriveTypeDescription();
        final WriterOptions orcConf = OrcFile.writerOptions(conf).setSchema(schema).compress(m_compression)
                .version(OrcFile.Version.CURRENT);
        if (getBatchSize() > -1) {
            orcConf.stripeSize(getBatchSize());
        }

        m_writer = OrcFile.createWriter(new Path(getTargetFile().getURI()), orcConf);
        m_rowBatch = schema.createRowBatch();
    }

    private OrcType<?>[] getOrcKNIMETypes() {
        final List<OrcType<?>> collect = m_fields.stream().map(Pair::getSecond).collect(Collectors.toList());
        return collect.toArray(new OrcType<?>[collect.size()]);
    }

    /** Internal helper */
    private TypeDescription deriveTypeDescription() {
        final TypeDescription schema = TypeDescription.createStruct();
        for (final Pair<String, OrcType<?>> colEntry : m_fields) {
            schema.addField(colEntry.getFirst(), colEntry.getSecond().getTypeDescription());
        }
        return schema;
    }

    /**
     * Writes a row to ORC.
     *
     * @param row the {@link DataRow}
     * @throws IOException if row cannot be written
     */
    @Override
    public void writeRow(final DataRow row) throws IOException {
        final int rowInBatch = m_rowBatch.size;
        m_rowBatch.size++;
        int c = 0;
        if (isWriteRowKey()) {
            OrcStringType.writeString((BytesColumnVector) m_rowBatch.cols[0], rowInBatch, row.getKey().getString());
            c += 1;
        }
        for (; c < m_rowBatch.numCols; c++) {
            final DataCell cell = row.getCell(isWriteRowKey() ? c - 1 : c);
            @SuppressWarnings("unchecked")
            final OrcType<ColumnVector> type = (OrcType<ColumnVector>) m_columnTypes[c];
            type.writeValue(m_rowBatch.cols[c], rowInBatch, cell);
        }
        if (m_rowBatch.size == getBatchSize() - 1) {
            m_writer.addRowBatch(m_rowBatch);
            m_rowBatch.reset();
        }
    }

    /**
     * Closes the ORC Writer.
     *
     * @throws IOException if writer cannot be closed
     */
    @Override
    public void close() throws IOException {
        if (m_rowBatch.size != 0) {
            m_writer.addRowBatch(m_rowBatch);
            m_rowBatch.reset();
        }
        m_writer.close();
    }

    /**
     * Writes additional metadata to the the file
     *
     * @param settings node settings for metadata
     */
    @Override
    public void writeMetaInfoAfterWrite(final NodeSettingsWO settings) {
        final NodeSettingsWO columnsSettings = settings.addNodeSettings("columns");
        for (final Pair<String, OrcType<?>> entry : m_fields) {
            // Remember the type which has been used to write this thingy..
            final NodeSettingsWO colSetting = columnsSettings.addNodeSettings(entry.getFirst());
            colSetting.addString("type", entry.getSecond().getClass().getName());
            final DataTableSpec tableSpec = getTableSpec();
            m_writer.addUserMetadata("knime_tablename.",
                    ByteBuffer.wrap(tableSpec.getName().getBytes(Charset.forName("UTF-8"))));
            for (final String column : tableSpec.getColumnNames()) {
                final DataColumnSpec columnSpec = tableSpec.getColumnSpec(column);

                final Class<? extends DataCell> cellClass = columnSpec.getType().getCellClass();
                m_writer.addUserMetadata("knime." + column,
                        ByteBuffer.wrap(cellClass.toString().getBytes(Charset.forName("UTF-8"))));
            }
        }
    }
}
