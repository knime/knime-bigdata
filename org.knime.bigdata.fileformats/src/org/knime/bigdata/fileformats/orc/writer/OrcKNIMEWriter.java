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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.WriterOptions;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.fileformats.node.writer.AbstractFileFormatWriter;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCDestination;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCParameter;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.data.convert.map.KnimeToExternalMapper;
import org.knime.core.data.convert.map.MappingFramework;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.util.Pair;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;

/**
 * ORC Writer for KNIME {@link DataRow}. It holds the rows in batches and hands
 * them to the ORC writer once the maximum batch size is reached.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class OrcKNIMEWriter extends AbstractFileFormatWriter {

    private List<Pair<String, TypeDescription>> m_fields;

    private Writer m_writer;

    private VectorizedRowBatch m_rowBatch;

    private final CompressionKind m_compression;

    private final ORCParameter[] m_params;

    private ORCDestination m_destination;

    private final ConsumptionPath[] m_consumptionPaths;

    private final KnimeToExternalMapper<ORCDestination, ORCParameter> m_knimeToExternalMapper;

    /**
     * Constructor for ORC writer, that writes KNIME {@link DataRow}s to an ORC file
     *
     * @param file
     *            the target file
     * @param spec
     *            the data table spec
     * @param batchSize
     *            size of the batch
     * @param compression
     *            the compression to use for writing
     * @param inputputDataTypeMappingConfiguration
     *            the type mapping configuration
     * @throws IOException
     *             if writer cannot be initialized
     */
    public OrcKNIMEWriter(final RemoteFile<Connection> file, final DataTableSpec spec, final int batchSize,
            final String compression, final DataTypeMappingConfiguration<?> inputputDataTypeMappingConfiguration)
                    throws IOException {
        super(file, batchSize, spec);
        m_compression = CompressionKind.valueOf(compression);
        m_params = new ORCParameter[spec.getNumColumns()];
        for (int i = 0; i < spec.getNumColumns(); i++) {
            m_params[i] = new ORCParameter(i);
        }
        try {
            m_consumptionPaths = inputputDataTypeMappingConfiguration.getConsumptionPathsFor(spec);
        } catch (final InvalidSettingsException e) {
            throw new BigDataFileFormatException(e);
        }
        m_knimeToExternalMapper = MappingFramework.createMapper(m_consumptionPaths);
        initWriter();
    }

    /**
     * Closes the ORC Writer.
     *
     * @throws IOException
     *             if writer cannot be closed
     */
    @Override
    public void close() throws IOException {
        if (m_rowBatch.size != 0) {
            m_writer.addRowBatch(m_rowBatch);
            m_rowBatch.reset();
        }
        m_writer.close();
    }

    /** Internal helper */
    private TypeDescription deriveTypeDescription() {
        final TypeDescription schema = TypeDescription.createStruct();

        for (final Pair<String, TypeDescription> colEntry : m_fields) {
            schema.addField(colEntry.getFirst(), colEntry.getSecond());
        }
        return schema;
    }

    private void initWriter() throws IOException {
        m_fields = new ArrayList<>();
        final DataTableSpec tableSpec = getTableSpec();
        for(int i = 0; i < tableSpec.getNumColumns(); i++) {
            final TypeDescription orcType = (TypeDescription)m_consumptionPaths[i].getConsumerFactory()
                    .getDestinationType();
            final String colName = tableSpec.getColumnSpec(i).getName();
            m_fields.add(new Pair<>(colName, TypeDescription.fromString(orcType.toString())));
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
        m_destination = new ORCDestination(m_rowBatch, m_rowBatch.size);
    }

    /**
     * Writes additional metadata to the the file
     *
     * @param settings
     *            node settings for metadata
     */
    @Override
    public void writeMetaInfoAfterWrite(final NodeSettingsWO settings) {
        final NodeSettingsWO columnsSettings = settings.addNodeSettings("columns");
        for (final Pair<String, TypeDescription> entry : m_fields) {
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

    /**
     * Writes a row to ORC.
     *
     * @param row
     *            the {@link DataRow}
     * @throws Exception
     */
    @Override
    public void writeRow(final DataRow row) throws Exception {
        m_destination.next();
        m_knimeToExternalMapper.map(row, m_destination, m_params);

        if (m_rowBatch.size == getBatchSize() - 1) {
            m_writer.addRowBatch(m_rowBatch);
            m_rowBatch.reset();
        }
    }
}
