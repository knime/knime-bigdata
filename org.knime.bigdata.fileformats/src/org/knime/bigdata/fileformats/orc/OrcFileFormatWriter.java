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
 *   11 Nov 2021 (Tobias): created
 */
package org.knime.bigdata.fileformats.orc;

import java.io.IOException;
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
import org.knime.bigdata.fileformats.node.writer2.FileFormatWriter;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCDestination;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCParameter;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.bigdata.hadoop.filesystem.NioFileSystemUtil;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.data.convert.map.KnimeToExternalMapper;
import org.knime.core.data.convert.map.MappingException;
import org.knime.core.data.convert.map.MappingFramework;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.util.Pair;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.filehandling.core.connections.FSPath;

/**
 * ORC file writer implementation.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public final class OrcFileFormatWriter implements FileFormatWriter {

    private final CompressionKind m_compression;

    private final ORCParameter[] m_params;

    private final ConsumptionPath[] m_consumptionPaths;

    private final KnimeToExternalMapper<ORCDestination, ORCParameter> m_knimeToExternalMapper;

    private final long m_fileSize;

    private final int m_chunkSize;

    private final Path m_path;

    private final DataTableSpec m_spec;

    private List<Pair<String, TypeDescription>> m_fields;

    private Writer m_writer;

    private VectorizedRowBatch m_rowBatch;

    private long m_noOfRowsWritten = 0;

    private ORCDestination m_destination;


    /**
     * @param path the {@link FSPath} to write to
     * @param spec the {@link DataTableSpec} of the rows that will be written
     * @param fileSize the maximum file size in bytes
     * @param chunkSize the chunk size
     * @param compression the {@link CompressionKind}
     * @param typeMappingConfiguration the DataTypeMappingConfiguration to use
     * @throws IOException if the parquet writer can not be initialized
     */
    public OrcFileFormatWriter(final FSPath path, final DataTableSpec spec, final long fileSize, final int chunkSize,
        final CompressionKind compression, final DataTypeMappingConfiguration<?> typeMappingConfiguration)
        throws IOException {
        final Configuration hadoopFileSystemConfig = NioFileSystemUtil.getConfiguration();
        m_path = NioFileSystemUtil.getHadoopPath(path, hadoopFileSystemConfig);
        m_spec = spec;
        m_fileSize = fileSize;
        m_chunkSize = chunkSize;
        m_compression = compression;
        m_params = new ORCParameter[spec.getNumColumns()];
        for (int i = 0; i < spec.getNumColumns(); i++) {
            m_params[i] = new ORCParameter(i);
        }
        try {
            m_consumptionPaths = typeMappingConfiguration.getConsumptionPathsFor(spec);
        } catch (final InvalidSettingsException e) {
            throw new BigDataFileFormatException(e);
        }
        m_knimeToExternalMapper = MappingFramework.createMapper(m_consumptionPaths);
        initWriter();
    }

    @Override
    public void close() throws IOException {
        if (m_rowBatch.size != 0) {
            m_writer.addRowBatch(m_rowBatch);
            m_rowBatch.reset();
        }
        m_writer.close();
    }

    @Override
    public boolean writeRow(final DataRow row) {
        m_destination.next();
        try {
            m_knimeToExternalMapper.map(row, m_destination, m_params);
            m_noOfRowsWritten++;
            if (m_noOfRowsWritten % m_chunkSize == 0) {
                m_writer.addRowBatch(m_rowBatch);
                m_rowBatch.reset();
            }
            return m_noOfRowsWritten >= m_fileSize;
        } catch (IOException | MappingException ex) {
            throw new IllegalStateException("Exception while writing row to ORC.", ex);
        }
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
        for (int i = 0; i < m_spec.getNumColumns(); i++) {
            final TypeDescription orcType =
                (TypeDescription)m_consumptionPaths[i].getConsumerFactory().getDestinationType();
            final String colName = m_spec.getColumnSpec(i).getName();
            m_fields.add(new Pair<>(colName, TypeDescription.fromString(orcType.toString())));
        }
        final Configuration conf = new Configuration();
        final TypeDescription schema = deriveTypeDescription();
        final WriterOptions orcConf =
            OrcFile.writerOptions(conf).setSchema(schema).compress(m_compression).version(OrcFile.Version.CURRENT);
        ////TODO: Supported with ORC 1.4.5 https://issues.apache.org/jira/browse/ORC-231
        //orcConf.override();

        m_writer = OrcFile.createWriter(m_path, orcConf);
        m_rowBatch = schema.createRowBatch(m_chunkSize);
        m_destination = new ORCDestination(m_rowBatch, m_rowBatch.size);
    }
}