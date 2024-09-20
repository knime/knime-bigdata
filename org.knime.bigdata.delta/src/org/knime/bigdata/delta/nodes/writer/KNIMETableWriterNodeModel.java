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
 *   2024-09-17 (Tobias): created
 */
package org.knime.bigdata.delta.nodes.writer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.EnumSet;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.knime.bigdata.fileformats.parquet.ParquetFileFormatWriter;
import org.knime.bigdata.fileformats.parquet.ParquetFormatFactory;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.core.data.DataRow;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.util.FileUtil;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeSettings;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.filehandling.core.connections.DefaultFSConnectionFactory;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.FileOverwritePolicy;
import org.knime.filehandling.core.defaultnodesettings.status.NodeModelStatusConsumer;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage.MessageType;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction") // New Node UI is not yet API
public class KNIMETableWriterNodeModel extends NodeModel {

    private KNIMETableWriterNodeSettings m_settings = new KNIMETableWriterNodeSettings();

    private final NodeModelStatusConsumer m_statusConsumer;

    /**
     * Constructor.
     *
     * @param portsConfiguration the ports configuration
     */
    KNIMETableWriterNodeModel(final PortsConfiguration portsConfiguration) {
        super(portsConfiguration.getInputPorts(), portsConfiguration.getOutputPorts());
        m_statusConsumer = new NodeModelStatusConsumer(EnumSet.of(MessageType.ERROR, MessageType.WARNING));
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return null;
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {

        final BufferedDataTable tbl = (BufferedDataTable)inObjects[0];
        final File tempFile = FileUtil.createTempFile("data", ".parquet");
        final String absolutePath = tempFile.getAbsolutePath();
        final FSConnection fsConnection = DefaultFSConnectionFactory.createLocalFSConnection();
        final FSPath path = fsConnection.getFileSystem().getPath(absolutePath);

        final ParquetFormatFactory factory = new ParquetFormatFactory(true);
        final DataTypeMappingService<ParquetType, ?, ?> mappingService = factory.getTypeMappingService();
        final DataTypeMappingConfiguration<ParquetType> mappingConfiguration =
            mappingService.newDefaultKnimeToExternalMappingConfiguration();
        try (ParquetFileFormatWriter writer =
            (ParquetFileFormatWriter)factory.getWriter(path, FileOverwritePolicy.OVERWRITE, tbl.getDataTableSpec(),
                Long.MAX_VALUE, Integer.MAX_VALUE, CompressionCodecName.UNCOMPRESSED.name(), mappingConfiguration)) {
            for (DataRow row : tbl) {
                writer.writeRow(row);
            }
        }
        //upload local file to S3
        final OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder().url(m_settings.m_url)
            .put(RequestBody.create(tempFile, null))
            .addHeader("content-type", "application/vnd.apache.parquet;").addHeader("cache-control", "no-cache")
            .addHeader("Content-Length", Long.toString(Files.size(path)))
            .build();
        Response execute = client.newCall(request).execute();
        return null;

    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings = DefaultNodeSettings.loadSettings(settings, KNIMETableWriterNodeSettings.class);
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        DefaultNodeSettings.saveSettings(KNIMETableWriterNodeSettings.class, m_settings, settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
    }

    @Override
    protected void reset() {
        //nothing to reset
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

    }

}
