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
 */
package org.knime.bigdata.filehandling.knox.node.connector;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.bigdata.filehandling.knox.KnoxHDFSConnection;
import org.knime.bigdata.filehandling.knox.KnoxHDFSConnectionInformation;
import org.knime.bigdata.filehandling.knox.util.KnoxHdfsConnectionInformationPortObject;
import org.knime.bigdata.filehandling.knox.util.KnoxHdfsConnectionInformationPortObjectSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Web HDFS via KNOX connection node model.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class KnoxHDFSConnectionNodeModel extends NodeModel {

    private final KnoxHDFSConnectionNodeSettings m_settings = new KnoxHDFSConnectionNodeSettings();

    /**
     * Default constructor.
     */
    protected KnoxHDFSConnectionNodeModel() {
        super(new PortType[] {}, new PortType[] { KnoxHdfsConnectionInformationPortObject.TYPE });
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        m_settings.validateValues();
        return new PortObjectSpec[]{createSpec()};
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        testConnection();
        return new PortObject[]{new KnoxHdfsConnectionInformationPortObject(createSpec())};
    }

    private KnoxHdfsConnectionInformationPortObjectSpec createSpec() {
        final KnoxHDFSConnectionInformation connectionInformation =
            m_settings.createConnectionInformation(getCredentialsProvider());
        return new KnoxHdfsConnectionInformationPortObjectSpec(connectionInformation);
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
    }

    /**
     * Perform a simple exists operation to validate that we can connect with given settings.
     *
     * @throws InvalidSettingsException on invalid connection settings
     */
    private void testConnection() throws Exception {
        final ConnectionMonitor<KnoxHDFSConnection> connectionMonitor = new ConnectionMonitor<>();
        try {
            final KnoxHDFSConnectionInformation connectionInformation =
                m_settings.createConnectionInformation(getCredentialsProvider());
            final URI resolve = connectionInformation.toURI();
            final RemoteFile<KnoxHDFSConnection> remoteFile =
                RemoteFileFactory.createRemoteFile(resolve, connectionInformation, connectionMonitor);
            // validate:
            remoteFile.exists();
        } catch (InvalidSettingsException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new InvalidSettingsException(ex.getMessage());
        } finally {
            connectionMonitor.closeAll();
        }
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // nothing todo
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // nothing todo
    }

    @Override
    protected void reset() {
        // nothing todo
    }
}
