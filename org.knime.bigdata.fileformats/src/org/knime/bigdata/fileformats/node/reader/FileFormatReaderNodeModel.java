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
package org.knime.bigdata.fileformats.node.reader;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.commons.hadoop.ConfigurationFactory;
import org.knime.bigdata.commons.hadoop.UserGroupUtil;
import org.knime.bigdata.fileformats.utility.FileHandlingUtility;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowIterator;
import org.knime.core.node.BufferedDataTable;
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
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;

/**
 * Node model for generic file format reader.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class FileFormatReaderNodeModel extends NodeModel {
    private final FileFormatReaderNodeSettings m_settings;
	/**
     * Constructor for the node model.
     */
    protected FileFormatReaderNodeModel(final FileFormatReaderNodeSettings settings) {
        super(new PortType[] { ConnectionInformationPortObject.TYPE_OPTIONAL },
                new PortType[] { BufferedDataTable.TYPE });
        m_settings = settings;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BufferedDataTable[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final ConnectionInformationPortObject connInfoObj = (ConnectionInformationPortObject) inObjects[0];

        final BigDataFileFormatTable table = createTable(connInfoObj, exec);
        final BufferedDataTable out = exec.createBufferedDataTable(table, exec);

        return new BufferedDataTable[] { out };
    }

    private BigDataFileFormatTable createTable(final ConnectionInformationPortObject connInfoObj,
            final ExecutionContext exec) throws Exception {
        ConnectionInformation connInfo = null;
        if (connInfoObj != null) {
            connInfo = connInfoObj.getConnectionInformation();
        }
        final RemoteFile<Connection> sourceFile = FileHandlingUtility.createRemoteFile(m_settings.getFileName(),
                connInfo);
		final DataTypeMappingConfiguration<?> outputDataTypeMappingConfiguration = m_settings.getmappingModel()
				.getDataTypeMappingConfiguration();
		final AbstractFileFormatReader reader = getReader(sourceFile, exec, outputDataTypeMappingConfiguration);
        return new BigDataFileFormatTable(reader);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // Nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        ConnectionInformation connInfo = null;
        final ConnectionInformationPortObjectSpec connectionSpec = (ConnectionInformationPortObjectSpec) inSpecs[0];
        if (m_settings.getFileName().isEmpty()) {
            throw new InvalidSettingsException("No source location provided! Please enter a valid location.");
        }
        if (connectionSpec != null) {

            connInfo = connectionSpec.getConnectionInformation();
            if (connInfo == null) {
                throw new InvalidSettingsException("No connection Information avaiable");
            }

            if (connInfo instanceof CloudConnectionInformation) {

                return new DataTableSpec[] { null };
            }
        }
        try {
			final DataTypeMappingConfiguration<?> outputDataTypeMappingConfiguration = m_settings.getmappingModel()
					.getDataTypeMappingConfiguration();
            // Create a reader to get the generated TableSpec
            final RemoteFile<Connection> remoteFile = FileHandlingUtility.createRemoteFile(m_settings.getFileName(),
                    connInfo);
            if (!remoteFile.exists()) {
                throw new InvalidSettingsException("Input file '" + remoteFile.getPath() + "' does not exist");
            }
            if (remoteFile.isDirectory() && remoteFile.listFiles().length == 0) {
                throw new InvalidSettingsException(String.format("Empty directory %s.", m_settings.getFileName()));
            }
			final AbstractFileFormatReader reader = getReader(remoteFile, null, outputDataTypeMappingConfiguration);
            final DataTableSpec spec = reader.getTableSpec();
            return new DataTableSpec[] { spec };

        } catch (final Exception e) {
            throw new InvalidSettingsException(e);
        }
    }

    /**
	 * @param remoteFile
	 *            the file to read
	 * @param context
	 *            the execution context
	 * @param outputDataTypeMappingConfiguration
	 * @return the reader object
	 * @throws Exception
	 *             thrown if doAs user does not work
	 */
	private AbstractFileFormatReader getReader(final RemoteFile<Connection> remoteFile, final ExecutionContext context,
			DataTypeMappingConfiguration<?> outputDataTypeMappingConfiguration)
            throws Exception {
        final AbstractFileFormatReader reader;
		if (remoteFile.getConnectionInformation() != null && remoteFile.getConnectionInformation().useKerberos()) {
            final Configuration conf = ConfigurationFactory.createBaseConfigurationWithKerberosAuth();
            final UserGroupInformation user = UserGroupUtil.getKerberosUser(conf);
            reader = user.doAs(new PrivilegedExceptionAction<AbstractFileFormatReader>() {
                @Override
                public AbstractFileFormatReader run() throws Exception {

					return m_settings.getFormatFactory().getReader(remoteFile, context,
							outputDataTypeMappingConfiguration);
                }
            });
            reader.setUser(user);
        } else {
			reader = m_settings.getFormatFactory().getReader(remoteFile, context, outputDataTypeMappingConfiguration);
        }
        return reader;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File internDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // Nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File internDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // Nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo,
            final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new StreamableOperator() {

            @Override
            public void runFinal(final PortInput[] inputs, final PortOutput[] outputs, final ExecutionContext exec)
                    throws Exception {
                final PortObject portObj = (PortObject) inputs[0];
                final RowOutput out = (RowOutput) outputs[0];
                final ConnectionInformationPortObject connInfoObj = (ConnectionInformationPortObject) portObj;
                try {
                    final BigDataFileFormatTable table = createTable(connInfoObj, exec);
                    final RowIterator rowIterator = table.iterator();
                    while (rowIterator.hasNext()) {
                        out.push(rowIterator.next());
                        exec.checkCanceled();
                    }
                } finally {
                    out.close();
                }
            }
        };
    }
}
