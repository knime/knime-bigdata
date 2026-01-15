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
 * ------------------------------------------------------------------------
 */

package org.knime.bigdata.hadoop.filehandling.node;

import java.util.function.Supplier;

import org.knime.bigdata.hadoop.filehandling.fs.HdfsFSConnection;
import org.knime.bigdata.hadoop.filehandling.fs.HdfsFSConnectionConfig;
import org.knime.bigdata.hadoop.filehandling.fs.HdfsFileSystem;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FSConnectionProvider;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileSelectionWidget;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.SingleFileSelectionMode;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.WithCustomFileSystem;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.filehandling.core.connections.base.auth.AuthSettings;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.migration.LoadDefaultsForAbsentFields;
import org.knime.node.parameters.migration.Migrate;
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MaxValidation;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsPositiveIntegerValidation;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.text.TextInputWidgetValidation.PatternValidation.IsNotEmptyValidation;

/**
 * Node parameters for HDFS Connector.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 * @author AI Migration Pipeline v1.2
 */
@LoadDefaultsForAbsentFields
@SuppressWarnings("restriction")
class HdfsConnectorNodeParameters implements NodeParameters {

    // ===================== Section definitions =====================

    @Section(title = "Connection")
    interface ConnectionSection {
    }

    @Section(title = "Authentication")
    @After(ConnectionSection.class)
    interface AuthenticationSection {
    }

    @Section(title = "File System")
    @After(AuthenticationSection.class)
    interface FileSystemSection {
    }

    // ===================== References =====================

    interface ProtocolRef extends ParameterReference<Protocol> {
    }

    interface HostRef extends ParameterReference<String> {
    }

    interface UseCustomPortRef extends ParameterReference<Boolean> {
    }

    interface CustomPortRef extends ParameterReference<Integer> {
    }

    interface AuthTypeRef extends ParameterReference<AuthenticationType> {
    }

    interface UsernameRef extends ParameterReference<String> {
    }

    interface WorkingDirectoryRef extends ParameterReference<String> {
    }

    // ===================== Predicates =====================

    static final class UseCustomPort implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getBoolean(UseCustomPortRef.class).isTrue();
        }
    }

    static final class IsSimpleAuth implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(AuthTypeRef.class).isOneOf(AuthenticationType.SIMPLE);
        }
    }

    // ===================== Enums =====================

    enum Protocol {
            @Label(value = "HDFS", description = "Native HDFS protocol (default port: 8020)")
            HDFS,

            @Label(value = "WebHDFS", description = "WebHDFS REST API (default port: 9870)")
            WEBHDFS,

            @Label(value = "WebHDFS with SSL",
                description = "WebHDFS REST API with SSL encryption (default port: 9871)")
            WEBHDFS_SSL,

            @Label(value = "HttpFS", description = "HttpFS REST API (default port: 14000)")
            HTTPFS,

            @Label(value = "HttpFS with SSL", description = "HttpFS REST API with SSL encryption (default port: 14000)")
            HTTPFS_SSL;

        HdfsProtocol toHdfsProtocol() {
            return HdfsProtocol.valueOf(this.name());
        }
    }

    enum AuthenticationType {
            @Label(value = "Username", description = "Pseudo/Simple authentication using a given username")
            SIMPLE,

            @Label(value = "Kerberos", description = "Kerberos ticket based authentication")
            KERBEROS;
    }

    // ===================== Validation classes =====================

    static final class PortMaxValidation extends MaxValidation {
        @Override
        protected double getMax() {
            return 65535;
        }
    }

    // ===================== Fields =====================

    @Layout(ConnectionSection.class)
    @Persist(configKey = "protocol")
    @Widget(title = "Protocol", description = "HDFS protocol to use.")
    @ValueReference(ProtocolRef.class)
    Protocol m_protocol = Protocol.HDFS;

    @Layout(ConnectionSection.class)
    @Persist(configKey = "host")
    @Widget(title = "Host", description = "Address of HDFS name node or WebHDFS/HTTPFS node.")
    @TextInputWidget(minLengthValidation = IsNotEmptyValidation.class)
    @ValueReference(HostRef.class)
    String m_host = "localhost";

    @Layout(ConnectionSection.class)
    @Persist(configKey = "useCustomPort")
    @Widget(title = "Custom Port",
        description = "Choose whether to use the default port for the selected protocol or specify a custom port.")
    @ValueReference(UseCustomPortRef.class)
    boolean m_useCustomPort = false;

    @Layout(ConnectionSection.class)
    @Persist(configKey = "customPort")
    @Widget(title = "Custom port",
        description = "The custom port number to connect to. Must be between 1 and 65535. "
            + "Note: The WebHDFS default ports are the Hadoop 3.x default ports. "
            + "The default WebHDFS port on Hadoop 2.x is 50070 and 50470 with SSL.")
    @NumberInputWidget(minValidation = IsPositiveIntegerValidation.class, maxValidation = PortMaxValidation.class)
    @Effect(predicate = UseCustomPort.class, type = EffectType.SHOW)
    @ValueReference(CustomPortRef.class)
    int m_customPort = HdfsProtocol.HDFS.getDefaultPort();

    @Layout(AuthenticationSection.class)
    @Migrate
    @Persistor(AuthSettingsPersistor.class)
    AuthenticationFields m_authSettings = new AuthenticationFields();

    @Layout(FileSystemSection.class)
    @Persist(configKey = "workingDirectory")
    @Widget(title = "Working directory",
        description = "Specify the working directory of the resulting file system connection. "
            + "The working directory must be specified as an absolute path (starting with \"/\"). "
            + "A working directory allows downstream nodes to access files/folders using relative paths, "
            + "i.e. paths that do not have a leading slash. The default working directory is the root \"/\".")
    @FileSelectionWidget(SingleFileSelectionMode.FOLDER)
    @WithCustomFileSystem(connectionProvider = FileSystemConnectionProvider.class)
    @ValueReference(WorkingDirectoryRef.class)
    String m_workingDirectory = HdfsFileSystem.PATH_SEPARATOR;

    // ===================== Nested settings =====================

    static final class AuthenticationFields implements NodeParameters {

        @Persist(configKey = AuthSettingsPersistor.CFG_KEY_TYPE)
        @Widget(title = "Type", description = "Authentication method to use for connecting to HDFS.")
        @ValueReference(AuthTypeRef.class)
        AuthenticationType m_authType = AuthenticationType.SIMPLE;

        @Persist(configKey = AuthSettingsPersistor.CFG_KEY_USER)
        @Widget(title = "Username",
            description = "The username to use for Pseudo/Simple authentication. "
                + "This username will be used to access the HDFS file system.")
        @Effect(predicate = IsSimpleAuth.class, type = EffectType.SHOW)
        @ValueReference(UsernameRef.class)
        String m_username = System.getProperty("user.name", "");
    }

    // ===================== File System Provider =====================

    /**
     * Provides an HDFS file system connection that can be used in the working directory widget.
     *
     * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
     */
    static final class FileSystemConnectionProvider implements StateProvider<FSConnectionProvider> {

        private Supplier<Protocol> m_protocolSupplier;

        private Supplier<String> m_hostSupplier;

        private Supplier<Boolean> m_useCustomPortSupplier;

        private Supplier<Integer> m_customPortSupplier;

        private Supplier<AuthenticationType> m_authTypeSupplier;

        private Supplier<String> m_usernameSupplier;

        private Supplier<String> m_workingDirectorySupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_protocolSupplier = initializer.computeFromValueSupplier(ProtocolRef.class);
            m_hostSupplier = initializer.computeFromValueSupplier(HostRef.class);
            m_useCustomPortSupplier = initializer.computeFromValueSupplier(UseCustomPortRef.class);
            m_customPortSupplier = initializer.computeFromValueSupplier(CustomPortRef.class);
            m_authTypeSupplier = initializer.computeFromValueSupplier(AuthTypeRef.class);
            m_usernameSupplier = initializer.computeFromValueSupplier(UsernameRef.class);
            m_workingDirectorySupplier = initializer.computeFromValueSupplier(WorkingDirectoryRef.class);
            initializer.computeAfterOpenDialog();
        }

        @Override
        public FSConnectionProvider computeState(final NodeParametersInput parametersInput)
            throws StateComputationFailureException {

            return () -> { // NOSONAR: Longer lambda acceptable, as it improves readability
                final var protocol = m_protocolSupplier.get().toHdfsProtocol();
                final var builder = HdfsFSConnectionConfig.builder() //
                    .withEndpoint(protocol.getHadoopScheme(), m_hostSupplier.get(), getPort()) //
                    .withWorkingDirectory(m_workingDirectorySupplier.get());

                if (m_authTypeSupplier.get() == AuthenticationType.KERBEROS) {
                    builder.withKerberosAuthentication();
                } else {
                    builder.withSimpleAuthentication(m_usernameSupplier.get());
                }

                try {
                    return new HdfsFSConnection(builder.build());
                } catch (final IllegalArgumentException e) {
                    throw new InvalidSettingsException(e.getMessage(), e);
                }
            };
        }

        private int getPort() {
            if (m_useCustomPortSupplier.get().booleanValue()) {
                return m_customPortSupplier.get();
            }
            return m_protocolSupplier.get().toHdfsProtocol().getDefaultPort();
        }

    }

    // ===================== Persistors =====================

    static final class AuthSettingsPersistor implements NodeParametersPersistor<AuthenticationFields> {

        static final String CFG_KEY_AUTH = AuthSettings.KEY_AUTH;

        static final String CFG_KEY_TYPE = "type";

        static final String CFG_KEY_KERBEROS = "kerberos";

        static final String CFG_KEY_SIMPLE = "simple";

        static final String CFG_KEY_USER = "user";

        @Override
        public AuthenticationFields load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final var fields = new AuthenticationFields();

            if (settings.containsKey(CFG_KEY_AUTH)) {
                final NodeSettingsRO authNode = settings.getNodeSettings(CFG_KEY_AUTH);
                final var selectedType = authNode.getString(CFG_KEY_TYPE, CFG_KEY_SIMPLE);

                if (CFG_KEY_KERBEROS.equals(selectedType)) {
                    fields.m_authType = AuthenticationType.KERBEROS;
                } else {
                    fields.m_authType = AuthenticationType.SIMPLE;
                    // Load username from simple auth settings
                    if (authNode.containsKey(CFG_KEY_SIMPLE)) {
                        final NodeSettingsRO simpleNode = authNode.getNodeSettings(CFG_KEY_SIMPLE);
                        fields.m_username = simpleNode.getString(CFG_KEY_USER, System.getProperty("user.name", ""));
                    }
                }
            }

            return fields;
        }

        @Override
        public void save(final AuthenticationFields fields, final NodeSettingsWO settings) {
            final NodeSettingsWO authNode = settings.addNodeSettings(CFG_KEY_AUTH);

            // Save the authentication type
            if (fields.m_authType == AuthenticationType.KERBEROS) {
                authNode.addString(CFG_KEY_TYPE, CFG_KEY_KERBEROS);
            } else {
                authNode.addString(CFG_KEY_TYPE, CFG_KEY_SIMPLE);
            }

            // Save kerberos auth settings (always, for backward compatibility)
            authNode.addNodeSettings(CFG_KEY_KERBEROS);

            // Save simple auth settings (always, for backward compatibility)
            final NodeSettingsWO simpleNode = authNode.addNodeSettings(CFG_KEY_SIMPLE);
            simpleNode.addString(CFG_KEY_USER, fields.m_username);
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{CFG_KEY_AUTH, CFG_KEY_TYPE}, {CFG_KEY_AUTH, CFG_KEY_SIMPLE, CFG_KEY_USER}};
        }
    }

}
