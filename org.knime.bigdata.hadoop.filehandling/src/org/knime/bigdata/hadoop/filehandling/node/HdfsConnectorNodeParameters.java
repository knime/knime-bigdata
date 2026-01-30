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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.function.Supplier;

import org.knime.bigdata.hadoop.filehandling.fs.HdfsFSConnection;
import org.knime.bigdata.hadoop.filehandling.fs.HdfsFSConnectionConfig;
import org.knime.bigdata.hadoop.filehandling.fs.HdfsFileSystem;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FSConnectionProvider;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileSelectionWidget;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.SingleFileSelectionMode;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.WithCustomFileSystem;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.validation.custom.CustomValidation;
import org.knime.core.webui.node.dialog.defaultdialog.widget.validation.custom.SimpleValidation;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.migration.LoadDefaultsForAbsentFields;
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
import org.knime.node.parameters.updates.util.BooleanReference;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MaxValidation;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsPositiveIntegerValidation;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.text.TextInputWidgetValidation.PatternValidation.IsNotBlankValidation;
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

    static final class UseCustomPortRef implements BooleanReference {
    }

    interface CustomPortRef extends ParameterReference<Integer> {
    }

    interface AuthenticationParametersRef extends ParameterReference<AuthenticationParameters> {
    }

    interface WorkingDirectoryRef extends ParameterReference<String> {
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

    // ===================== Validation classes =====================

    static final class PortMaxValidation extends MaxValidation {
        @Override
        protected double getMax() {
            return 65535;
        }
    }

    // ===================== Fields =====================

    @Layout(ConnectionSection.class)
    @Widget(title = "Protocol", description = "HDFS protocol to use.")
    @ValueReference(ProtocolRef.class)
    Protocol m_protocol = Protocol.HDFS;

    @Layout(ConnectionSection.class)
    @Widget(title = "Host", description = "Address of HDFS name node or WebHDFS/HTTPFS node.")
    @TextInputWidget(patternValidation = IsNotBlankValidation.class)
    @ValueReference(HostRef.class)
    String m_host = "localhost";

    @Layout(ConnectionSection.class)
    @Widget(title = "Custom Port",
        description = "Choose whether to use the default port for the selected protocol or specify a custom port.")
    @ValueReference(UseCustomPortRef.class)
    boolean m_useCustomPort;

    @Layout(ConnectionSection.class)
    @Widget(title = "Custom port", description = """
        The custom port number to connect to. Must be between 1 and 65535.
        Note: The WebHDFS default ports are the Hadoop 3.x default ports.
        The default WebHDFS port on Hadoop 2.x is 50070 and 50470 with SSL.""")
    @NumberInputWidget(minValidation = IsPositiveIntegerValidation.class, maxValidation = PortMaxValidation.class)
    @Effect(predicate = UseCustomPortRef.class, type = EffectType.SHOW)
    @ValueReference(CustomPortRef.class)
    int m_customPort = HdfsProtocol.HDFS.getDefaultPort();

    @Layout(AuthenticationSection.class)
    @Persist(configKey = AuthenticationParameters.CFG_KEY)
    @ValueReference(AuthenticationParametersRef.class)
    AuthenticationParameters m_authentication = new AuthenticationParameters();

    @Layout(FileSystemSection.class)
    @Widget(title = "Working directory", description = """
        Specify the working directory of the resulting file system connection.
        The working directory must be specified as an absolute path (starting with "/").
        A working directory allows downstream nodes to access files/folders using relative paths,
        i.e. paths that do not have a leading slash. The default working directory is the root "/".""")
    @FileSelectionWidget(SingleFileSelectionMode.FOLDER)
    @WithCustomFileSystem(connectionProvider = FileSystemConnectionProvider.class)
    @ValueReference(WorkingDirectoryRef.class)
    @CustomValidation(WorkingDirectoryValidator.class)
    String m_workingDirectory = HdfsFileSystem.PATH_SEPARATOR;

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

        private Supplier<AuthenticationParameters> m_authParametersSupplier;

        private Supplier<String> m_workingDirectorySupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_protocolSupplier = initializer.computeFromValueSupplier(ProtocolRef.class);
            m_hostSupplier = initializer.computeFromValueSupplier(HostRef.class);
            m_useCustomPortSupplier = initializer.computeFromValueSupplier(UseCustomPortRef.class);
            m_customPortSupplier = initializer.computeFromValueSupplier(CustomPortRef.class);
            m_authParametersSupplier = initializer.computeFromValueSupplier(AuthenticationParametersRef.class);
            m_workingDirectorySupplier = initializer.computeFromValueSupplier(WorkingDirectoryRef.class);
            initializer.computeAfterOpenDialog();
        }

        @Override
        public FSConnectionProvider computeState(final NodeParametersInput parametersInput)
            throws StateComputationFailureException {

            return () -> { // NOSONAR: Longer lambda acceptable, as it improves readability
                final var params = new HdfsConnectorNodeParameters();
                params.m_protocol = m_protocolSupplier.get();
                params.m_host = m_hostSupplier.get();
                params.m_useCustomPort = m_useCustomPortSupplier.get();
                params.m_customPort = m_customPortSupplier.get();
                params.m_authentication = m_authParametersSupplier.get();

                params.m_workingDirectory = m_workingDirectorySupplier.get();
                if (!isValidWorkingDirectory(params.m_workingDirectory)) {
                    params.m_workingDirectory = HdfsFileSystem.PATH_SEPARATOR;
                }

                params.validateOnConfigure();

                try {
                    return new HdfsFSConnection(params.toFSConnectionConfig());
                } catch (final IllegalArgumentException e) {
                    throw new InvalidSettingsException(e.getMessage(), e);
                }
            };
        }

    }

    // ===================== Nested settings =====================

    private static final class AuthenticationParameters implements NodeParameters {

        private static final String CFG_KEY = "auth";
        private static final String CFG_KEY_SIMPLE = "simple";
        private static final String CFG_KEY_KERBEROS = "kerberos";

        enum AuthenticationMethod {
          @Label(value = "Username", description = "Pseudo/Simple authentication using a given username")
          SIMPLE,

          @Label(value = "Kerberos", description = "Kerberos ticket based authentication")
          KERBEROS,
        }

        @Widget(title = "Authentication method", description = "Specify the authentication method to use.")
        @ValueReference(AuthenticationMethodRef.class)
        @Persistor(AuthenticationMethodPersistor.class)
        AuthenticationMethod m_type = AuthenticationMethod.SIMPLE;

        static final class AuthenticationMethodRef implements ParameterReference<AuthenticationMethod> {
        }

        static final class IsSimpleAuth implements EffectPredicateProvider {
            @Override
            public EffectPredicate init(final PredicateInitializer i) {
                return i.getEnum(AuthenticationMethodRef.class).isOneOf(AuthenticationMethod.SIMPLE);
            }
        }

        static final class AuthenticationMethodPersistor implements NodeParametersPersistor<AuthenticationMethod> {

            private static final String ENTRY_KEY = "type";

            @Override
            public AuthenticationMethod load(final NodeSettingsRO settings) throws InvalidSettingsException {
                final var typeString = settings.getString(ENTRY_KEY, "");
                if (typeString.equalsIgnoreCase(CFG_KEY_SIMPLE)) {
                    return AuthenticationMethod.SIMPLE;
                } else if (typeString.equalsIgnoreCase(CFG_KEY_KERBEROS)) {
                    return AuthenticationMethod.KERBEROS;
                }
                throw new InvalidSettingsException(
                    String.format("Unknown authentication method: '%s'. Possible values: '%s', '%s'", typeString,
                        CFG_KEY_SIMPLE, CFG_KEY_KERBEROS));
            }

            @Override
            public void save(final AuthenticationMethod param, final NodeSettingsWO settings) {
                saveInternal(param, settings);
            }

            private static void saveInternal(final AuthenticationMethod param, final NodeSettingsWO settings) {
                if (param == AuthenticationMethod.SIMPLE) {
                    settings.addString(ENTRY_KEY, CFG_KEY_SIMPLE);
                } else if (param == AuthenticationMethod.KERBEROS) {
                    settings.addString(ENTRY_KEY, CFG_KEY_KERBEROS);
                }
            }

            @Override
            public String[][] getConfigPaths() {
                return new String[][] { { ENTRY_KEY } };
            }
        }

        // ----- SECOND LEVEL NESTING NEEDED FOR BACKWARD COMPATIBILITY -----

        @Persist(configKey = CFG_KEY_SIMPLE)
        @Effect(predicate = IsSimpleAuth.class, type = EffectType.SHOW)
        SimpleAuthParameters m_simpleAuth = new SimpleAuthParameters();

        static final class SimpleAuthParameters implements NodeParameters {

            @Widget(title = "Username", description = """
                    The username to use for Pseudo/Simple authentication.
                    This username will be used to access the HDFS file system.""")
            @Persistor(SimpleUserPersistor.class)
            @TextInputWidget(patternValidation = IsNotBlankValidation.class)
            String m_username = System.getProperty("user.name");

            static final class SimpleUserPersistor implements NodeParametersPersistor<String> {

                private static final String ENTRY_KEY_USER = "user";

                @Override
                public String load(final NodeSettingsRO settings) throws InvalidSettingsException {
                    return settings.getString(ENTRY_KEY_USER, System.getProperty("user.name"));
                }

                @Override
                public void save(final String param, final NodeSettingsWO settings) {
                    saveInternal(param, settings);
                }

                private static void saveInternal(final String param, final NodeSettingsWO settings) {
                    settings.addString(ENTRY_KEY_USER, param);
                }

                @Override
                public String[][] getConfigPaths() {
                    return new String[][] { { ENTRY_KEY_USER } };
                }
            }
        }
    }

    // ===================== Validation =====================

    void validateOnConfigure() throws InvalidSettingsException {
        // host validation
        CheckUtils.checkSetting(isNotBlank(m_host), "Host required.");

        // custom port validation
        if (m_useCustomPort && (m_customPort < 1 || m_customPort > 65535)) {
            throw new InvalidSettingsException("Custom port must be between 1 and 65535.");
        }

        // authentication validation
        if (m_authentication.m_type == AuthenticationParameters.AuthenticationMethod.SIMPLE) {
            final var username = m_authentication.m_simpleAuth.m_username;
            CheckUtils.checkSetting(isNotBlank(username), "Username required for simple authentication.");
        }

        // working directory validation
        validateWorkingDirectory(m_workingDirectory);
    }

    private static boolean isValidWorkingDirectory(final String workingDirectory) {
        return workingDirectory != null && workingDirectory.startsWith(HdfsFileSystem.PATH_SEPARATOR);
    }

    private static void validateWorkingDirectory(final String workingDirectory) throws InvalidSettingsException {
        if (!isValidWorkingDirectory(workingDirectory)) {
            throw new InvalidSettingsException("Working directory must be set to an absolute path.");
        }
    }

    // ===================== Custom Validations =====================

    static class WorkingDirectoryValidator extends SimpleValidation<String> {
        @Override
        public void validate(final String workingDir) throws InvalidSettingsException {
            validateWorkingDirectory(workingDir);
        }
    }

    // ===================== File System Config =====================

    /**
     * Convert this settings to a {@link HdfsFSConnectionConfig} instance.
     *
     * @return a {@link HdfsFSConnectionConfig} using this settings
     */
    public HdfsFSConnectionConfig toFSConnectionConfig() {
        final var port = m_useCustomPort ? m_customPort : m_protocol.toHdfsProtocol().getDefaultPort();
        final var builder = HdfsFSConnectionConfig.builder() //
            .withEndpoint(m_protocol.toHdfsProtocol().getHadoopScheme(), m_host, port) //
            .withWorkingDirectory(m_workingDirectory);

        if (m_authentication.m_type == AuthenticationParameters.AuthenticationMethod.KERBEROS) {
            builder.withKerberosAuthentication();
        } else {
            builder.withSimpleAuthentication(m_authentication.m_simpleAuth.m_username);
        }

        return builder.build();
    }

}
