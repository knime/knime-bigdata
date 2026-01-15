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

package org.knime.bigdata.hadoop.filehandling.knox.node;

import java.time.Duration;
import java.util.function.Supplier;

import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFSConnection;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFSConnectionConfig;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFileSystem;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.webui.node.dialog.defaultdialog.NodeParametersInputImpl;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FSConnectionProvider;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileSelectionWidget;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.SingleFileSelectionMode;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.WithCustomFileSystem;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.credentials.base.node.CredentialsSettings.CredentialsFlowVarChoicesProvider;
import org.knime.filehandling.core.connections.base.auth.IDWithSecretAuthProviderSettings;
import org.knime.node.parameters.Advanced;
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
import org.knime.node.parameters.updates.ValueProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.credentials.Credentials;
import org.knime.node.parameters.widget.credentials.CredentialsWidget;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsNonNegativeValidation;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.text.TextInputWidgetValidation.PatternValidation.IsNotEmptyValidation;

/**
 * Node parameters for HDFS Connector (KNOX).
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 * @author AI Migration Pipeline v1.2
 */
@LoadDefaultsForAbsentFields
@SuppressWarnings("restriction")
class KnoxHdfsConnectorNodeParameters implements NodeParameters {

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

    @Section(title = "Advanced")
    @After(FileSystemSection.class)
    @Advanced
    interface TimeoutsSection {
    }

    // ===================== References =====================

    interface UrlRef extends ParameterReference<String> {
    }

    interface AuthenticationParametersRef extends ParameterReference<AuthenticationParameters> {
    }

    interface WorkingDirectoryRef extends ParameterReference<String> {
    }

    interface ConnectionTimeoutRef extends ParameterReference<Integer> {
    }

    interface ReceiveTimeoutRef extends ParameterReference<Integer> {
    }

    // ===================== Fields =====================

    @Layout(ConnectionSection.class)
    @Persist(configKey = "url")
    @Widget(title = "URL",
        description = "KNOX URL including cluster name, for example "
            + "<tt>https://&lt;server&gt;:8443/gateway/&lt;default&gt;</tt> "
            + "(replace &lt;server&gt; with the fully qualified hostname and &lt;default&gt; "
            + "with your cluster name). The protocol should be <i>https</i> and port <i>8443</i> "
            + "on most deployments.")
    @TextInputWidget(minLengthValidation = IsNotEmptyValidation.class)
    @ValueReference(UrlRef.class)
    String m_url = "https://server:8443/gateway/default";

    @Layout(AuthenticationSection.class)
    @Persist(configKey = AuthenticationParameters.CFG_KEY)
    @ValueReference(AuthenticationParametersRef.class)
    AuthenticationParameters m_authentication = new AuthenticationParameters();

    @Layout(FileSystemSection.class)
    @Persist(configKey = "workingDirectory")
    @Widget(title = "Working directory",
        description = "Specify the working directory of the resulting file system connection, "
            + "using the UNIX-like path syntax. The working directory must be specified as an absolute path "
            + "(starting with \"/\"). A working directory allows downstream nodes to access files/folders "
            + "using relative paths, i.e. paths that do not have a leading slash. "
            + "The default working directory is the root \"/\".")
    @FileSelectionWidget(SingleFileSelectionMode.FOLDER)
    @WithCustomFileSystem(connectionProvider = FileSystemConnectionProvider.class)
    @ValueReference(WorkingDirectoryRef.class)
    String m_workingDirectory = KnoxHdfsFileSystem.PATH_SEPARATOR;

    @Layout(TimeoutsSection.class)
    @Persist(configKey = "connectionTimeout")
    @Widget(title = "Connection timeout (seconds)",
        description = "Timeout in seconds to establish a connection, or 0 for an infinite timeout.")
    @NumberInputWidget(minValidation = IsNonNegativeValidation.class)
    @ValueReference(ConnectionTimeoutRef.class)
    int m_connectionTimeout = 30;

    @Layout(TimeoutsSection.class)
    @Persist(configKey = "receiveTimeout")
    @Widget(title = "Read timeout (seconds)",
        description = "Timeout in seconds to read data from the connection, or 0 for an infinite timeout.")
    @NumberInputWidget(minValidation = IsNonNegativeValidation.class)
    @ValueReference(ReceiveTimeoutRef.class)
    int m_receiveTimeout = 30;

    // ===================== File System Provider =====================

    /**
     * Provides a KNOX HDFS file system connection that can be used in the working directory widget.
     */
    static final class FileSystemConnectionProvider implements StateProvider<FSConnectionProvider> {

        private Supplier<String> m_urlSupplier;

        private Supplier<AuthenticationParameters> m_authParamsSupplier;

        private Supplier<String> m_workingDirectorySupplier;

        private Supplier<Integer> m_connectionTimeoutSupplier;

        private Supplier<Integer> m_receiveTimeoutSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_urlSupplier = initializer.computeFromValueSupplier(UrlRef.class);
            m_authParamsSupplier = initializer.computeFromValueSupplier(AuthenticationParametersRef.class);
            m_workingDirectorySupplier = initializer.computeFromValueSupplier(WorkingDirectoryRef.class);
            m_connectionTimeoutSupplier = initializer.computeFromValueSupplier(ConnectionTimeoutRef.class);
            m_receiveTimeoutSupplier = initializer.computeFromValueSupplier(ReceiveTimeoutRef.class);
            initializer.computeAfterOpenDialog();
        }

        @Override
        public FSConnectionProvider computeState(final NodeParametersInput parametersInput)
            throws StateComputationFailureException {

            return () -> { // NOSONAR: Longer lambda acceptable, as it improves readability
                final var authParams = m_authParamsSupplier.get();
                final var cred = getCredentialsProvider(parametersInput);
                final var config = KnoxHdfsFSConnectionConfig.builder()
                    .withEndpointUrl(m_urlSupplier.get())
                    .withWorkingDirectory(m_workingDirectorySupplier.get())
                    .withUserAndPassword(getUsername(authParams, cred), getPassword(authParams, cred))
                    .withConnectionTimeout(Duration.ofSeconds(m_connectionTimeoutSupplier.get()))
                    .withReceiveTimeout(Duration.ofSeconds(m_receiveTimeoutSupplier.get()))
                    .build();

                try {
                    return new KnoxHdfsFSConnection(config);
                } catch (final IllegalArgumentException e) {
                    throw new InvalidSettingsException(e.getMessage(), e);
                }
            };
        }

        private static CredentialsProvider getCredentialsProvider(final NodeParametersInput input) {
            return ((NodeParametersInputImpl) input).getCredentialsProvider().orElseThrow();
        }

        private static String getUsername(final AuthenticationParameters ap, final CredentialsProvider cp) {
            if (ap.m_userPwdAuth.m_useCredentials) {
                return cp.get(ap.m_userPwdAuth.m_credentialsFlowVarName).getLogin();
            } else {
                return ap.m_userPwdAuth.m_userPassword.getUsername();
            }
        }

        private static String getPassword(final AuthenticationParameters ap, final CredentialsProvider cp) {
            if (ap.m_userPwdAuth.m_useCredentials) {
                return cp.get(ap.m_userPwdAuth.m_credentialsFlowVarName).getPassword();
            } else {
                return ap.m_userPwdAuth.m_userPassword.getPassword();
            }
        }

    }

    // ===================== Parameter Groups =====================

    private static final class AuthenticationParameters implements NodeParameters {

        private static final String CFG_KEY = "auth";
        private static final String CFG_KEY_USER_PWD = "user_pwd";
        private static final String ENTRY_KEY_USE_CREDENTIALS = "use_credentials";

        enum AuthenticationMethod {
                @Label(value = "Username & password", description = """
                        Authenticate with a username and password. Either enter a username and password, in which case \
                        the password will be persistently stored (in encrypted form) with the workflow. Or select "Use \
                        credentials" and a choose a credentials flow variable to supply the username and password.""")
                USERNAME_PASSWORD,

                @Label(value = "Use credentials", description = """
                        Authenticate with a username and password stored in a credentials flow variable.
                        """)
                CREDENTIALS,
        }

        @Widget(title = "Authentication method", description = "Specify the authentication method to use.")
        @ValueReference(AuthenticationMethodRef.class)
        @Persistor(AuthenticationMethodPersistor.class)
        AuthenticationMethod m_type = AuthenticationMethod.USERNAME_PASSWORD;

        static final class AuthenticationMethodRef implements ParameterReference<AuthenticationMethod> {
        }

        static final class IsUserPwdAuth implements EffectPredicateProvider {
            @Override
            public EffectPredicate init(final PredicateInitializer i) {
                return i.getEnum(AuthenticationMethodRef.class).isOneOf(AuthenticationMethod.USERNAME_PASSWORD);
            }
        }

        static final class IsCredentialsAuth implements EffectPredicateProvider {
            @Override
            public EffectPredicate init(final PredicateInitializer i) {
                return i.getEnum(AuthenticationMethodRef.class).isOneOf(AuthenticationMethod.CREDENTIALS);
            }
        }

        static final class UseCredentialsValueProvider implements StateProvider<Boolean> {

            private Supplier<AuthenticationMethod> m_authenticationMethodSupplier;

            @Override
            public void init(final StateProviderInitializer initializer) {
                m_authenticationMethodSupplier = initializer.computeFromValueSupplier(AuthenticationMethodRef.class);
            }

            @Override
            public Boolean computeState(final NodeParametersInput parametersInput) {
                return m_authenticationMethodSupplier.get() == AuthenticationMethod.CREDENTIALS;
            }
        }

        static final class AuthenticationMethodPersistor implements NodeParametersPersistor<AuthenticationMethod> {

            private static final String ENTRY_KEY = "type";

            @Override
            public AuthenticationMethod load(final NodeSettingsRO settings) throws InvalidSettingsException {
                final var typeString = settings.getString(ENTRY_KEY, "");
                if (typeString.equals(CFG_KEY_USER_PWD)) {
                    final var useCredentials = settings.getNodeSettings(CFG_KEY_USER_PWD)
                            .getBoolean(ENTRY_KEY_USE_CREDENTIALS, false);
                    return useCredentials ? AuthenticationMethod.CREDENTIALS : AuthenticationMethod.USERNAME_PASSWORD;
                }
                throw new InvalidSettingsException(String.format(
                    "Unknown authentication method: '%s'. Possible values: '%s'", typeString, CFG_KEY_USER_PWD));

            }

            @Override
            public void save(final AuthenticationMethod param, final NodeSettingsWO settings) {
                saveInternal(settings);
            }

            private static void saveInternal(final NodeSettingsWO settings) {
                settings.addString(ENTRY_KEY, CFG_KEY_USER_PWD);
            }

            @Override
            public String[][] getConfigPaths() {
                return new String[][] { { ENTRY_KEY }, { CFG_KEY_USER_PWD, ENTRY_KEY_USE_CREDENTIALS } };
            }
        }

        // ----- SECOND LEVEL NESTING NEEDED FOR BACKWARD COMPATIBILITY -----

        @Persist(configKey = CFG_KEY_USER_PWD)
        UserPwdAuthParameters m_userPwdAuth = new UserPwdAuthParameters();

        static final class UserPwdAuthParameters implements NodeParameters {

            private static final String ENTRY_KEY_CREDENTIALS = "credentials";

            @Widget(title = "Username & password", description = """
                    Authentication settings for username and password. Select "Use credentials" as authentication \
                    method to provide the username and password via a credentials flow variable.
                    """)
            @Effect(predicate = IsUserPwdAuth.class, type = EffectType.SHOW)
            @CredentialsWidget
            @Persistor(UserPasswordPersistor.class)
            Credentials m_userPassword = new Credentials(System.getProperty("user.name"), "");

            @ValueProvider(UseCredentialsValueProvider.class)
            @Persist(configKey = ENTRY_KEY_USE_CREDENTIALS)
            boolean m_useCredentials;

            @Widget(title = "Use credentials", description = "Use credentials from a flow variable.")
            @Effect(predicate = IsCredentialsAuth.class, type = EffectType.SHOW)
            @ChoicesProvider(CredentialsFlowVarChoicesProvider.class)
            @Persist(configKey = ENTRY_KEY_CREDENTIALS)
            String m_credentialsFlowVarName;

            static final class UserPasswordPersistor implements NodeParametersPersistor<Credentials> {

                /**
                 * See {@link IDWithSecretAuthProviderSettings#SECRET_ENCRYPTION_KEY}.
                 */
                private static final String USER_PWD_ENCRYPTION_KEY = "laig9eeyeix:ae$Lo6lu";
                private static final String ENTRY_KEY_USER = "user";
                private static final String ENTRY_KEY_PASSWORD = "password";

                @Override
                public Credentials load(final NodeSettingsRO settings) throws InvalidSettingsException {
                    final var user = settings.getString(ENTRY_KEY_USER, System.getProperty("user.name"));
                    final var password = settings.getPassword(ENTRY_KEY_PASSWORD, USER_PWD_ENCRYPTION_KEY, "");
                    return new Credentials(user, password);
                }

                @Override
                public void save(final Credentials param, final NodeSettingsWO settings) {
                    saveInternal(param, settings);
                }

                private static void saveInternal(final Credentials param, final NodeSettingsWO settings) {
                    settings.addString(ENTRY_KEY_USER, param.getUsername());
                    settings.addPassword(ENTRY_KEY_PASSWORD, USER_PWD_ENCRYPTION_KEY, param.getPassword());
                }

                @Override
                public String[][] getConfigPaths() {
                    return new String[][] { { ENTRY_KEY_USER } }; // See AP-14067: It is not possible to overwrite
                                                                  // password fields
                }
            }
        }
    }

}
