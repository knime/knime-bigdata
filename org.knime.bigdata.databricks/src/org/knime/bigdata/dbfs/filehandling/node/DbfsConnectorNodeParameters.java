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

package org.knime.bigdata.dbfs.filehandling.node;

import java.util.Arrays;
import java.util.function.Supplier;

import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.node.DbfsAuthenticationNodeSettings.AuthType;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObjectSpec;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFSConnection;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFSConnectionConfig;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFileSystem;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.webui.node.dialog.defaultdialog.NodeParametersInputImpl;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FSConnectionProvider;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileSelectionWidget;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.SingleFileSelectionMode;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.WithCustomFileSystem;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.credentials.base.node.CredentialsSettings.CredentialsFlowVarChoicesProvider;
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
import org.knime.node.parameters.widget.credentials.PasswordWidget;
import org.knime.node.parameters.widget.message.TextMessage;
import org.knime.node.parameters.widget.message.TextMessage.MessageType;
import org.knime.node.parameters.widget.message.TextMessage.SimpleTextMessageProvider;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsNonNegativeValidation;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.text.TextInputWidgetValidation.PatternValidation.IsNotEmptyValidation;

/**
 * Node parameters for Databricks File System Connector.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 * @author AI Migration Pipeline v1.2
 */
@LoadDefaultsForAbsentFields
@SuppressWarnings("restriction")
class DbfsConnectorNodeParameters implements NodeParameters {

    // ===================== Section definitions =====================

    @Section(title = "Connection")
    @Effect(predicate = HasWorkspaceConnection.class, type = EffectType.HIDE)
    interface ConnectionSection {
    }

    @Section(title = "Authentication")
    @After(ConnectionSection.class)
    @Effect(predicate = HasWorkspaceConnection.class, type = EffectType.HIDE)
    interface AuthenticationSection {
    }

    @Section(title = "File System")
    @After(AuthenticationSection.class)
    interface FileSystemSection {
    }

    @Section(title = "Timeouts")
    @After(FileSystemSection.class)
    @Advanced
    @Effect(predicate = HasWorkspaceConnection.class, type = EffectType.HIDE)
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

    interface ReadTimeoutRef extends ParameterReference<Integer> {
    }

    // ===================== Predicates =====================

    private static boolean hasCredentialPort(final PortType[] types) {
        return Arrays.stream(types).anyMatch(CredentialPortObject.TYPE::equals);
    }

    /**
     * Predicate that checks if a workspace input connection is present.
     */
    static final class HasWorkspaceConnection implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getConstant(context -> hasCredentialPort(context.getInPortTypes()));
        }
    }

    // ===================== Messages =====================

    static final class AuthenticationManagedByPortMessage implements SimpleTextMessageProvider {

        @Override
        public boolean showMessage(final NodeParametersInput context) {
            return hasCredentialPort(context.getInPortTypes());
        }

        @Override
        public String title() {
            return "Authentication settings controlled by input port";
        }

        @Override
        public String description() {
            return "Remove the input port to change the settings";
        }

        @Override
        public MessageType type() {
            return MessageType.INFO;
        }

    }

    // ===================== Fields =====================

    @TextMessage(value = AuthenticationManagedByPortMessage.class)
    Void m_authenticationManagedByPortText;

    @Layout(ConnectionSection.class)
    @Persist(configKey = "url")
    @Widget(title = "Databricks URL",
        description = "Full URL of the Databricks deployment, e.g. "
            + "<i>https://&lt;account&gt;.cloud.databricks.com</i> on AWS or "
            + "<i>https://&lt;region&gt;.azuredatabricks.net</i> on Azure.")
    @TextInputWidget(minLengthValidation = IsNotEmptyValidation.class)
    @ValueReference(UrlRef.class)
    String m_url = "https://";

    @Layout(AuthenticationSection.class)
    @Persist(configKey = AuthenticationParameters.CFG_KEY)
    @ValueReference(AuthenticationParametersRef.class)
    AuthenticationParameters m_authentication = new AuthenticationParameters();

    @Layout(FileSystemSection.class)
    @Persist(configKey = "workingDirectory")
    @Widget(title = "Working directory",
        description = "Specifies the working directory using the path syntax explained above. The working "
            + "directory must be specified as an absolute path. A working directory allows downstream nodes to "
            + "access files/folders using relative paths, i.e. paths that do not have a leading slash. If "
            + "not specified, the default working directory is \"/\".")
    @FileSelectionWidget(SingleFileSelectionMode.FOLDER)
    @WithCustomFileSystem(connectionProvider = FileSystemConnectionProvider.class)
    @ValueReference(WorkingDirectoryRef.class)
    String m_workingDirectory = DbfsFileSystem.PATH_SEPARATOR;

    @Layout(TimeoutsSection.class)
    @Persist(configKey = "connectionTimeout")
    @Widget(title = "Connection timeout (seconds)",
        description = "Timeout in seconds to establish a connection, or 0 for an infinite timeout.")
    @NumberInputWidget(minValidation = IsNonNegativeValidation.class)
    @ValueReference(ConnectionTimeoutRef.class)
    int m_connectionTimeout = 30;

    @Layout(TimeoutsSection.class)
    @Persist(configKey = "readTimeout")
    @Widget(title = "Read timeout (seconds)",
        description = "Timeout in seconds to read data from an established connection, or 0 for an infinite timeout.")
    @NumberInputWidget(minValidation = IsNonNegativeValidation.class)
    @ValueReference(ReadTimeoutRef.class)
    int m_readTimeout = 30;

    // ===================== File System Provider =====================

    /**
     * Provides a DBFS file system connection that can be used in the working directory widget.
     */
    static final class FileSystemConnectionProvider implements StateProvider<FSConnectionProvider> {

        private Supplier<String> m_urlSupplier;

        private Supplier<AuthenticationParameters> m_authParamsSupplier;

        private Supplier<String> m_workingDirectorySupplier;

        private Supplier<Integer> m_connectionTimeoutSupplier;

        private Supplier<Integer> m_readTimeoutSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_urlSupplier = initializer.computeFromValueSupplier(UrlRef.class);
            m_authParamsSupplier = initializer.computeFromValueSupplier(AuthenticationParametersRef.class);
            m_workingDirectorySupplier = initializer.computeFromValueSupplier(WorkingDirectoryRef.class);
            m_connectionTimeoutSupplier = initializer.computeFromValueSupplier(ConnectionTimeoutRef.class);
            m_readTimeoutSupplier = initializer.computeFromValueSupplier(ReadTimeoutRef.class);
            initializer.computeAfterOpenDialog();
        }

        @Override
        public FSConnectionProvider computeState(final NodeParametersInput parametersInput)
            throws StateComputationFailureException {

            return () -> { // NOSONAR: Longer lambda acceptable for readability
                final var inputSpecs = parametersInput.getInPortSpecs();
                final var hasWorkspaceConnection = inputSpecs != null && inputSpecs.length > 0
                        && inputSpecs[0] instanceof DatabricksWorkspacePortObjectSpec;

                final DbfsFSConnectionConfig config;
                if (hasWorkspaceConnection) {
                    final var spec = (DatabricksWorkspacePortObjectSpec)inputSpecs[0];
                    config = createWithWorkspaceConnection(spec);
                } else {
                    final var credentialsProvider = getCredentialsProvider(parametersInput);
                    config = createConfigFromSettings(credentialsProvider);
                }

                return new DbfsFSConnection(config);
            };
        }

        private DbfsFSConnectionConfig createWithWorkspaceConnection(final DatabricksWorkspacePortObjectSpec spec)
            throws InvalidSettingsException {

            final var settings = new DbfsConnectorNodeSettings(false);
            settings.getWorkingDirectoryModel().setStringValue(m_workingDirectorySupplier.get());
            settings.getConnectionTimeoutModel().setIntValue(m_connectionTimeoutSupplier.get());
            settings.getReadTimeoutModel().setIntValue(m_readTimeoutSupplier.get());
            settings.validate();

            try {
                final DatabricksAccessTokenCredential credential =
                    spec.resolveCredential(DatabricksAccessTokenCredential.class);
                return settings.toFSConnectionConfig(spec, credential);

            } catch (final NoSuchCredentialException ex) {
                throw new InvalidSettingsException(ex.getMessage(), ex);
            }
        }

        private DbfsFSConnectionConfig createConfigFromSettings(final CredentialsProvider credentialsProvider)
            throws InvalidSettingsException {

            final var settings = new DbfsConnectorNodeSettings(false);
            settings.getUrlModel().setStringValue(m_urlSupplier.get());
            settings.getWorkingDirectoryModel().setStringValue(m_workingDirectorySupplier.get());
            settings.getConnectionTimeoutModel().setIntValue(m_connectionTimeoutSupplier.get());
            settings.getReadTimeoutModel().setIntValue(m_readTimeoutSupplier.get());

            final var authModel = settings.getAuthenticationSettings();
            final var authParams = m_authParamsSupplier.get();
            if (authParams.m_type == AuthenticationParameters.AuthenticationType.TOKEN) {
                authModel.setAuthType(AuthType.TOKEN);
                authModel.getTokenUseCredentialsModel().setBooleanValue(false);
                authModel.getTokenModel().setStringValue(authParams.m_tokenAuth.m_token.getPassword());
            } else if (authParams.m_type == AuthenticationParameters.AuthenticationType.TOKEN_CREDENTIALS) {
                authModel.setAuthType(AuthType.TOKEN);
                authModel.getTokenUseCredentialsModel().setBooleanValue(true);
                authModel.getTokenCredentialsNameModel()
                    .setStringValue(authParams.m_tokenAuth.m_credentialsFlowVarName);
            } else if (authParams.m_type == AuthenticationParameters.AuthenticationType.USER_PWD) {
                authModel.setAuthType(AuthType.USER_PWD);
                authModel.getUserPassUseCredentialsModel().setBooleanValue(false);
                authModel.getUserModel().setStringValue(authParams.m_userPwdAuth.m_userPassword.getUsername());
                authModel.getPasswordModel().setStringValue(authParams.m_userPwdAuth.m_userPassword.getPassword());
            } else if (authParams.m_type == AuthenticationParameters.AuthenticationType.USER_PWD_CREDENTIALS) {
                authModel.setAuthType(AuthType.USER_PWD);
                authModel.getUserPassUseCredentialsModel().setBooleanValue(true);
                authModel.getUserPassCredentialsNameModel()
                    .setStringValue(authParams.m_userPwdAuth.m_credentialsFlowVarName);
            }

            settings.validate();

            return settings.toFSConnectionConfig(credentialsProvider);
        }

        private static CredentialsProvider getCredentialsProvider(final NodeParametersInput input) {
            return ((NodeParametersInputImpl)input).getCredentialsProvider().orElseThrow();
        }

    }

    // ===================== Authentication Parameters =====================

    static final class AuthenticationParameters implements NodeParameters {

        static final String CFG_KEY = "auth";
        private static final String CFG_KEY_USER_PWD = "user_pwd";
        private static final String CFG_KEY_TOKEN = "token";

        /**
         * Authentication type enumeration covering all modes.
         */
        enum AuthenticationType {
                @Label(value = "Username & password",
                    description = "Authenticate with a username and password. The password will be persistently "
                        + "stored (in encrypted form) with the workflow.")
                USER_PWD,

                @Label(value = "Username & password (credentials)",
                    description = "Authenticate with a username and password from a credentials flow variable.")
                USER_PWD_CREDENTIALS,

                @Label(value = "Personal access token",
                    description = "Authenticate with a personal access token. The token will be persistently "
                        + "stored (in encrypted form) with the workflow. Databricks strongly recommends tokens.")
                TOKEN,

                @Label(value = "Personal access token (credentials)",
                    description = "Authenticate with a personal access token from a credentials flow variable. "
                        + "The password of the credentials flow variable will be used as the token.")
                TOKEN_CREDENTIALS,
        }

        @Widget(title = "Authentication method",
            description = "Specify the authentication method to use. Username and password or a personal access "
                + "token can be used for authentication. Databricks strongly recommends tokens.")
        @ValueReference(AuthenticationTypeRef.class)
        @Persistor(AuthenticationTypePersistor.class)
        AuthenticationType m_type = AuthenticationType.TOKEN;

        static final class AuthenticationTypeRef implements ParameterReference<AuthenticationType> {
        }

        static final class IsUserPwd implements EffectPredicateProvider {
            @Override
            public EffectPredicate init(final PredicateInitializer i) {
                return i.getEnum(AuthenticationTypeRef.class).isOneOf(AuthenticationType.USER_PWD);
            }
        }

        static final class IsUserPwdCredentials implements EffectPredicateProvider {
            @Override
            public EffectPredicate init(final PredicateInitializer i) {
                return i.getEnum(AuthenticationTypeRef.class).isOneOf(AuthenticationType.USER_PWD_CREDENTIALS);
            }
        }

        static final class IsToken implements EffectPredicateProvider {
            @Override
            public EffectPredicate init(final PredicateInitializer i) {
                return i.getEnum(AuthenticationTypeRef.class).isOneOf(AuthenticationType.TOKEN);
            }
        }

        static final class IsTokenCredentials implements EffectPredicateProvider {
            @Override
            public EffectPredicate init(final PredicateInitializer i) {
                return i.getEnum(AuthenticationTypeRef.class).isOneOf(AuthenticationType.TOKEN_CREDENTIALS);
            }
        }

        /**
         * Persistor for the authentication type. Maps the 4 UI options to the 2 old config values + useCredentials
         * flags.
         */
        static final class AuthenticationTypePersistor implements NodeParametersPersistor<AuthenticationType> {

            private static final String ENTRY_KEY_TYPE = "type";
            private static final String ENTRY_KEY_USE_CREDENTIALS = "use_credentials";

            @Override
            public AuthenticationType load(final NodeSettingsRO settings) throws InvalidSettingsException {
                final var typeString = settings.getString(ENTRY_KEY_TYPE, CFG_KEY_TOKEN);

                if (typeString.equals(CFG_KEY_USER_PWD)) {
                    final var useCredentials = settings.getNodeSettings(CFG_KEY_USER_PWD)
                        .getBoolean(ENTRY_KEY_USE_CREDENTIALS, false);
                    return useCredentials ? AuthenticationType.USER_PWD_CREDENTIALS : AuthenticationType.USER_PWD;
                } else if (typeString.equals(CFG_KEY_TOKEN)) {
                    final var useCredentials = settings.getNodeSettings(CFG_KEY_TOKEN)
                        .getBoolean(ENTRY_KEY_USE_CREDENTIALS, false);
                    return useCredentials ? AuthenticationType.TOKEN_CREDENTIALS : AuthenticationType.TOKEN;
                }
                throw new InvalidSettingsException(String.format(
                    "Unknown authentication type: '%s'. Possible values: '%s', '%s'",
                    typeString, CFG_KEY_USER_PWD, CFG_KEY_TOKEN));
            }

            @Override
            public void save(final AuthenticationType param, final NodeSettingsWO settings) {
                switch (param) {
                    case USER_PWD, USER_PWD_CREDENTIALS:
                        settings.addString(ENTRY_KEY_TYPE, CFG_KEY_USER_PWD);
                        // Note that the use_credentials flag is saved via the UserPwdAuthParameters
                        break;
                    case TOKEN, TOKEN_CREDENTIALS:
                        settings.addString(ENTRY_KEY_TYPE, CFG_KEY_TOKEN);
                        // Note that the use_credentials flag is saved via the TokenAuthParameters
                        break;
                }
            }

            @Override
            public String[][] getConfigPaths() {
                return new String[][]{ //
                    {ENTRY_KEY_TYPE}, //
                    {CFG_KEY_USER_PWD, ENTRY_KEY_USE_CREDENTIALS}, //
                    {CFG_KEY_TOKEN, ENTRY_KEY_USE_CREDENTIALS}};
            }
        }

        // ===== User/Password Authentication =====

        @Persist(configKey = CFG_KEY_USER_PWD)
        UserPwdAuthParameters m_userPwdAuth = new UserPwdAuthParameters();

        static final class UserPwdAuthParameters implements NodeParameters {

            private static final String ENTRY_KEY_USE_CREDENTIALS = "use_credentials";
            private static final String ENTRY_KEY_CREDENTIALS = "credentials";
            private static final String USER_PWD_ENCRYPTION_KEY = "laig9eeyeix:ae$Lo6lu";

            @Widget(title = "Username & password",
                description = "Enter username and password for authentication. The password will be persistently "
                    + "stored (in encrypted form) with the workflow.")
            @Effect(predicate = IsUserPwd.class, type = EffectType.SHOW)
            @CredentialsWidget
            @Persistor(UserPasswordPersistor.class)
            Credentials m_userPassword = new Credentials("", "");

            @ValueProvider(UseCredentialsValueProviderUserPwd.class)
            @Persist(configKey = ENTRY_KEY_USE_CREDENTIALS)
            boolean m_useCredentials;

            @Widget(title = "Credentials",
                description = "Select a credentials flow variable to supply username and password.")
            @Effect(predicate = IsUserPwdCredentials.class, type = EffectType.SHOW)
            @ChoicesProvider(CredentialsFlowVarChoicesProvider.class)
            @Persist(configKey = ENTRY_KEY_CREDENTIALS)
            String m_credentialsFlowVarName;

            static final class UseCredentialsValueProviderUserPwd implements StateProvider<Boolean> {

                private Supplier<AuthenticationType> m_authTypeSupplier;

                @Override
                public void init(final StateProviderInitializer initializer) {
                    m_authTypeSupplier = initializer.computeFromValueSupplier(AuthenticationTypeRef.class);
                }

                @Override
                public Boolean computeState(final NodeParametersInput parametersInput) {
                    return m_authTypeSupplier.get() == AuthenticationType.USER_PWD_CREDENTIALS;
                }
            }

            static final class UserPasswordPersistor implements NodeParametersPersistor<Credentials> {

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
                    settings.addString(ENTRY_KEY_USER, param.getUsername());
                    settings.addPassword(ENTRY_KEY_PASSWORD, USER_PWD_ENCRYPTION_KEY, param.getPassword());
                }

                @Override
                public String[][] getConfigPaths() {
                    // See AP-14067: It is not possible to overwrite password fields
                    return new String[][] { { ENTRY_KEY_USER } };
                }
            }
        }

        // ===== Token Authentication =====

        @Persist(configKey = CFG_KEY_TOKEN)
        TokenAuthParameters m_tokenAuth = new TokenAuthParameters();

        static final class TokenAuthParameters implements NodeParameters {

            private static final String ENTRY_KEY_USE_CREDENTIALS = "use_credentials";
            private static final String ENTRY_KEY_CREDENTIALS = "credentials";
            private static final String TOKEN_ENCRYPTION_KEY = "xe:sh'o4uch1Ox2Shoh:";

            @Widget(title = "Personal access token",
                description = "Enter a personal access token for authentication. The token will be persistently "
                    + "stored (in encrypted form) with the workflow.")
            @Effect(predicate = IsToken.class, type = EffectType.SHOW)
            @PasswordWidget(passwordLabel = "Token")
            @Persistor(TokenPersistor.class)
            Credentials m_token = new Credentials("", "");

            @ValueProvider(UseCredentialsValueProviderToken.class)
            @Persist(configKey = ENTRY_KEY_USE_CREDENTIALS)
            boolean m_useCredentials;

            @Widget(title = "Credentials",
                description = "Select a credentials flow variable. The password of the credentials will be "
                    + "used as the token (username is ignored).")
            @Effect(predicate = IsTokenCredentials.class, type = EffectType.SHOW)
            @ChoicesProvider(CredentialsFlowVarChoicesProvider.class)
            @Persist(configKey = ENTRY_KEY_CREDENTIALS)
            String m_credentialsFlowVarName;

            static final class UseCredentialsValueProviderToken implements StateProvider<Boolean> {

                private Supplier<AuthenticationType> m_authTypeSupplier;

                @Override
                public void init(final StateProviderInitializer initializer) {
                    m_authTypeSupplier = initializer.computeFromValueSupplier(AuthenticationTypeRef.class);
                }

                @Override
                public Boolean computeState(final NodeParametersInput parametersInput) {
                    return m_authTypeSupplier.get() == AuthenticationType.TOKEN_CREDENTIALS;
                }
            }

            static final class TokenPersistor implements NodeParametersPersistor<Credentials> {

                private static final String ENTRY_KEY_SECRET = "secret";

                @Override
                public Credentials load(final NodeSettingsRO settings) throws InvalidSettingsException {
                    final var token = settings.getPassword(ENTRY_KEY_SECRET, TOKEN_ENCRYPTION_KEY, "");
                    return new Credentials("", token);
                }

                @Override
                public void save(final Credentials param, final NodeSettingsWO settings) {
                    settings.addPassword(ENTRY_KEY_SECRET, TOKEN_ENCRYPTION_KEY, param.getPassword());
                }

                @Override
                public String[][] getConfigPaths() {
                    // Password fields cannot be overwritten via flow variables
                    return new String[][] {};
                }
            }
        }
    }

}
