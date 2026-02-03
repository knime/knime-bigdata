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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.node.DbfsAuthenticationNodeSettings;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObject;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObjectSpec;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFSConnection;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFSConnectionConfig;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFileSystem;
import org.knime.bigdata.dbfs.filehandling.node.DbfsConnectorNodeParameters.AuthenticationParameters.AuthenticationMethod;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelPassword;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.webui.node.dialog.defaultdialog.NodeParametersInputImpl;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FSConnectionProvider;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileSelectionWidget;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.SingleFileSelectionMode;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.WithCustomFileSystem;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.LegacyCredentials;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.validation.custom.CustomValidation;
import org.knime.core.webui.node.dialog.defaultdialog.widget.validation.custom.SimpleValidation;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.node.parameters.Advanced;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.migration.ConfigMigration;
import org.knime.node.parameters.migration.LoadDefaultsForAbsentFields;
import org.knime.node.parameters.migration.Migration;
import org.knime.node.parameters.migration.NodeParametersMigration;
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
import org.knime.node.parameters.widget.credentials.Credentials;
import org.knime.node.parameters.widget.credentials.PasswordWidget;
import org.knime.node.parameters.widget.message.TextMessage;
import org.knime.node.parameters.widget.message.TextMessage.MessageType;
import org.knime.node.parameters.widget.message.TextMessage.SimpleTextMessageProvider;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsNonNegativeValidation;

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

    private static final Predicate<PortType> isCredentialPort = CredentialPortObject.TYPE::equals;

    private static final Predicate<PortType> isWorkspacePort = DatabricksWorkspacePortObject.TYPE::equals;

    private static boolean hasCredentialPort(final PortType[] types) {
        return Arrays.stream(types).anyMatch(isCredentialPort.or(isWorkspacePort));
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
            return "Settings controlled by input port";
        }

        @Override
        public String description() {
            return "Remove the input port to change the connection, authentication and timeout settings";
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
    @Widget(title = "Databricks URL",
        description = "Full URL of the Databricks deployment, e.g. "
            + "<i>https://&lt;account&gt;.cloud.databricks.com</i> on AWS or "
            + "<i>https://&lt;region&gt;.azuredatabricks.net</i> on Azure.")
    @ValueReference(UrlRef.class)
    @CustomValidation(UrlValidator.class)
    String m_url = "";

    @Layout(AuthenticationSection.class)
    @Persist(configKey = AuthenticationParameters.CFG_KEY)
    @ValueReference(AuthenticationParametersRef.class)
    AuthenticationParameters m_authentication = new AuthenticationParameters();

    @Layout(FileSystemSection.class)
    @Widget(title = "Working directory",
        description = "Specifies the working directory using the path syntax explained above. The working "
            + "directory must be specified as an absolute path. A working directory allows downstream nodes to "
            + "access files/folders using relative paths, i.e. paths that do not have a leading slash. If "
            + "not specified, the default working directory is \"/\".")
    @FileSelectionWidget(SingleFileSelectionMode.FOLDER)
    @WithCustomFileSystem(connectionProvider = FileSystemConnectionProvider.class)
    @ValueReference(WorkingDirectoryRef.class)
    @CustomValidation(WorkingDirectoryValidator.class)
    String m_workingDirectory = DbfsFileSystem.PATH_SEPARATOR;

    @Layout(TimeoutsSection.class)
    @Widget(title = "Connection timeout (seconds)",
        description = "Timeout in seconds to establish a connection, or 0 for an infinite timeout.")
    @NumberInputWidget(minValidation = IsNonNegativeValidation.class)
    @ValueReference(ConnectionTimeoutRef.class)
    int m_connectionTimeout = 30;

    @Layout(TimeoutsSection.class)
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
                final PortObjectSpec[] inputSpecs = parametersInput.getInPortSpecs();
                final boolean hasWorkspaceConnection = inputSpecs != null && inputSpecs.length > 0
                        && inputSpecs[0] instanceof DatabricksWorkspacePortObjectSpec;

                final DbfsFSConnectionConfig config;
                if (hasWorkspaceConnection) {
                    final DatabricksWorkspacePortObjectSpec spec = (DatabricksWorkspacePortObjectSpec)inputSpecs[0];
                    config = createWithWorkspaceConnection(spec);
                } else {
                    final CredentialsProvider credentialsProvider = getCredentialsProvider(parametersInput);
                    config = createConfigFromSettings(credentialsProvider);
                }

                return new DbfsFSConnection(config);
            };
        }

        private DbfsFSConnectionConfig createWithWorkspaceConnection(
            final DatabricksWorkspacePortObjectSpec spec) throws InvalidSettingsException {

            final DbfsConnectorNodeParameters params = new DbfsConnectorNodeParameters();
            params.m_workingDirectory = getWorkingDirectory();
            params.validateOnConfigureWithWorkspaceConnection();

            try {
                final DatabricksAccessTokenCredential credential =
                    spec.resolveCredential(DatabricksAccessTokenCredential.class);
                return params.toFSConnectionConfig(spec, credential);

            } catch (final NoSuchCredentialException ex) {
                throw new InvalidSettingsException(ex.getMessage(), ex);
            }
        }

        private DbfsFSConnectionConfig createConfigFromSettings(final CredentialsProvider credentialsProvider)
            throws InvalidSettingsException {

            final DbfsConnectorNodeParameters params = new DbfsConnectorNodeParameters();
            params.m_url = m_urlSupplier.get();
            params.m_authentication = m_authParamsSupplier.get();
            params.m_workingDirectory = getWorkingDirectory();
            params.m_connectionTimeout = m_connectionTimeoutSupplier.get();
            params.m_readTimeout = m_readTimeoutSupplier.get();
            params.validateOnConfigureWithWorkspaceConnection();

            return params.toFSConnectionConfig(credentialsProvider);
        }

        private String getWorkingDirectory() {
            final String workingDir = m_workingDirectorySupplier.get();
            return isValidWorkingDirectory(workingDir) ? workingDir : DbfsFileSystem.PATH_SEPARATOR;
        }

        private static CredentialsProvider getCredentialsProvider(final NodeParametersInput input) {
            return ((NodeParametersInputImpl)input).getCredentialsProvider().orElseThrow();
        }

    }

    // ===================== Authentication Parameters =====================

    static final class AuthenticationParameters implements NodeParameters {

        static final String CFG_KEY = "auth";

        private static final String CFG_KEY_TOKEN = "token";

        private static final String CFG_KEY_TOKEN_V2 = "token_v2";

        private static final String CFG_KEY_USER_PWD = "user_pwd";

        private static final String CFG_KEY_USER_PWD_V2 = "user_pwd_v2";

        /**
         * Authentication type enumeration covering all modes.
         */
        enum AuthenticationMethod {
                @Label(value = "Personal access token",
                    description = "Authenticate with a personal access token. The token will be persistently "
                        + "stored (in encrypted form) with the workflow if not provided by a flow variable. "
                        + "Databricks strongly recommends tokens.")
                TOKEN,

                @Label(value = "Username & password",
                    description = "Authenticate with a username and password. The password will be persistently "
                        + "stored (in encrypted form) with the workflow.")
                USERNAME_PASSWORD,

        }

        @Widget(title = "Authentication method",
            description = "Specify the authentication method to use. Personal access token or "
                + "Username and password can be used for authentication. Databricks strongly recommends tokens.")
        @ValueReference(AuthenticationMethodRef.class)
        @Persistor(AuthenticationMethodPersistor.class)
        AuthenticationMethod m_type = AuthenticationMethod.TOKEN;

        static final class AuthenticationMethodRef implements ParameterReference<AuthenticationMethod> {
        }

        static final class IsTokenAuth implements EffectPredicateProvider {
            @Override
            public EffectPredicate init(final PredicateInitializer i) {
                return i.getEnum(AuthenticationMethodRef.class).isOneOf(AuthenticationMethod.TOKEN);
            }
        }

        static final class IsUserPwdAuth implements EffectPredicateProvider {
            @Override
            public EffectPredicate init(final PredicateInitializer i) {
                return i.getEnum(AuthenticationMethodRef.class).isOneOf(AuthenticationMethod.USERNAME_PASSWORD);
            }
        }

        /**
         * Persistor that supports V2 and legacy types in load, but always writes V2 types.
         */
        static final class AuthenticationMethodPersistor implements NodeParametersPersistor<AuthenticationMethod> {

            private static final String ENTRY_KEY = "type";

            @Override
            public AuthenticationMethod load(final NodeSettingsRO settings) throws InvalidSettingsException {
                final String typeString = settings.getString(ENTRY_KEY, "");
                if (typeString.equalsIgnoreCase(CFG_KEY_TOKEN) || typeString.equalsIgnoreCase(CFG_KEY_TOKEN_V2)) {
                    return AuthenticationMethod.TOKEN;
                } else if (typeString.equalsIgnoreCase(CFG_KEY_USER_PWD)
                    || typeString.equalsIgnoreCase(CFG_KEY_USER_PWD_V2)) {
                    return AuthenticationMethod.USERNAME_PASSWORD;
                }
                throw new InvalidSettingsException(
                    String.format("Unknown authentication method: '%s'. Possible values: '%s', '%s'", typeString,
                        CFG_KEY_TOKEN_V2, CFG_KEY_USER_PWD_V2));
            }

            @Override
            public void save(final AuthenticationMethod param, final NodeSettingsWO settings) {
                saveInternal(param, settings);
            }

            private static void saveInternal(final AuthenticationMethod param, final NodeSettingsWO settings) {
                if (param == AuthenticationMethod.TOKEN) {
                    settings.addString(ENTRY_KEY, CFG_KEY_TOKEN_V2);
                } else if (param == AuthenticationMethod.USERNAME_PASSWORD) {
                    settings.addString(ENTRY_KEY, CFG_KEY_USER_PWD_V2);
                }
            }

            @Override
            public String[][] getConfigPaths() {
                return new String[][]{{ENTRY_KEY}};
            }
        }

        // ===== Token Authentication =====

        @Persist(configKey = CFG_KEY_TOKEN_V2)
        @Migration(LoadTokenFromDbfsAuthenticationNodeSettingsMigration.class)
        @Effect(predicate = IsTokenAuth.class, type = EffectType.SHOW)
        @Widget(title = "Personal access token",
            description = "Enter a personal access token for authentication."
                + " The token will be persistently stored (in encrypted form) with the workflow.")
        @PasswordWidget(passwordLabel = "Token")
        LegacyCredentials m_tokenAuth = new LegacyCredentials(new Credentials());

        /**
         * Migration that loads the token settings from {@link DbfsAuthenticationNodeSettings} based legacy settings.
         */
        static final class LoadTokenFromDbfsAuthenticationNodeSettingsMigration
            implements NodeParametersMigration<LegacyCredentials> {

            private static final String KEY_USE_CREDENTIALS = "use_credentials";

            private static final String KEY_CREDENTIALS = "credentials";

            private static final String KEY_SECRET = "secret";

            private static final String TOKEN_ENCRYPTION_KEY = "xe:sh'o4uch1Ox2Shoh:";

            private final SettingsModelBoolean m_useCredentials = //
                new SettingsModelBoolean(KEY_USE_CREDENTIALS, false);

            private final SettingsModelString m_credentialsName = //
                new SettingsModelString(KEY_CREDENTIALS, "");

            private final SettingsModelPassword m_token = //
                new SettingsModelPassword(KEY_SECRET, TOKEN_ENCRYPTION_KEY, "");

            @Override
            public List<ConfigMigration<LegacyCredentials>> getConfigMigrations() {
                return List.of(ConfigMigration.builder(this::loadLegacyCredentials)
                    .withDeprecatedConfigPath(CFG_KEY_TOKEN).build());
            }

            private LegacyCredentials loadLegacyCredentials(final NodeSettingsRO authSettings)
                throws InvalidSettingsException {

                final NodeSettingsRO userPassSettings = authSettings.getNodeSettings(CFG_KEY_TOKEN);
                m_useCredentials.loadSettingsFrom(userPassSettings);
                m_credentialsName.loadSettingsFrom(userPassSettings);
                m_token.loadSettingsFrom(userPassSettings);

                if (m_useCredentials.getBooleanValue()) {
                    return new LegacyCredentials(m_credentialsName.getStringValue());
                }

                final String user = "";
                final String password = m_token.getStringValue();
                return new LegacyCredentials(new Credentials(user, password));
            }

        }

        // ===== User/Password Authentication =====

        @Persist(configKey = CFG_KEY_USER_PWD_V2)
        @Migration(LoadUserPwdFromDbfsAuthenticationNodeSettingsMigration.class)
        @Effect(predicate = IsUserPwdAuth.class, type = EffectType.SHOW)
        @Widget(title = "Username & Password", description = "Credentials for username and password authentication.")
        LegacyCredentials m_userPasswordAuth = new LegacyCredentials(new Credentials());

        /**
         * Migration that loads the username/password settings from {@link DbfsAuthenticationNodeSettings} based legacy
         * settings.
         */
        static final class LoadUserPwdFromDbfsAuthenticationNodeSettingsMigration
            implements NodeParametersMigration<LegacyCredentials> {

            private static final String KEY_USE_CREDENTIALS = "use_credentials";

            private static final String KEY_CREDENTIALS = "credentials";

            private static final String KEY_USER = "user";

            private static final String KEY_PASSWORD = "password";

            private static final String USER_PASSWORD_ENCRYPTION_KEY = "laig9eeyeix:ae$Lo6lu";

            private final SettingsModelBoolean m_useCredentials = //
                new SettingsModelBoolean(KEY_USE_CREDENTIALS, false);

            private final SettingsModelString m_credentialsName = //
                new SettingsModelString(KEY_CREDENTIALS, "");

            private final SettingsModelString m_user = //
                new SettingsModelString(KEY_USER, System.getProperty("user.name"));

            private final SettingsModelPassword m_password = //
                new SettingsModelPassword(KEY_PASSWORD, USER_PASSWORD_ENCRYPTION_KEY, "");

            @Override
            public List<ConfigMigration<LegacyCredentials>> getConfigMigrations() {
                return List.of(ConfigMigration.builder(this::loadLegacyCredentials)
                    .withDeprecatedConfigPath(CFG_KEY_USER_PWD).build());
            }

            private LegacyCredentials loadLegacyCredentials(final NodeSettingsRO authSettings)
                throws InvalidSettingsException {

                final NodeSettingsRO userPassSettings = authSettings.getNodeSettings(CFG_KEY_USER_PWD);
                m_useCredentials.loadSettingsFrom(userPassSettings);
                m_credentialsName.loadSettingsFrom(userPassSettings);
                m_user.loadSettingsFrom(userPassSettings);
                m_password.loadSettingsFrom(userPassSettings);

                if (m_useCredentials.getBooleanValue()) {
                    return new LegacyCredentials(m_credentialsName.getStringValue());
                }

                final String user = m_user.getStringValue();
                final String password = m_password.getStringValue();
                return new LegacyCredentials(new Credentials(user, password));
            }
        }

    }

    // ===================== Validation =====================

    /**
     * Validate settings without a workspace connection.
     */
    void validateOnConfigure(final CredentialsProvider credProv) throws InvalidSettingsException {
        // URL validation
        validateUrl(m_url);

        // authentication validation
        if (m_authentication.m_type == AuthenticationMethod.TOKEN) {
            final Credentials cred = m_authentication.m_tokenAuth.toCredentials(credProv);
            CheckUtils.checkSetting(isNotBlank(cred.getPassword()), "Token required for authentication.");
        } else if (m_authentication.m_type == AuthenticationMethod.USERNAME_PASSWORD) {
            final Credentials cred = m_authentication.m_tokenAuth.toCredentials(credProv);
            CheckUtils.checkSetting(isNotBlank(cred.getUsername()), "Username required for authentication.");
            CheckUtils.checkSetting(isNotBlank(cred.getPassword()), "Password required for authentication.");
        }

        // working dir validation
        validateWorkingDirectory(m_workingDirectory);

        // timeout validation
        if (m_connectionTimeout < 0) {
            throw new InvalidSettingsException("Connection timeout must be greater than or equal to zero.");
        }
        if (m_readTimeout < 0) {
            throw new InvalidSettingsException("Read timeout must be greater than or equal to zero.");
        }
    }

    /**
     * Validate settings with a workspace connection.
     */
    void validateOnConfigureWithWorkspaceConnection() throws InvalidSettingsException {
        // working dir validation
        validateWorkingDirectory(m_workingDirectory);
    }

    private static void validateUrl(final String url) throws InvalidSettingsException {
        CheckUtils.checkSetting(isNotBlank(url), "Databricks deployment URL required.");

        try {
            final URI uri = new URI(url);

            if (StringUtils.isBlank(uri.getScheme()) || !uri.getScheme().equalsIgnoreCase("https")) {
                throw new InvalidSettingsException(
                    "HTTPS Protocol in Databricks deployment URL required (only https supported)");
            } else if (StringUtils.isBlank(uri.getHost())) {
                throw new InvalidSettingsException("Hostname in Databricks deployment URL required.");
            }

        } catch (final URISyntaxException e) {
            throw new InvalidSettingsException( //
                String.format("Invalid Databricks deployment URL: %s", e.getMessage()), e);
        }
    }

    private static boolean isValidWorkingDirectory(final String workingDirectory) {
        return workingDirectory != null && workingDirectory.startsWith(DbfsFileSystem.PATH_SEPARATOR);
    }

    private static void validateWorkingDirectory(final String workingDirectory) throws InvalidSettingsException {
        if (!isValidWorkingDirectory(workingDirectory)) {
            throw new InvalidSettingsException("Working directory must be set to an absolute path.");
        }
    }

    // ===================== Custom Validations =====================

    static class UrlValidator extends SimpleValidation<String> {
        @Override
        public void validate(final String url) throws InvalidSettingsException {
            validateUrl(url);
        }
    }

    static class WorkingDirectoryValidator extends SimpleValidation<String> {
        @Override
        public void validate(final String workingDir) throws InvalidSettingsException {
            validateWorkingDirectory(workingDir);
        }
    }

    // ===================== File System Config =====================

    /**
     * Create file system config using a workspace connection.
     *
     * @param spec port spec with timeout settings
     * @param credential credential to authenticate with
     * @return file system settings
     */
    public DbfsFSConnectionConfig toFSConnectionConfig(final DatabricksWorkspacePortObjectSpec spec,
        final DatabricksAccessTokenCredential credential) {

        return DbfsFSConnectionConfig.builder() //
            .withCredential(credential) //
            .withWorkingDirectory(m_workingDirectory) //
            .withConnectionTimeout(spec.getConnectionTimeout()) //
            .withReadTimeout(spec.getReadTimeout()) //
            .build();
    }

    /**
     * Convert this settings to a {@link DbfsFSConnectionConfig} instance without a workspace connection.
     *
     * @param credProv provider of credential variables
     * @return a {@link DbfsFSConnectionConfig} using this settings
     */
    public DbfsFSConnectionConfig toFSConnectionConfig(final CredentialsProvider credProv) {
        final DbfsFSConnectionConfig.Builder builder = DbfsFSConnectionConfig.builder() //
            .withDeploymentUrl(m_url) //
            .withWorkingDirectory(m_workingDirectory) //
            .withConnectionTimeout(Duration.ofSeconds(m_connectionTimeout)) //
            .withReadTimeout(Duration.ofSeconds(m_readTimeout));

        if (m_authentication.m_type == AuthenticationMethod.TOKEN) {
            final Credentials cred = m_authentication.m_tokenAuth.toCredentials(credProv);
            builder.withToken(cred.getPassword());
        } else if (m_authentication.m_type == AuthenticationMethod.USERNAME_PASSWORD) {
            final Credentials cred = m_authentication.m_userPasswordAuth.toCredentials(credProv);
            builder.withUserAndPassword(cred.getUsername(), cred.getPassword());
        }

        return builder.build();
    }

}
