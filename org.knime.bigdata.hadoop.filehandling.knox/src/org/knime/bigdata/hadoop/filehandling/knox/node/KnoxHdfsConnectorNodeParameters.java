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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.function.Supplier;

import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFSConnection;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFSConnectionConfig;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFileSystem;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.webui.node.dialog.defaultdialog.NodeParametersInputImpl;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FSConnectionProvider;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileSelectionWidget;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.SingleFileSelectionMode;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.WithCustomFileSystem;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.LegacyCredentials;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.LegacyCredentialsAuthProviderSettings;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.validation.custom.CustomValidation;
import org.knime.core.webui.node.dialog.defaultdialog.widget.validation.custom.SimpleValidation;
import org.knime.filehandling.core.connections.base.auth.StandardAuthTypes;
import org.knime.filehandling.core.connections.base.auth.UserPasswordAuthProviderSettings;
import org.knime.node.parameters.Advanced;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.migration.LoadDefaultsForAbsentFields;
import org.knime.node.parameters.migration.Migration;
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.credentials.Credentials;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsNonNegativeValidation;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.text.TextInputWidgetValidation.PatternValidation.IsNotBlankValidation;

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

    @Section(title = "Timeouts")
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
    @Widget(title = "URL", description = """
        KNOX URL including cluster name, for example
        <tt>https://&lt;server&gt;:8443/gateway/&lt;default&gt;</tt>
        (replace &lt;server&gt; with the fully qualified hostname and &lt;default&gt;
        with your cluster name). The protocol should be <i>https</i> and port <i>8443</i>
        on most deployments.""")
    @TextInputWidget(patternValidation = IsNotBlankValidation.class)
    @ValueReference(UrlRef.class)
    @CustomValidation(UrlValidator.class)
    String m_url = "https://server:8443/gateway/default";

    @Layout(AuthenticationSection.class)
    @Persist(configKey = AuthenticationParameters.CFG_KEY)
    @ValueReference(AuthenticationParametersRef.class)
    AuthenticationParameters m_authentication = new AuthenticationParameters();

    @Layout(FileSystemSection.class)
    @Persist(configKey = "workingDirectory")
    @Widget(title = "Working directory", description = """
        Specify the working directory of the resulting file system connection,
        using the UNIX-like path syntax. The working directory must be specified as an absolute path
        (starting with "/"). A working directory allows downstream nodes to access files/folders
        using relative paths, i.e. paths that do not have a leading slash.
        The default working directory is the root "/".""")
    @FileSelectionWidget(SingleFileSelectionMode.FOLDER)
    @WithCustomFileSystem(connectionProvider = FileSystemConnectionProvider.class)
    @ValueReference(WorkingDirectoryRef.class)
    @CustomValidation(WorkingDirectoryValidator.class)
    String m_workingDirectory = KnoxHdfsFileSystem.PATH_SEPARATOR;

    @Layout(TimeoutsSection.class)
    @Widget(title = "Connection timeout (seconds)",
        description = "Timeout in seconds to establish a connection, or 0 for an infinite timeout.")
    @NumberInputWidget(minValidation = IsNonNegativeValidation.class)
    @ValueReference(ConnectionTimeoutRef.class)
    int m_connectionTimeout = 30;

    @Layout(TimeoutsSection.class)
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
                final var params = new KnoxHdfsConnectorNodeParameters();
                params.m_url = m_urlSupplier.get();
                params.m_authentication = m_authParamsSupplier.get();
                params.m_connectionTimeout = m_connectionTimeoutSupplier.get();
                params.m_receiveTimeout = m_receiveTimeoutSupplier.get();

                params.m_workingDirectory = m_workingDirectorySupplier.get();
                if (!isValidWorkingDirectory(params.m_workingDirectory)) {
                    params.m_workingDirectory = KnoxHdfsFileSystem.PATH_SEPARATOR;
                }

                final var cp = getCredentialsProvider(parametersInput);
                params.validateOnConfigure(cp);

                try {
                    return new KnoxHdfsFSConnection(params.toFSConnectionConfig(cp));
                } catch (final IllegalArgumentException e) {
                    throw new InvalidSettingsException(e.getMessage(), e);
                }
            };
        }

        private static CredentialsProvider getCredentialsProvider(final NodeParametersInput input) {
            return ((NodeParametersInputImpl) input).getCredentialsProvider().orElseThrow();
        }

    }

    // ===================== Parameter Groups =====================

    private static final class AuthenticationParameters implements NodeParameters {

        private static final String CFG_KEY = "auth";
        private static final String CFG_KEY_USER_PWD = "user_pwd";
        private static final String CFG_KEY_USER_PWD_V2 = "user_pwd_v2";

        enum AuthenticationMethod {
            USERNAME_PASSWORD
        }

        @ValueReference(AuthenticationMethodRef.class)
        @Persistor(AuthenticationMethodPersistor.class)
        AuthenticationMethod m_type = AuthenticationMethod.USERNAME_PASSWORD;

        static final class AuthenticationMethodRef implements ParameterReference<AuthenticationMethod> {
        }

        static final class AuthenticationMethodPersistor implements NodeParametersPersistor<AuthenticationMethod> {

            private static final String ENTRY_KEY = "type";

            @Override
            public AuthenticationMethod load(final NodeSettingsRO settings) throws InvalidSettingsException {
                final var typeString = settings.getString(ENTRY_KEY, "");
                if (typeString.equalsIgnoreCase(CFG_KEY_USER_PWD) || typeString.equalsIgnoreCase(CFG_KEY_USER_PWD_V2)) {
                    return AuthenticationMethod.USERNAME_PASSWORD;
                }
                throw new InvalidSettingsException(
                    String.format("Unknown authentication method: '%s'. Possible values: '%s'", typeString,
                        CFG_KEY_USER_PWD_V2));
            }

            @Override
            public void save(final AuthenticationMethod param, final NodeSettingsWO settings) {
                saveInternal(param, settings);
            }

            private static void saveInternal(final AuthenticationMethod param, final NodeSettingsWO settings) {
                if (param == AuthenticationMethod.USERNAME_PASSWORD) {
                    settings.addString(ENTRY_KEY, CFG_KEY_USER_PWD_V2);
                }
            }

            @Override
            public String[][] getConfigPaths() {
                return new String[][] { { ENTRY_KEY } };
            }
        }

        // ----- SECOND LEVEL NESTING NEEDED FOR BACKWARD COMPATIBILITY -----

        @Persist(configKey = CFG_KEY_USER_PWD_V2)
        @Migration(LoadFromUserPwdAuthMigration.class)
        @Widget(title = "Username & Password", description = "Credentials for username and password authentication.")
        LegacyCredentials m_userPasswordAuth =
            new LegacyCredentials(new Credentials(System.getProperty("user.name"), ""));

        static final class LoadFromUserPwdAuthMigration
                extends LegacyCredentialsAuthProviderSettings.FromUserPasswordAuthProviderSettingsMigration {

            protected LoadFromUserPwdAuthMigration() {
                super(new UserPasswordAuthProviderSettings(StandardAuthTypes.USER_PASSWORD, true));
            }

        }

    }

    // ===================== Validation =====================

    void validateOnConfigure(final CredentialsProvider credProv) throws InvalidSettingsException {
        // URL validation
        validateUrl(m_url);

        // authentication validation
        final var cred = m_authentication.m_userPasswordAuth.toCredentials(credProv);
        CheckUtils.checkSetting(isNotBlank(cred.getUsername()), "Username required for authentication.");
        CheckUtils.checkSetting(isNotBlank(cred.getPassword()), "Password required for authentication.");

        // working directory validation
        validateWorkingDirectory(m_workingDirectory);

        // timeout validation
        if (m_connectionTimeout < 0) {
            throw new InvalidSettingsException("Connection timeout must be greater than or equal to zero.");
        }
        if (m_receiveTimeout < 0) {
            throw new InvalidSettingsException("Receive timeout must be greater than or equal to zero.");
        }
    }

    private static void validateUrl(final String url) throws InvalidSettingsException {
        CheckUtils.checkSetting(isNotBlank(url), "URL required.");

        try {
            final var uri = new URI(url);

            if (isBlank(uri.getScheme())) {
                throw new InvalidSettingsException("Empty scheme in URL " +  uri.toString());
            } else if (isBlank(uri.getHost())) {
                throw new InvalidSettingsException("Empty host in URL " +  uri.toString());
            } else if (isBlank(uri.getPath())) { // NOSONAR no else required
                throw new InvalidSettingsException("Empty path in URL " +  uri.toString());
            }

        } catch (final URISyntaxException e) {
            throw new InvalidSettingsException("Failed to parse URL: " + e.getMessage(), e);
        }
    }

    private static boolean isValidWorkingDirectory(final String workingDirectory) {
        return workingDirectory != null && workingDirectory.startsWith(KnoxHdfsFileSystem.PATH_SEPARATOR);
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
     * Convert this settings to a {@link KnoxHdfsFSConnectionConfig} instance.
     *
     * @param credProv provider of credential variables
     * @return a {@link KnoxHdfsFSConnectionConfig} using this settings
     */
    public KnoxHdfsFSConnectionConfig toFSConnectionConfig(final CredentialsProvider credProv) {
        final var cred = m_authentication.m_userPasswordAuth.toCredentials(credProv);

        return KnoxHdfsFSConnectionConfig.builder() //
            .withEndpointUrl(m_url) //
            .withWorkingDirectory(m_workingDirectory) //
            .withUserAndPassword(cred.getUsername(), cred.getPassword()) //
            .withConnectionTimeout(Duration.ofSeconds(m_connectionTimeout)) //
            .withReceiveTimeout(Duration.ofSeconds(m_receiveTimeout)) //
            .build();
    }

}