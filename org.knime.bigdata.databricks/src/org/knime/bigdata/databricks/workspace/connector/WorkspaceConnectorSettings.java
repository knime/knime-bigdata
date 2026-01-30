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
 *   May 16, 2024 (Bjoern Lohrmann, KNIME GmbH): created
 */
package org.knime.bigdata.databricks.workspace.connector;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.webui.node.dialog.defaultdialog.widget.validation.custom.CustomValidation;
import org.knime.core.webui.node.dialog.defaultdialog.widget.validation.custom.SimpleValidation;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.credentials.base.CredentialType;
import org.knime.node.parameters.Advanced;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ParameterReference;
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
 * Node settings for the Databricks Workspace Connector node.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SuppressWarnings("restriction")
public class WorkspaceConnectorSettings implements NodeParameters {

    private static boolean hasCredentialPort(final PortType[] types) {
        return Arrays.stream(types).anyMatch(CredentialPortObject.TYPE::equals);
    }

    /**
     * Constant signal to indicate whether the user has added a credential port or not.
     */
    static final class CredentialInputConnected implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getConstant(context -> hasCredentialPort(context.getInPortTypes()));
        }
    }

    /**
     * Constant signal to indicate whether the user has added a databricks credential port or not.
     */
    static final class CredentialInputDatabricks implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getConstant(
                context -> databricksPortAvailable(context.getInPortSpecs()));
        }
    }

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
            return "Remove the input port to change the connection and authentication settings";
        }

        @Override
        public MessageType type() {
            return MessageType.INFO;
        }

    }

    @TextMessage(value = AuthenticationManagedByPortMessage.class)
    Void m_authenticationManagedByPortText;

    @Section(title = "Connection")
    @Effect(predicate = CredentialInputDatabricks.class, type = EffectType.HIDE)
    interface ConnectionSection {
    }

    @Section(title = "Authentication")
    @After(ConnectionSection.class)
    @Effect(predicate = CredentialInputConnected.class, type = EffectType.HIDE)
    interface AuthenticationSection {
    }

    @Section(title = "Timeouts")
    @Advanced
    @After(AuthenticationSection.class)
    interface TimeoutsSection {
    }

    @Widget(title = "Databricks workspace URL", //
        description = "Full URL of the Databricks workspace, e.g. https://&lt;workspace&gt;.cloud.databricks.com/ "//
            + "or https://adb-&lt;workspace-id&gt;.&lt;random-number&gt;.azuredatabricks.net/ on Azure.")
    @Layout(ConnectionSection.class)
    @CustomValidation(UrlValidator.class)
    String m_workspaceUrl = "";

    @Widget(title = "Authentication method", //
        description = "Specify the authentication method to use. The following types are supported:\n"//
            + "<ul>"//
            + "<li><b>Personal access token</b>: Authenticate with a personal access token.</li>\n"//
            + "</ul>")
    @ValueReference(AuthTypeRef.class)
    @Layout(AuthenticationSection.class)
    AuthType m_authType = AuthType.TOKEN;

    static final class AuthTypeRef implements ParameterReference<AuthType> {
    }

    enum AuthType {
            @Label("Personal access token")
            TOKEN;

        static final class IsTokenAuthTypeAndCredentialInputNotConnected implements EffectPredicateProvider {
            @Override
            public EffectPredicate init(final PredicateInitializer i) {
                return i.getEnum(AuthTypeRef.class).isOneOf(AuthType.TOKEN)
                    .and(i.getPredicate(CredentialInputConnected.class).negate());
            }
        }
    }

    @Widget(title = "Personal access token", //
        description = "The Databricks personal access token to use. The value\"\n"
            + " entered here will be stored in weakly encrypted form with the workflow.")
    @PasswordWidget(passwordLabel = "Token")
    @Effect(predicate = AuthType.IsTokenAuthTypeAndCredentialInputNotConnected.class, type = EffectType.SHOW)
    @Layout(AuthenticationSection.class)
    Credentials m_token = new Credentials();

    @Widget(title = "Connection timeout (seconds)",
        description = "Timeout in seconds to establish a connection, or 0 for an infinite timeout.  "
            + " Used by this and downstream nodes connecting to Databricks.", //
        advanced = true)
    @NumberInputWidget(minValidation = IsNonNegativeValidation.class)
    @Layout(TimeoutsSection.class)
    int m_connectionTimeout = 30;

    @Widget(title = "Read timeout (seconds)",
        description = "Timeout in seconds to read data from an established connection,"
            + " or 0 for an infinite timeout. Used by this and downstream nodes connecting to Databricks.", //
        advanced = true)
    @NumberInputWidget(minValidation = IsNonNegativeValidation.class)
    @Layout(TimeoutsSection.class)
    int m_readTimeout = 30;

    Duration getReadTimeout() {
        return Duration.ofSeconds(m_readTimeout);
    }

    Duration getConnectionTimeout() {
        return Duration.ofSeconds(m_readTimeout);
    }

    void validate(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        validateBasicSettings();
        if (databricksPortAvailable(inSpecs)) {
            //If a Databricks input port is available we can ignore all other settings
            return;
        }

        validateWorkspaceURL(m_workspaceUrl);

        if (inSpecs != null && inSpecs.length > 0) {
            // if a credential input port is attached then usernamePassword and token don't matter
            // because they will be ignored anyway
            return;
        }

        if (m_authType == AuthType.TOKEN && StringUtils.isBlank(m_token.getPassword())) {
            throw new InvalidSettingsException("Please specify the personal access token to use");
        }
    }

    private static void validateWorkspaceURL(final String workspaceUrl) throws InvalidSettingsException {
        if (StringUtils.isBlank(workspaceUrl)) {
            throw new InvalidSettingsException("Please specify a Databricks workspace URL");
        }

        try {
            final URI parsedUrl = new URI(workspaceUrl);
            if (!Objects.equals("https", parsedUrl.getScheme())) {
                throw new InvalidSettingsException("The provided Databricks workspace URL must start with https://");
            }
        } catch (URISyntaxException e) { // NOSONAR not rethrowing
            throw new InvalidSettingsException("The provided Databricks workspace URL is invalid");
        }
    }

    private void validateBasicSettings() throws InvalidSettingsException {

        if (m_connectionTimeout < 0) {
            throw new InvalidSettingsException("Connection timeout must be a positive number.");
        }

        if (m_readTimeout < 0) {
            throw new InvalidSettingsException("Read timeout must be a positive number.");
        }
    }

    private static boolean databricksPortAvailable(final PortObjectSpec[] specs) {
        if (specs == null || specs.length < 1 || !(specs[0] instanceof CredentialPortObjectSpec)) {
            return false;
        }
        final CredentialPortObjectSpec credSpec = (CredentialPortObjectSpec)specs[0];
        Optional<CredentialType> credentialType = credSpec.getCredentialType();
        if (credentialType.isEmpty()) {
            return false;
        }
        return DatabricksAccessTokenCredential.TYPE == credentialType.get();
    }

    // ===================== Custom Validations =====================

    static class UrlValidator extends SimpleValidation<String> {
        @Override
        public void validate(final String url) throws InvalidSettingsException {
            validateWorkspaceURL(url);
        }
    }

}
