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
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeSettings;
import org.knime.core.webui.node.dialog.defaultdialog.layout.After;
import org.knime.core.webui.node.dialog.defaultdialog.layout.Layout;
import org.knime.core.webui.node.dialog.defaultdialog.layout.Section;
import org.knime.core.webui.node.dialog.defaultdialog.rule.And;
import org.knime.core.webui.node.dialog.defaultdialog.rule.ConstantSignal;
import org.knime.core.webui.node.dialog.defaultdialog.rule.Effect;
import org.knime.core.webui.node.dialog.defaultdialog.rule.Effect.EffectType;
import org.knime.core.webui.node.dialog.defaultdialog.rule.OneOfEnumCondition;
import org.knime.core.webui.node.dialog.defaultdialog.rule.Signal;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.Credentials;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Label;
import org.knime.core.webui.node.dialog.defaultdialog.widget.NumberInputWidget;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Widget;
import org.knime.core.webui.node.dialog.defaultdialog.widget.credentials.PasswordWidget;
import org.knime.credentials.base.CredentialPortObject;

/**
 * Node settings for the Databricks Workspace Connector node.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SuppressWarnings("restriction")
public class WorkspaceConnectorSettings implements DefaultNodeSettings {

    /**
     * Constant signal to indicate whether the user has added a credential port or not.
     */
    public static final class CredentialInputNotConnectedSignal implements ConstantSignal {
        @Override
        public boolean applies(final DefaultNodeSettingsContext context) {
            return Stream.of(context.getInPortTypes()).noneMatch(CredentialPortObject.TYPE::equals);
        }
    }

    @Section(title = "Token")
    @Effect(signals = {AuthType.IsToken.class, CredentialInputNotConnectedSignal.class}, //
        operation = And.class, //
        type = EffectType.SHOW)
    interface TokenSection {
    }

    @Section(title = "Timeouts", advanced = true)
    @After(TokenSection.class)
    interface ConnectionTimeoutsSection {
    }

    @Widget(title = "Databricks workspace URL", //
        description = "Full URL of the Databricks workspace, e.g. https://&lt;workspace&gt;.cloud.databricks.com/ "//
            + "or https://adb-&lt;workspace-id&gt;.&lt;random-number&gt;.azuredatabricks.net/ on Azure.")
    String m_workspaceUrl = "";

    @Widget(title = "Authentication type", //
        description = "Authentication type to use. The following types are supported:\n"//
            + "<ul>"//
            + "<li><b>Personal access token</b>: Authenticate with a personal access token.</li>\n"//
            + "</ul>")
    @Signal(condition = AuthType.IsToken.class)
    @Effect(signals = CredentialInputNotConnectedSignal.class, type = EffectType.SHOW)
    AuthType m_authType = AuthType.TOKEN;

    enum AuthType {
            @Label("Personal access token")
            TOKEN;

        static class IsToken extends OneOfEnumCondition<AuthType> {
            @Override
            public AuthType[] oneOf() {
                return new AuthType[]{TOKEN};
            }
        }
    }

    @Widget(title = "Personal access token", //
        description = "The Databricks personal access token to use. The value\"\n"
            + " entered here will be stored in weakly encrypted form with the workflow.")
    @PasswordWidget(passwordLabel = "Token")
    @Layout(TokenSection.class)
    Credentials m_token = new Credentials();

    @Widget(title = "Connection timeout (seconds)",
        description = "Timeout in seconds to establish a connection, or 0 for an infinite timeout.  "
            + " Used by this and downstream nodes connecting to Databricks.", //
        advanced = true)
    @NumberInputWidget(min = 0)
    @Layout(ConnectionTimeoutsSection.class)
    int m_connectionTimeout = 30;

    @Widget(title = "Read timeout (seconds)",
        description = "Timeout in seconds to read data from an established connection,"
            + " or 0 for an infinite timeout. Used by this and downstream nodes connecting to Databricks.", //
        advanced = true)
    @NumberInputWidget(min = 0)
    @Layout(ConnectionTimeoutsSection.class)
    int m_readTimeout = 30;

    Duration getReadTimeout() {
        return Duration.ofSeconds(m_readTimeout);
    }

    Duration getConnectionTimeout() {
        return Duration.ofSeconds(m_readTimeout);
    }

    void validate(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        validateBasicSettings();

        if (inSpecs != null && inSpecs.length > 0) {
            // if a credential input port is attached then usernamePassword and token don't matter
            // because they will be ignored anyway
            return;
        }

        if (m_authType == AuthType.TOKEN && StringUtils.isBlank(m_token.getPassword())) {
            throw new InvalidSettingsException("Please specify the personal access token to use");
        }
    }

    private void validateBasicSettings() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_workspaceUrl)) {
            throw new InvalidSettingsException("Please specify a Databricks workspace URL");
        }

        try {
            final URI parsedUrl = new URI(m_workspaceUrl);
            if (!Objects.equals("https", parsedUrl.getScheme())) {
                throw new InvalidSettingsException("The provided Databricks workspace URL must start with https://");
            }
        } catch (URISyntaxException e) { // NOSONAR not rethrowing
            throw new InvalidSettingsException("The provided Databricks workspace URL is invalid");
        }

        if (m_connectionTimeout < 0) {
            throw new InvalidSettingsException("Connection timeout must be a positive number.");
        }

        if (m_readTimeout < 0) {
            throw new InvalidSettingsException("Read timeout must be a positive number.");
        }
    }
}
