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
 *   Dec 26, 2019 (wiswedel): created
 */
package org.knime.bigdata.databricks.workspace.port;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.knime.core.node.util.CheckUtils;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.credentials.base.CredentialType;

/**
 * Spec for a a connection to a Databricks workspace, including credential. Subclass of
 * {@link CredentialPortObjectSpec}.
 *
 * @author Jannik Loescher, KNIME GmbH
 */
public final class DatabricksWorkspacePortObjectSpec extends CredentialPortObjectSpec {

    private static final String CFG_READ_TIMEOUT_KEY = "readTimeout";

    private static final String CFG_CONNECT_TIMEOUT_KEY = "connectTimeout";

    /** Serializer as required by extension point. */
    public static final class Serializer
        extends AbstractSimplePortObjectSpecSerializer<DatabricksWorkspacePortObjectSpec> {
    }

    private Duration m_connectionTimeout;

    private Duration m_readTimeout;

    /**
     * Constructor.
     *
     * @param credentialType The type of the underlying credential. May be null, but then cacheId must also be null.
     * @param cacheId The cache UUID of the underlying credential. May be null, but in.
     * @param connectionTimeout HTTP connection timeout to use by downstream nodes.
     * @param readTimeout HTTP read timeout to use by downstream nodes.
     */
    public DatabricksWorkspacePortObjectSpec(final CredentialType credentialType, final UUID cacheId,
        final Duration connectionTimeout, final Duration readTimeout) {
        super(credentialType, cacheId);
        CheckUtils.checkArgument(connectionTimeout.getSeconds() >= 0, //
            "Connection timeout must be reather than or equal to zero.");
        CheckUtils.checkArgument(readTimeout.getSeconds() >= 0, //
            "Read timeout  must be reather than or equal to zero.");
        m_connectionTimeout = connectionTimeout;
        m_readTimeout = readTimeout;
    }

    /**
     * API method, do not use.
     */
    public DatabricksWorkspacePortObjectSpec() {
    }

    @Override
    protected void load(final ModelContentRO model) throws InvalidSettingsException {
        super.load(model);

        m_connectionTimeout = Duration.ofSeconds(model.getLong(CFG_CONNECT_TIMEOUT_KEY));
        m_readTimeout = Duration.ofSeconds(model.getLong(CFG_READ_TIMEOUT_KEY));
    }

    @Override
    protected void save(final ModelContentWO model) {
        super.save(model);
        model.addLong(CFG_CONNECT_TIMEOUT_KEY, m_connectionTimeout.toSeconds());
        model.addLong(CFG_READ_TIMEOUT_KEY, m_readTimeout.toSeconds());
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_connectionTimeout, m_readTimeout, super.hashCode());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final DatabricksWorkspacePortObjectSpec other = (DatabricksWorkspacePortObjectSpec)obj;
        return m_connectionTimeout == other.m_connectionTimeout && //
            m_readTimeout == other.m_readTimeout && //
            super.equals(other);
    }

    /**
     * @return the HTTP connection timeout
     */
    public Duration getConnectionTimeout() {
        return m_connectionTimeout;
    }

    /**
     * @return the HTTP read timeout
     */
    public Duration getReadTimeout() {
        return m_readTimeout;
    }
}
