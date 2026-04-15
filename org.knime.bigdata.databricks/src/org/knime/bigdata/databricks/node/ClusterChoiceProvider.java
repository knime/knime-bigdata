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
 *   2026-03-16 (bjoern): created
 */
package org.knime.bigdata.databricks.node;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import org.knime.bigdata.databricks.rest.DatabricksRESTClient;
import org.knime.bigdata.databricks.rest.clusters.ClusterAPI;
import org.knime.bigdata.databricks.rest.clusters.ClusterInfo;
import org.knime.bigdata.databricks.rest.clusters.ClusterInfoList;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObjectSpec;
import org.knime.core.data.sort.AlphanumericComparator;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.node.parameters.experimental.validation.WidgetHandlerException;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.widget.choices.StringChoice;
import org.knime.node.parameters.widget.choices.StringChoicesProvider;

/**
 *
 * Reusable implementation for a {@link StringChoicesProvider} that lists Databricks clusters, given a Databricks
 * Workspace connection.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SuppressWarnings("restriction")
public class ClusterChoiceProvider implements StringChoicesProvider {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ClusterChoiceProvider.class);

    private static final Comparator<ClusterInfo> CLUSTER_COMPARATOR =
        Comparator.comparing(c -> c.cluster_name, AlphanumericComparator.NATURAL_ORDER);

    @Override
    public void init(final StateProviderInitializer initializer) {
        initializer.computeAfterOpenDialog();
    }

    @Override
    public List<StringChoice> computeState(final NodeParametersInput context) {

        if (context.getInPortSpec(0).isEmpty()
            || !(context.getInPortSpec(0).get() instanceof DatabricksWorkspacePortObjectSpec)) {
            return List.of();
        }

        try {
            final ClusterInfoList clusterList = DatabricksRESTClient //
                .fromSingleWorkspaceInputPort(ClusterAPI.class, context.getInPortSpecs()) //
                .list();

            if (clusterList.clusters == null) {
                return List.of();
            }
            return clusterList.clusters.stream() //
                .sorted(CLUSTER_COMPARATOR) //
                .map(i -> new StringChoice(i.cluster_id, toLabel(i))) //
                .toList();
        } catch (final InvalidSettingsException | NoSuchCredentialException e) { // NOSONAR catch all exceptions here
            throw new WidgetHandlerException(e.getMessage());
        } catch (Exception e) {
            LOGGER.info("Unable to fetch cluster list.", e);
            throw new WidgetHandlerException("Unable to fetch cluster list: " + e.getMessage());
        }
    }

    private static String toLabel(final ClusterInfo info) {
        if (info.state == null) {
            return info.cluster_name;
        }

        final String state = info.state.toString().toLowerCase(Locale.ENGLISH);
        return String.format("%s (%s)", info.cluster_name, state);
    }
}
