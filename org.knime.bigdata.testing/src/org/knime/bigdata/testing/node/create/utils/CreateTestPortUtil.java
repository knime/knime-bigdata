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
 */
package org.knime.bigdata.testing.node.create.utils;

import java.util.Map;

import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Describes an initializer of one of "Create Big Data Test Environment" node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public interface CreateTestPortUtil {

    /**
     * Create the output test {@link PortObjectSpec} instance.
     *
     * @param sparkScheme spark scheme to match
     * @param flowVars current flow variables
     * @return testing {@link PortObjectSpec} instance
     * @throws InvalidSettingsException
     */
    public PortObjectSpec configure(final SparkContextIDScheme sparkScheme, final Map<String, FlowVariable> flowVars)
        throws InvalidSettingsException;

    /**
     * Create the output test {@link PortObject} and test the wrapped connection if possible.
     *
     * @param sparkScheme spark scheme to match
     * @param flowVars current flow variables
     * @param exec to display progress
     * @param credentialsProvider current credentials provider
     * @return testing {@link PortObject} instance
     * @throws Exception
     */
    public PortObject execute(final SparkContextIDScheme sparkScheme, final Map<String, FlowVariable> flowVars,
        final ExecutionContext exec, final CredentialsProvider credentialsProvider) throws Exception;

    /**
     * Called by the framework when the node is disposed.
     *
     * @see NodeModel#onDispose()
     */
    @SuppressWarnings("javadoc")
    public default void onDispose() {
    }

    /**
     * Called by the framework when the node is reseted.
     *
     * @see NodeModel#reset()
     */
    @SuppressWarnings("javadoc")
    public default void reset() {
    }
}
