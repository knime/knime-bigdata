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
package org.knime.bigdata.spark.local.node.create.utils;

import java.io.File;
import java.io.IOException;

import org.knime.bigdata.spark.local.context.LocalSparkContext;
import org.knime.bigdata.spark.local.context.LocalSparkContextConfig;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Describes an initializer of one of "Create Local Big Data Environment" node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SuppressWarnings("javadoc") // ignore not visible warnings
public interface CreateLocalBDEPortUtil {

    /**
     * Create the output test {@link PortObjectSpec} instance.
     *
     * @return testing {@link PortObjectSpec} instance
     * @throws InvalidSettingsException
     */
    public PortObjectSpec configure() throws InvalidSettingsException;

    /**
     * Create the output test {@link PortObject} and test the wrapped connection if possible.
     *
     * @param sparkContext local spark context
     * @param exec to display progress
     * @return testing {@link PortObject} instance
     * @throws Exception
     */
    public PortObject execute(final LocalSparkContext sparkContext, final ExecutionContext exec) throws Exception;

    /**
     * Called by the framework when the node is disposed.
     *
     * @see NodeModel#onDispose()
     */
    public default void onDispose() {
    }

    /**
     * Called by the framework when the node is reseted.
     *
     * @see NodeModel#reset()
     */
    public default void reset() {
    }

    /**
     * Load internals of the port utility.
     *
     * @param nodeInternDir The directory to read from.
     * @param exec Used to report progress and to cancel the load process.
     * @param sparkContext local spark context
     * @param sparkContextConfig local spark context configuration
     * @throws IOException If an error occurs during reading from this dir.
     * @throws CanceledExecutionException If the loading has been canceled.
     * @see NodeModel#loadInternals(File nodeInternDir, ExecutionContext exec)
     */
    public default void loadInternals(final File nodeInternDir, final ExecutionMonitor exec,
        final LocalSparkContext sparkContext, final LocalSparkContextConfig sparkContextConfig)
        throws IOException, CanceledExecutionException {

    }

    /**
     * Save internals of the port utility.
     *
     * @param nodeInternDir The directory to write into.
     * @param exec Used to report progress and to cancel the save process.
     * @throws IOException If an error occurs during writing to this dir.
     * @throws CanceledExecutionException If the saving has been canceled.
     * @see NodeModel#saveInternals(File nodeInternDir, ExecutionContext exec)
     */
    public default void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
    }

}
