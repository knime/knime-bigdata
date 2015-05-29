/*
 * -------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
 * -------------------------------------------------------------------
 *
 * History
 *    23.07.2008 (Tobias Koetter): created
 */

package com.knime.bigdata.spark.node;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedList;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.config.Config;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;


/**
 *
 * @author Tobias Koetter, University of Konstanz
 */
public abstract class AbstractSparkNodeModel extends NodeModel {

//    private static final String NET_DIR_PREFIX = "net_";

    private static final NodeLogger LOGGER =
            NodeLogger.getLogger(AbstractSparkNodeModel.class);

    private static final String CFG_FILE = "settingsFile.xml";

    private static final String CFG_SETTING = "saveInternalsSettings";

    private static final String CFG_CONTEXT = "context";

    private static final String CFG_NAMED_RDD_UUIDS = "namedRDDs";

    private final LinkedList<String> m_contexts = new LinkedList<>();

    private final LinkedList<String> m_namedRDDs = new LinkedList<>();

    private boolean m_destroyRDDs = true;

    /**Constructor for class AbstractGraphNodeModel.
     * @param inPortTypes the input port types
     * @param outPortTypes the output port types
     */
    protected AbstractSparkNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        super(inPortTypes, outPortTypes);
    }

    /**
     * @param destroy <code>true</code> if created networks should be destroyed
     * on node reset. <code>false</code> if they should be removed from cache
     * but not destroyed in the persistence layer.
     */
    protected void destroyRDDs(final boolean destroy) {
        m_destroyRDDs = destroy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final PortObject[] portObjects = executeInternal(inData, exec);
        if (portObjects != null && portObjects.length > 0) {
            for (final PortObject portObject : portObjects) {
                if (portObject instanceof  SparkDataPortObject) {
                   addObject((SparkDataPortObject)portObject);
                }
            }
        }
        return portObjects;
    }

    private void addObject(final SparkDataPortObject sparkObject) {
        m_contexts.add(sparkObject.getContext());
        m_namedRDDs.add(sparkObject.getTableName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void reset() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Entering reset() of class AbstractSparkNodeModel.");
        }
        m_contexts.clear();
        m_namedRDDs.clear();
        resetInternal();
    }

    /**
     *
     * @param inData The input objects including the {@link SparkDataPortObject}.
     * @param exec For {@link BufferedDataTable} creation and progress.
     * @return The output objects.
     * @throws Exception If the node execution fails for any reason.
     */
    protected abstract PortObject[] executeInternal(PortObject[] inData, ExecutionContext exec) throws Exception;

    /**
     * Called when the node is reseted.
     */
    protected void resetInternal() {
        //override if you need to reset anything
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
    throws IOException, CanceledExecutionException {
        final File settingFile = new File(nodeInternDir, CFG_FILE);
        try(final FileInputStream inData = new FileInputStream(settingFile)){
            final ConfigRO config = NodeSettings.loadFromXML(inData);
            final String[] namedRDDUUIDs =
                config.getStringArray(CFG_NAMED_RDD_UUIDS);
            if (namedRDDUUIDs.length > 0) {
                for (int i = 0, length = namedRDDUUIDs.length; i < length; i++) {
                    final String uuid = namedRDDUUIDs[i];
                    m_namedRDDs.add(uuid);
                }
            }
            final String[] context =
                    config.getStringArray(CFG_CONTEXT);
                if (context.length > 0) {
                    for (int i = 0, length = context.length; i < length; i++) {
                        final String uuid = context[i];
                        m_contexts.add(uuid);
                        if (!KnimeContext.sparkContextExists(uuid)) {
                            setWarningMessage("Spark context no longer available. Reset node.");
                        }
                    }
                }
            loadAdditionalInternals(nodeInternDir, exec);
        } catch (final InvalidSettingsException | GenericKnimeSparkException e) {
            throw new IOException("Invalid loadInternals settings file", e.getCause());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
    throws IOException, CanceledExecutionException {
        final File settingFile = new File(nodeInternDir, CFG_FILE);
        try (final FileOutputStream dataOS = new FileOutputStream(settingFile)){
            final Config config = new NodeSettings(CFG_SETTING);
            exec.checkCanceled();
            config.addStringArray(CFG_CONTEXT, m_contexts.toArray(new String[0]));
            config.addStringArray(CFG_NAMED_RDD_UUIDS, m_namedRDDs.toArray(new String[0]));
            config.saveToXML(dataOS);
        } catch (final Exception e) {
            throw new IOException(e.getMessage(), e.getCause());
        }
        saveAdditionalInternals(nodeInternDir, exec);
    }

    /**
     * Load internals into the derived <code>NodeModel</code>. This method is
     * only called if the <code>Node</code> was executed. Read all your
     * internal structures from the given file directory to create your internal
     * data structure which is necessary to provide all node functionalities
     * after the workflow is loaded, e.g. view content and/or hilite mapping.
     * <br>
     *
     * @param nodeInternDir The directory to read from.
     * @param exec Used to report progress and to cancel the load process.
     * @throws IOException If an error occurs during reading from this dir.
     * @throws CanceledExecutionException If the loading has been canceled.
     * @see #saveAdditionalInternals(File, ExecutionMonitor)
     */
    protected void loadAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // override if you need to load some internal data
    }

    /**
     * Save internals of the derived <code>NodeModel</code>. This method is
     * only called if the <code>Node</code> is executed. Write all your
     * internal structures into the given file directory which are necessary to
     * recreate this model when the workflow is loaded, e.g. view content and/or
     * hilite mapping.<br>
     *
     * @param nodeInternDir The directory to write into.
     * @param exec Used to report progress and to cancel the save process.
     * @throws IOException If an error occurs during writing to this dir.
     * @throws CanceledExecutionException If the saving has been canceled.
     * @see #loadAdditionalInternals(File, ExecutionMonitor)
     */
    protected void saveAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // override if you need to save some internal data
    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    protected void onDispose() {
//        try {
//            //detach all networks from the repository
//            //when the workflow is closed or the node is removed from the
//            //workflow
//            if (!m_namedRDDs.isEmpty()) {
//                for (final String id : m_namedRDDs) {
////                    GraphRepository.getInstance().detach(id);
//                }
//                m_namedRDDs.clear();
//            }
//        } catch (final Throwable e) {
//            LOGGER.debug("Exception while finalizing Spark node: " + e.getMessage());
//        }
//    }
}
