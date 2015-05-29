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
import java.util.Collection;
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

import com.knime.bigdata.spark.port.data.SparkDataPortObject;


/**
 * This model handles the caching and saving of {@link KPartiteGraph}. All nodes
 * that handle {@link KPartiteGraph}s within KNIME should extend this class
 * or one of its extensions in order to use {@link KPartiteGraph}s within KNIME.
 *
 * @author Tobias Koetter, University of Konstanz
 */
public abstract class AbstractSparkNodeModel extends NodeModel {

//    private static final String NET_DIR_PREFIX = "net_";

    private static final NodeLogger LOGGER =
            NodeLogger.getLogger(AbstractSparkNodeModel.class);

    private static final String CFG_FILE = "settingsFile.xml";

    private static final String CFG_SETTING = "saveInternalsSettings";

    private static final String CFG_NAMED_RDD_UUIDS = "namedRDDs";

    private final LinkedList<String> m_namedRDDs = new LinkedList<>();

    private boolean m_destroyRDDs = true;

    /**Constructor for class AbstractGraphNodeModel.
     * @param inPortTypes the input port types
     * @param outPortTypes the output port types
     */
    protected AbstractSparkNodeModel(final PortType[] inPortTypes,
            final PortType[] outPortTypes) {
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
    protected final PortObject[] execute(final PortObject[] inData,
            final ExecutionContext exec) throws Exception {
        final PortObject[] portObjects = executeInternal(inData, exec);
        if (portObjects != null && portObjects.length > 0) {
            for (final PortObject portObject : portObjects) {
                if (portObject instanceof  SparkDataPortObject) {
                   final  SparkDataPortObject netPortObject =
                       (SparkDataPortObject)portObject;
                   final String uuid = netPortObject.getTableName();
                   m_namedRDDs.add(uuid);
                }
            }
        }
        return portObjects;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void reset() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Entering reset() of class AbstractSparkNodeModel.");
        }
        for (final String id : m_namedRDDs) {
//            if (m_destroyNetworks) {
//                //destroy all networks that were created by this node
//                GraphRepository.getInstance().destroy(id);
//            } else {
//                //detach the networks that were created by this node
//                //only from cache
//                GraphRepository.getInstance().detach(id);
//            }
        }
        m_namedRDDs.clear();
        resetInternal();
    }

    /**
     * This method is called from the
     * {@link #execute(PortObject[], ExecutionContext)} method. In order to
     * retrieve the provided {@link FeaturedHypergraph} or
     * {@link FeaturedHypergraphView} call the
     * {@link GraphRepository#getIncrementalNet(PortObject[])} or
     * {@link GraphRepository#getView(PortObject[])} method with the inData.
     * If you use an incremental network take care to destroy the network
     * if an exception occurs see the
     * {@link KPartiteGraphNodeModel#executeInternal(PortObject[],
     * ExecutionContext)}
     * method as an example!
     *
     * @param inData The input objects including the {@link GraphPortObject}.
     * @param exec For {@link BufferedDataTable} creation and progress.
     * @return The output objects.
     * @throws Exception If the node execution fails for any reason.
     */
    protected abstract PortObject[] executeInternal(PortObject[] inData,
            ExecutionContext exec) throws Exception;

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
    protected final void loadInternals(final File nodeInternDir,
            final ExecutionMonitor exec)
    throws IOException, CanceledExecutionException {
       final File settingFile = new File(nodeInternDir, CFG_FILE);
        final FileInputStream inData = new FileInputStream(settingFile);
        final ConfigRO config = NodeSettings.loadFromXML(inData);
        try {
            final String[] namedRDDUUIDs =
                config.getStringArray(CFG_NAMED_RDD_UUIDS);
            if (namedRDDUUIDs.length > 0) {
                final double progress = 1.0 / namedRDDUUIDs.length;
                for (int i = 0, length = namedRDDUUIDs.length; i < length; i++) {
                    final String uuid = namedRDDUUIDs[i];
                    m_namedRDDs.add(uuid);
                    //check if the network is already in the cache
//                    if (GraphRepository.getInstance().isPresent(uuid)) {
//                        if (LOGGER.isDebugEnabled()) {
//                            LOGGER.debug("Network already in repository: "
//                                + uuid
//                                + ". It seems a workflow has been reopend.");
//                        }
//                    } else {
//                        //it is not in the cache -> load it from file
//                        try {
//                            final File networkDir =
//                                new File(nodeInternDir, NET_DIR_PREFIX + i);
//                            GraphRepository.getInstance().load(
//                                    exec.createSubProgress(progress),
//                                    networkDir);
//                        } catch (final FileNotFoundException e) {
//                            //be backward compatible to the time when we used
//                            //the network uuid as directory name this has
//                            //changed since the uuid can be pretty long
//                            GraphRepository.getInstance().load(
//                                    exec.createSubProgress(progress),
//                                    new File(nodeInternDir, uuid));
//                        }
//                    }
                }
            }
        } catch (final InvalidSettingsException e) {
            throw new IOException("Invalid loadInternals settings file", e.getCause());
        }
        loadAdditionalInternals(nodeInternDir, exec);
    }

    /**
     * @param nodeInternDir
     * @param exec
     */
    protected void loadAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // override if you need to load some internal data
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
    throws IOException, CanceledExecutionException {
        try {
            final File settingFile = new File(nodeInternDir, CFG_FILE);
            final FileOutputStream dataOS = new FileOutputStream(settingFile);
            final Config config = new NodeSettings(CFG_SETTING);
            exec.checkCanceled();
            final Collection<String> uuids = new LinkedList<>();
            if (!m_namedRDDs.isEmpty()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Spark ids to save: " + m_namedRDDs.toString());
                }
                final double subProgress = 1.0 / m_namedRDDs.size();
                int i = 0;
                for (final String id : m_namedRDDs) {
//                    final File networkDir =
//                        new File(nodeInternDir, NET_DIR_PREFIX + i++);
//                    if (!networkDir.mkdir()) {
//                        throw new IOException(
//                                "Unable to create spark data subdirectory: "
//                                + networkDir.getPath());
//                    }
//                    GraphRepository.getInstance().save(exec.createSubProgress(
//                            subProgress), networkDir, id);
                    uuids.add(id);
                    exec.checkCanceled();
                }
            }
            config.addStringArray(CFG_NAMED_RDD_UUIDS, uuids.toArray(new String[0]));
            config.saveToXML(dataOS);
        } catch (final Exception e) {
            throw new IOException(e.getMessage(), e.getCause());
        }
        saveAdditionalInternals(nodeInternDir, exec);
    }

    /**
     * @param nodeInternDir
     * @param exec
     */
    protected void saveAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // override if you need to save some internal data
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onDispose() {
        try {
            //detach all networks from the repository
            //when the workflow is closed or the node is removed from the
            //workflow
            if (!m_namedRDDs.isEmpty()) {
                for (final String id : m_namedRDDs) {
//                    GraphRepository.getInstance().detach(id);
                }
                m_namedRDDs.clear();
            }
        } catch (final Throwable e) {
            LOGGER.debug("Exception while finalizing Spark node: " + e.getMessage());
        }
    }
}
