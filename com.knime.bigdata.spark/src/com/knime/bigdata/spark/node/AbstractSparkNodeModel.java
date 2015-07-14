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
import java.util.List;

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
import org.knime.core.node.config.ConfigWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortType;
import org.knime.core.util.Pair;

import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;


/**
 *
 * @author Tobias Koetter, University of Konstanz
 */
public abstract class AbstractSparkNodeModel extends NodeModel {

    private static final NodeLogger LOGGER =
            NodeLogger.getLogger(AbstractSparkNodeModel.class);

    private static final String CFG_FILE = "settingsFile.xml";

    private static final String CFG_SETTING = "saveInternalsSettings";

    private static final String CFG_CONTEXT = "context";

    private static final String CFG_NAMED_RDD_UUIDS = "namedRDDs";

    private final List<Pair<KNIMESparkContext, String>> m_namedRDDs = new LinkedList<>();

    /**Constructor for class AbstractGraphNodeModel.
     * @param inPortTypes the input port types
     * @param outPortTypes the output port types
     */
    protected AbstractSparkNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        super(inPortTypes, outPortTypes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        //SparkPlugin.LICENSE_CHECKER.checkLicenseInNode();
        for (final PortObject portObject : inData) {
            if (portObject instanceof  SparkDataPortObject) {
                final KNIMESparkContext context = ((SparkDataPortObject)portObject).getContext();
                if (!KnimeContext.sparkContextExists(context)) {
                    throw new IllegalStateException(
                        "Incoming Spark context no longer exists. Please reset all preceding Spark nodes.");
                }
            }
        }
        final PortObject[] portObjects = executeInternal(inData, exec);
        if (portObjects != null && portObjects.length > 0) {
            for (final PortObject portObject : portObjects) {
                if (portObject instanceof  SparkDataPortObject) {
                   addSparkObject((SparkDataPortObject)portObject);
                }
            }
        }
        return portObjects;
    }

    private void addSparkObject(final SparkDataPortObject sparkObject) {
        m_namedRDDs.add(new Pair<>(sparkObject.getContext(), sparkObject.getTableName()));

    }

    /**
     * {@inheritDoc}
     * Gets called when the node is reseted or deleted.
     */
    @Override
    protected final void reset() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Entering reset() of class AbstractSparkNodeModel.");
        }
        for (Pair<KNIMESparkContext, String> rdd : m_namedRDDs) {
            KnimeContext.deleteNamedRDD(rdd.getFirst(), rdd.getSecond());
        }
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
            final String[] namedRDDUUIDs = config.getStringArray(CFG_NAMED_RDD_UUIDS);
            final int noOfContexts = namedRDDUUIDs.length;
            for (int i = 0; i < noOfContexts; i++) {
                final ConfigRO contextConfig = config.getConfig(CFG_CONTEXT + i);
                final KNIMESparkContext context = new KNIMESparkContext(contextConfig);
                m_namedRDDs.add(new Pair<>(context, namedRDDUUIDs[i]));
            }
            loadAdditionalInternals(nodeInternDir, exec);
        } catch (final InvalidSettingsException | RuntimeException e) {
            throw new IOException("Failed to load named rdd", e.getCause());
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
            final String[] rdds = new String[m_namedRDDs.size()];
            int idx = 0;
            for (Pair<KNIMESparkContext, String> p : m_namedRDDs) {
                rdds[idx] = p.getSecond();
                final ConfigWO contextConfig = config.addConfig(CFG_CONTEXT + idx++);
                p.getFirst().save(contextConfig);
                exec.checkCanceled();
            }
            config.addStringArray(CFG_NAMED_RDD_UUIDS, rdds);
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
}
