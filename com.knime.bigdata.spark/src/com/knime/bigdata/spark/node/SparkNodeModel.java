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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.config.Config;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.SparkPlugin;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.port.SparkContextProvider;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.preferences.KNIMEConfigContainer;


/**
 * Basic class that all NodeModel classes need to extend if they work with Spark data/model/etc objects.
 * @author Tobias Koetter, University of Konstanz
 */
public abstract class SparkNodeModel extends NodeModel {

    private static final NodeLogger LOGGER =
            NodeLogger.getLogger(SparkNodeModel.class);

    private static final String CFG_FILE = "settingsFile.xml";

    private static final String CFG_SETTING = "saveInternalsSettings";

    private static final String CF_RDD_SETTINGS = "RDDs";

    private static final String CFG_CONTEXT = "context";

    private static final String CFG_NAMED_RDD_UUIDS = "namedRDDs";

    private static final String CFG_DELETE_ON_RESET = "deleteRDDsOnReset";
    private static final boolean DEFAULT_DELETE_ON_RESET = true;

    private static final String CFG_DELETE_ON_DISPOSE = "deleteRDDsOnDispose";
    private static final boolean DEFAULT_DELETE_ON_DISPOSE = true;

    private final Map<KNIMESparkContext, List<String>> m_namedRDDs = new LinkedHashMap<>();

    private boolean m_deleteOnReset = DEFAULT_DELETE_ON_RESET;

    private boolean m_deleteOnDispose = DEFAULT_DELETE_ON_DISPOSE;

    private boolean m_validateRDDs;

    /**Constructor for class AbstractGraphNodeModel.
     * @param inPortTypes the input port types
     * @param outPortTypes the output port types
     */
    protected SparkNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        this(inPortTypes, outPortTypes, true);
    }

    /**Constructor for class AbstractGraphNodeModel.
     * @param inPortTypes the input port types
     * @param outPortTypes the output port types
     * @param deleteOnReset <code>true</code> if all output Spark RDDs should be deleted when the node is reseted
     * Always set this flag to <code>false</code> when you return the input RDD also as output RDD!
     */
    protected SparkNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes,
        final boolean deleteOnReset) {
        this(inPortTypes, outPortTypes, deleteOnReset, true);
    }

    /**Constructor for class AbstractGraphNodeModel.
     * @param inPortTypes the input port types
     * @param outPortTypes the output port types
     * @param deleteOnReset <code>true</code> if all output Spark RDDs should be deleted when the node is reseted
     * Always set this flag to <code>false</code> when you return the input RDD also as output RDD!
     * @param validateRDDs set to <code>false</code> to disable RDD validation prior execution
     */
    protected SparkNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes,
        final boolean deleteOnReset, final boolean validateRDDs) {
        super(inPortTypes, outPortTypes);
        m_deleteOnReset = deleteOnReset;
        m_validateRDDs = validateRDDs;
    }

    /**
     * @param deleteOnReset <code>true</code> if all output Spark RDDs should be deleted when the node is reseted
     */
    protected void setDeleteOnReset(final boolean deleteOnReset) {
        m_deleteOnReset = deleteOnReset;
    }

    /**
     * @return <code>true</code> if the output RDDs are deleted when the node is reseted
     */
    protected boolean isDeleteOnReset() {
        return m_deleteOnReset;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        SparkPlugin.LICENSE_CHECKER.checkLicenseInNode();
        return configureInternal(inSpecs);
    }

    /**
     *  @param inSpecs The input data table specs. Items of the array could be null if no spec is available from the
     *            corresponding input port (i.e. not connected or upstream node does not produce an output spec). If a
     *            port is of type {@link BufferedDataTable#TYPE} and no spec is available the framework will replace
     *            null by an empty {@link DataTableSpec} (no columns) unless the port is marked as optional as per
     *            constructor.
     * @return The output objects specs or null.
     * @throws InvalidSettingsException If this node can't be configured.
     * @see #configure(PortObjectSpec[])
     */
    protected abstract PortObjectSpec[] configureInternal(PortObjectSpec[] inSpecs)  throws InvalidSettingsException;

    /**
     * {@inheritDoc}
     */
    @Override
    protected final PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Validate input data...");
        SparkPlugin.LICENSE_CHECKER.checkLicenseInNode();
        KNIMESparkContext checkedContext = null;
        if (m_validateRDDs && KNIMEConfigContainer.validateRDDsPriorExecution()) {
            final Map<KNIMESparkContext, Set<String>> namedRDDsMap = new HashMap<>();
            for (final PortObject portObject : inData) {
                if (portObject instanceof  SparkContextProvider) {
                    exec.setMessage("Check Spark context...");
                    //check that the context is still available
                    final SparkContextProvider data = (SparkContextProvider)portObject;
                    final KNIMESparkContext newContext = data.getContext();
                    if (checkedContext == null || !checkedContext.equals(newContext)) {
                        if (!KnimeContext.sparkContextExists(newContext)) {
                            throw new IllegalStateException(
                                "Incoming Spark context no longer exists. Please reset all preceding Spark nodes.");
                        }
                        checkedContext = newContext;
                    }
                    exec.setMessage("Spark context valid.");
                }
                if (portObject instanceof  SparkDataPortObject) {
                    exec.setMessage("Check Spark RDD...");
                    final SparkDataPortObject data = (SparkDataPortObject)portObject;
                    final KNIMESparkContext newContext = data.getContext();
                    Set<String> listNamedRDDs = namedRDDsMap.get(newContext);
                    if (listNamedRDDs == null) {
                        //this is the first time the context is used get all its RDDs and put them into the map
                        listNamedRDDs = new HashSet<>(KnimeContext.listNamedRDDs(newContext));
                        namedRDDsMap.put(newContext, listNamedRDDs);
                    }
                    if (!listNamedRDDs.contains(data.getData().getID())) {
                        //JobServer comment in 'NamedRDDSupport.scala': "The caller should always expect
                        // that the data returned from the method 'getNames()' may be stale and incorrect.
                        //hence: wait a bit and try again
                        Thread.sleep(1000);
                        listNamedRDDs.addAll(KnimeContext.listNamedRDDs(newContext));
                        if (!listNamedRDDs.contains(data.getData().getID())) {
                            throw new IllegalStateException(
                                "Incoming Spark data object no longer exists. Please reset all preceding Spark nodes.");
                        }
                    }
                    exec.setMessage("Spark RDD valid.");
                }
            }
            exec.setMessage("Validation finished.");
        } else {
            LOGGER.debug("RDD validation disabled");
        }
        exec.setMessage("Start execution...");
        final PortObject[] portObjects = executeInternal(inData, exec);
        exec.setMessage("Execution finished.");
        m_namedRDDs.clear();
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
        addSparkObject(sparkObject.getContext(), sparkObject.getTableName());
    }

    private void addSparkObject(final KNIMESparkContext context, final String... RDDIDs) {
        List<String> rdds = m_namedRDDs.get(context);
        if (rdds == null) {
            rdds = new LinkedList<String>();
            m_namedRDDs.put(context, rdds);
        }
        for (String RDDID : RDDIDs) {
            rdds.add(RDDID);
        }
    }

    /**
     * @param context the {@link KNIMESparkContext} the RDDs live in
     * @param RDDIDs the RDD ids to delete when the node is reseted or disposed
     */
    protected final void additionalRDDs2Delete(final KNIMESparkContext context, final String... RDDIDs) {
        addSparkObject(context, RDDIDs);
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
     * {@inheritDoc}
     */
    @Override
    protected final void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
    throws IOException, CanceledExecutionException {
        final File settingFile = new File(nodeInternDir, CFG_FILE);
        try (final FileOutputStream dataOS = new FileOutputStream(settingFile)){
            final Config config = new NodeSettings(CFG_SETTING);
            config.addBoolean(CFG_DELETE_ON_RESET, m_deleteOnReset);
            config.addBoolean(CFG_DELETE_ON_DISPOSE, m_deleteOnDispose);
            final Config rddConfig = config.addConfig(CF_RDD_SETTINGS);
            int idx = 0;
            for (Entry<KNIMESparkContext, List<String>> e : m_namedRDDs.entrySet()) {
                final ConfigWO contextConfig = rddConfig.addConfig(CFG_CONTEXT + idx++);
                final Config contextSettingsConfig = contextConfig.addConfig(CFG_CONTEXT);
                e.getKey().save(contextSettingsConfig);
                exec.checkCanceled();
                contextConfig.addStringArray(CFG_NAMED_RDD_UUIDS, e.getValue().toArray(new String[0]));
            }
            config.saveToXML(dataOS);
        } catch (final Exception e) {
            throw new IOException(e.getMessage(), e.getCause());
        }
        saveAdditionalInternals(nodeInternDir, exec);
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
            m_deleteOnReset = config.getBoolean(CFG_DELETE_ON_RESET, DEFAULT_DELETE_ON_RESET);
            m_deleteOnDispose = config.getBoolean(CFG_DELETE_ON_DISPOSE, DEFAULT_DELETE_ON_DISPOSE);
            final Config rddConfig = config.getConfig(CF_RDD_SETTINGS);
            final int noOfContexts = rddConfig.getChildCount();
            for (int i = 0; i < noOfContexts; i++) {
                final ConfigRO contextConfig = rddConfig.getConfig(CFG_CONTEXT + i);
                final Config contextSettingsConfig = contextConfig.getConfig(CFG_CONTEXT);
                final KNIMESparkContext context = new KNIMESparkContext(contextSettingsConfig);
                final String[] namedRDDUUIDs = contextConfig.getStringArray(CFG_NAMED_RDD_UUIDS);
                m_namedRDDs.put(context, new ArrayList<>(Arrays.asList(namedRDDUUIDs)));
            }
            loadAdditionalInternals(nodeInternDir, exec);
        } catch (final InvalidSettingsException | RuntimeException e) {
            throw new IOException("Failed to load named rdd", e.getCause());
        }
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

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void onDispose() {
        if (m_deleteOnDispose) {
            LOGGER.debug("In onDispose() of SparkNodeModel. Calling deleteRDDs.");
            deleteRDDs(true);
        }
        onDisposeInternal();
    }

    /**
     * Called when the node is reseted.
     */
    protected void onDisposeInternal() {
        //override if you need to dispose anything when the node is deleted or the workflow closed
    }

    /**
     * {@inheritDoc}
     * Gets called when the node is reseted or deleted.
     */
    @Override
    protected final void reset() {
        if (m_deleteOnReset) {
            deleteRDDs(false);
        } else {
            //if we do not delete the rdds we have to clear at least the rdds list
            m_namedRDDs.clear();
        }
        resetInternal();
    }

    /**
     * Called when the node is reseted.
     */
    protected void resetInternal() {
        //override if you need to reset anything
    }


    private void deleteRDDs(final boolean onDispose) {
        if (m_deleteOnReset && m_namedRDDs != null && !m_namedRDDs.isEmpty()) {
            LOGGER.debug("In reset of SparkNodeModel. Deleting named rdds.");
            Future<?> future = KNIMEConstants.GLOBAL_THREAD_POOL.enqueue(new Runnable() {
                @Override
                public void run() {
                    final long startTime = System.currentTimeMillis();
                    for (Entry<KNIMESparkContext, List<String>> e : m_namedRDDs.entrySet()) {
                        try {
                            final KNIMESparkContext context = e.getKey();
                            if (!onDispose || context.deleteRDDsOnDispose()) {
                                KnimeContext.deleteNamedRDDs(context, e.getValue().toArray(new String[0]));
                            }
                        } catch (final Throwable ex) {
                            LOGGER.error("Exception while deleting named RDDs for context: "
                                   + e.getKey() + " Exception: " + ex.getMessage(), ex);
                        }
                    }
                    final long endTime = System.currentTimeMillis();
                        final long durationTime = endTime - startTime;
                    LOGGER.debug("Time deleting " + m_namedRDDs.size() + " namedRDD(s): " + durationTime + " ms");
                    m_namedRDDs.clear();
                }
            });
            try {
                //give the thread at least 1 seconds to delete the rdds
                future.get(1, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                LOGGER.info("Deleting RDDs on node " + (onDispose ? "dispose" : "reset")
                    + " was interrupted prior completion.");
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.warn("Deleting RDDs on node " + (onDispose ? "dispose" : "reset")
                    + " failed. Error: " + e.getMessage(), e);
            }
        }
    }


    /**
     * @param inputRDD the original {@link SparkDataPortObject}
     * @param newSpec the new {@link DataTableSpec} of the input {@link SparkDataPortObject}
     * @param resultRDDName the name of the result RDD
     * @return {@link SparkDataPortObject}
     */
    public static PortObject createSparkPortObject(final SparkDataPortObject inputRDD, final DataTableSpec newSpec,
        final String resultRDDName) {
        return new SparkDataPortObject(
            new SparkDataTable(inputRDD.getContext(), resultRDDName, newSpec));
    }


    /**
     * @param inputRDD the original {@link SparkDataPortObject}
     * @param newSpec the new {@link DataTableSpec} of the input {@link SparkDataPortObject}
     * @return the a new {@link SparkDataPortObject} based on the input object and spec
     */
    public static SparkDataPortObject createSparkPortObject(final SparkDataPortObject inputRDD,
        final DataTableSpec newSpec) {
        final SparkDataTable renamedRDD = new SparkDataTable(inputRDD.getContext(), inputRDD.getTableName(), newSpec);
        SparkDataPortObject result = new SparkDataPortObject(renamedRDD);
        return result;
    }


    /**
     * @param inputRDD the {@link SparkDataPortObject} that provides the context and the table spec
     * @param resultRDDName the name of the result RDD
     * @return {@link SparkDataPortObject}
     */
    public static SparkDataPortObject createSparkPortObject(final SparkDataPortObject inputRDD,
        final String resultRDDName) {
        return new SparkDataPortObject(
            new SparkDataTable(inputRDD.getContext(), resultRDDName, inputRDD.getTableSpec()));
    }
}
