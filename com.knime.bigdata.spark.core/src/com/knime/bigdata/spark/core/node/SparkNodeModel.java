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

package com.knime.bigdata.spark.core.node;

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

import org.knime.core.data.DataTableSpec;
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
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.core.SparkPlugin;
import com.knime.bigdata.spark.core.context.SparkContext;
import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextManager;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.exception.MissingJobException;
import com.knime.bigdata.spark.core.exception.MissingSparkModelHelperException;
import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.JobRunFactory;
import com.knime.bigdata.spark.core.job.ModelJobOutput;
import com.knime.bigdata.spark.core.job.SimpleJobRunFactory;
import com.knime.bigdata.spark.core.job.util.MLlibSettings;
import com.knime.bigdata.spark.core.port.SparkContextProvider;
import com.knime.bigdata.spark.core.port.context.SparkContextConfig;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;
import com.knime.bigdata.spark.core.port.model.SparkModel;
import com.knime.bigdata.spark.core.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Basic class that all NodeModel classes need to extend if they work with Spark data/model/etc objects.
 *
 * @author Tobias Koetter, University of Konstanz
 */
public abstract class SparkNodeModel extends NodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkNodeModel.class);

    private static final String CFG_FILE = "settingsFile.xml";

    private static final String CFG_SETTING = "saveInternalsSettings";

    private static final String CF_RDD_SETTINGS = "RDDs";

    /**
     * Key required to load legacy workflows (KNIME Spark Executor <= v1.3)
     */
    private static final String CFG_CONTEXT_LEGACY = "context";

    /**
     * Key required to load current workflows (KNIME Spark Executor >v1.3)
     */
    private static final String CFG_CONTEXT_ID = "contextID";

    private static final String CFG_NAMED_RDD_UUIDS = "namedRDDs";

    private static final String CFG_DELETE_ON_RESET = "deleteRDDsOnReset";

    private static final boolean DEFAULT_DELETE_ON_RESET = true;

    private final Map<SparkContextID, List<String>> m_namedRDDs = new LinkedHashMap<>();

    private boolean m_deleteOnReset = DEFAULT_DELETE_ON_RESET;

    private final List<File> filesToDeleteAfterExecute = new LinkedList<>();

    private boolean m_automaticHandling = true;

    /**
     * Constructor for SparkNodeModel
     *
     * @param inPortTypes the input port types
     * @param outPortTypes the output port types
     */
    protected SparkNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        this(inPortTypes, outPortTypes, true);
    }

    /**
     * Base constructor for SparkNodeModel.
     *
     * @param inPortTypes the input port types
     * @param outPortTypes the output port types
     * @param deleteOnReset <code>true</code> if all output Spark RDDs should be deleted when the node is reseted Always
     *            set this flag to <code>false</code> when you return the input RDD also as output RDD!
     */
    protected SparkNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes, final boolean deleteOnReset) {
        super(inPortTypes, outPortTypes);
        m_deleteOnReset = deleteOnReset;
    }

    protected void deleteFilesAfterExecute() {
        for (File toDelete : filesToDeleteAfterExecute) {
            try {
                toDelete.delete();
            } catch (Exception e) {
                // do nothing
            }
        }

        filesToDeleteAfterExecute.clear();
    }

    protected void addFileToDeleteAfterExecute(final File toDelete) {
        filesToDeleteAfterExecute.add(toDelete);
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
     * @param inSpecs The input data table specs. Items of the array could be null if no spec is available from the
     *            corresponding input port (i.e. not connected or upstream node does not produce an output spec). If a
     *            port is of type {@link BufferedDataTable#TYPE} and no spec is available the framework will replace
     *            null by an empty {@link DataTableSpec} (no columns) unless the port is marked as optional as per
     *            constructor.
     * @return The output objects specs or null.
     * @throws InvalidSettingsException If this node can't be configured.
     * @see #configure(PortObjectSpec[])
     */
    protected abstract PortObjectSpec[] configureInternal(PortObjectSpec[] inSpecs) throws InvalidSettingsException;

    /**
     * {@inheritDoc}
     */
    @Override
    protected final PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Validate input data...");
        SparkPlugin.LICENSE_CHECKER.checkLicenseInNode();

        try {
            exec.setMessage("Starting node execution...");
            final PortObject[] portObjects = executeInternal(inData, exec);
            exec.setMessage("Node execution finished.");
            if (m_automaticHandling && portObjects != null && portObjects.length > 0) {
                for (final PortObject portObject : portObjects) {
                    if (portObject instanceof SparkDataPortObject) {
                        addSparkObject((SparkDataPortObject)portObject);
                    }
                }
            }
            return portObjects;
        } finally {
            deleteFilesAfterExecute();
        }
    }

    private void addSparkObject(final SparkDataPortObject sparkObject) {
        addSparkObject(sparkObject.getContextID(), sparkObject.getTableName());
    }

    private void addSparkObject(final SparkContextID context, final String... RDDIDs) {
        List<String> rdds = m_namedRDDs.get(context);
        if (rdds == null) {
            rdds = new LinkedList<>();
            m_namedRDDs.put(context, rdds);
        }
        for (String RDDID : RDDIDs) {
            rdds.add(RDDID);
        }
    }

    /**
     * Can be used to disable automatic RDD deletion.
     * <b>Caution:</b> Disabling the RDD handling might result in resource problems
     * on the Spark job server. RDDs that are added via the {@link #additionalRDDs2Delete(SparkContextID, String...)} method
     * are deleted on the Spark server even if automatic RDD handling is disabled.
     * @param automaticHandling <code>false</code> if the automatic RDD deletion handling should be disabled
     * @see #additionalRDDs2Delete(SparkContextID, String...)
     */
    protected void setAutomticRDDHandling(final boolean automaticHandling) {
        m_automaticHandling = automaticHandling;
    }

    /**
     * @param context the {@link SparkContextConfig} the RDDs live in
     * @param RDDIDs the RDD ids to delete when the node is reseted or disposed
     */
    protected final void additionalRDDs2Delete(final SparkContextID context, final String... RDDIDs) {
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
        try (final FileOutputStream dataOS = new FileOutputStream(settingFile)) {
            final Config config = new NodeSettings(CFG_SETTING);
            config.addBoolean(CFG_DELETE_ON_RESET, m_deleteOnReset);
            final Config rddConfig = config.addConfig(CF_RDD_SETTINGS);
            int idx = 0;
            for (Entry<SparkContextID, List<String>> e : m_namedRDDs.entrySet()) {
                final ConfigWO contextConfig = rddConfig.addConfig(CFG_CONTEXT_ID + idx++);
                final Config contextSettingsConfig = contextConfig.addConfig(CFG_CONTEXT_ID);
                e.getKey().saveToConfigWO(contextSettingsConfig);
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
        try (final FileInputStream inData = new FileInputStream(settingFile)) {
            final ConfigRO config = NodeSettings.loadFromXML(inData);
            m_deleteOnReset = config.getBoolean(CFG_DELETE_ON_RESET, DEFAULT_DELETE_ON_RESET);
            final Config rddConfig = config.getConfig(CF_RDD_SETTINGS);
            final int noOfContexts = rddConfig.getChildCount();
            for (int i = 0; i < noOfContexts; i++) {
                if (rddConfig.containsKey(CFG_CONTEXT_ID + i)) {
                    final ConfigRO contextConfig = rddConfig.getConfig(CFG_CONTEXT_ID + i);
                    final Config contextSettingsConfig = contextConfig.getConfig(CFG_CONTEXT_ID);
                    final SparkContextID contextID = SparkContextID.fromConfigRO(contextSettingsConfig);
                    final String[] namedRDDUUIDs = contextConfig.getStringArray(CFG_NAMED_RDD_UUIDS);
                    m_namedRDDs.put(contextID, new ArrayList<>(Arrays.asList(namedRDDUUIDs)));
                } else if (rddConfig.containsKey(CFG_CONTEXT_LEGACY + i)) {
                    // Load legacy workflow (KNIME Spark Executor <= v1.3)
                    final ConfigRO contextConfig = rddConfig.getConfig(CFG_CONTEXT_LEGACY + i);
                    final String[] namedRDDUUIDs = contextConfig.getStringArray(CFG_NAMED_RDD_UUIDS);
                    m_namedRDDs.put(
                        SparkContextConfig
                            .createSparkContextIDFromLegacyConfig(contextConfig.getConfig(CFG_CONTEXT_LEGACY)),
                        new ArrayList<>(Arrays.asList(namedRDDUUIDs)));
                }
            }
            loadAdditionalInternals(nodeInternDir, exec);
        } catch (final InvalidSettingsException | RuntimeException e) {
            throw new IOException("Failed to load RDD settings.", e.getCause());
        }
    }

    /**
     * Load internals into the derived <code>NodeModel</code>. This method is only called if the <code>Node</code> was
     * executed. Read all your internal structures from the given file directory to create your internal data structure
     * which is necessary to provide all node functionalities after the workflow is loaded, e.g. view content and/or
     * hilite mapping. <br>
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
     * Save internals of the derived <code>NodeModel</code>. This method is only called if the <code>Node</code> is
     * executed. Write all your internal structures into the given file directory which are necessary to recreate this
     * model when the workflow is loaded, e.g. view content and/or hilite mapping.<br>
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
        LOGGER.debug("In onDispose() of SparkNodeModel. Calling deleteRDDs.");
        deleteRDDs(true);

        onDisposeInternal();
    }

    /**
     * Called when the node is reseted.
     */
    protected void onDisposeInternal() {
        //override if you need to dispose anything when the node is deleted or the workflow closed
    }

    /**
     * {@inheritDoc} Gets called when the node is reseted or deleted.
     */
    @Override
    protected final void reset() {
        if (m_deleteOnReset) {
            LOGGER.debug("In reset() of SparkNodeModel. Calling deleteRDDs.");
            deleteRDDs(false);
        }
        m_namedRDDs.clear();
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
            LOGGER.debug("In reset of SparkNodeModel. Deleting named rdds. On dispose: " + onDispose);
            if (KNIMEConfigContainer.verboseLogging()) {
                LOGGER.debug("RDDS in delete queue: " + m_namedRDDs);
            }

            //make a copy of the rdds to delete for the deletion thread
            final Map<SparkContextID, String[]> rdds2delete = new HashMap<>(m_namedRDDs.size());
            for (Entry<SparkContextID, List<String>> e : m_namedRDDs.entrySet()) {
                final SparkContextID contextID = e.getKey();
                // mark for deletion if we are either resetting, or disposing and deleteRDDsOnDispose is on
                SparkContext context = SparkContextManager.getOrCreateSparkContext(contextID);

                if (!onDispose
                    || (context.getConfiguration() != null && context.getConfiguration().deleteObjectsOnDispose())) {
                    rdds2delete.put(contextID, e.getValue().toArray(new String[0]));
                }
            }

            if (!rdds2delete.isEmpty()) {
                if (KNIMEConfigContainer.verboseLogging()) {
                    LOGGER.debug("RDDS to delete: " + rdds2delete);
                }
                SparkPlugin.getDefault().addJob(new Runnable() {
                    @Override
                    public void run() {
                        final long startTime = System.currentTimeMillis();
                        if (KNIMEConfigContainer.verboseLogging()) {
                            LOGGER.debug("Deleting rdds: " + rdds2delete);
                        }
                        for (Entry<SparkContextID, String[]> e : rdds2delete.entrySet()) {
                            try {
                                SparkContext context = SparkContextManager.getOrCreateSparkContext(e.getKey());
                                context.deleteNamedObjects(new HashSet<>(Arrays.asList(e.getValue())));
                            } catch (final Throwable ex) {
                                LOGGER.error("Exception while deleting named RDDs for context: " + e.getKey()
                                    + " Exception: " + ex.getMessage(), ex);
                            }
                        }
                        if (KNIMEConfigContainer.verboseLogging()) {
                            final long endTime = System.currentTimeMillis();
                            final long durationTime = endTime - startTime;
                            LOGGER
                                .debug("Time deleting " + rdds2delete.size() + " namedRDD(s): " + durationTime + " ms");
                        }
                    }
                });
            }
        }
    }

    /**
     * @param data the input {@link SparkDataPortObject} to get the {@link SparkVersion}
     * @param modelName the unique name of the model
     * @param settings {@link MLlibSettings}
     * @param model the {@link ModelJobOutput} to get the model
     * @return the {@link SparkModelPortObject}
     * @throws MissingSparkModelHelperException
     */
    public static SparkModelPortObject createSparkModelPortObject(final SparkDataPortObject data,
        final String modelName, final MLlibSettings settings, final ModelJobOutput model) throws MissingSparkModelHelperException {
        return new SparkModelPortObject(new SparkModel(getSparkVersion(data), modelName,
            model.getModel(), settings));
    }

    /**
     * @param data the {@link SparkDataPortObject} to get the {@link SparkContextID} from
     * @param jobId the unique job id of the {@link JobRunFactory}
     * @return the corresponding {@link JobRunFactory}
     * @throws MissingJobException if no job is available for the given {@link SparkContextID} and job id
     */
    public static <I extends JobInput, O extends JobOutput> JobRunFactory<I, O> getJobRunFactory(
        final SparkDataPortObject data, final String jobId)
        throws MissingJobException {
        return SparkContextUtil.getJobRunFactory(data.getContextID(), jobId);
    }

    /**
     * @param data the {@link SparkDataPortObject} to get the {@link SparkContextID} from
     * @param jobId the unique job id of the {@link JobRunFactory}
     * @return the corresponding {@link SimpleJobRunFactory}
     * @throws MissingJobException if no job is available for the given {@link SparkContextID} and job id
     */
    public static <I extends JobInput> SimpleJobRunFactory<I> getSimpleJobRunFactory(
        final SparkDataPortObject data, final String jobId) throws MissingJobException {
        return SparkContextUtil.getSimpleRunFactory(data.getContextID(), jobId);
    }

    /**
     * @param provider {@link SparkContextProvider}
     * @return the {@link SparkVersion} of the {@link SparkContextProvider}
     */
    public static SparkVersion getSparkVersion(final SparkContextProvider provider) {
        return SparkContextManager.getOrCreateSparkContext(provider.getContextID()).getSparkVersion();
    }

    /**
     * @param inputRDD the original {@link SparkDataPortObject}
     * @param dataTable the {@link SparkDataTable} object
     * @return {@link SparkDataPortObject}
     */
    public static PortObject createSparkPortObject(final SparkDataPortObject inputRDD, final SparkDataTable dataTable) {
        return createSparkPortObject(inputRDD, dataTable.getTableSpec(), dataTable.getID());
    }

    /**
     * @param inputRDD the original {@link SparkDataPortObject}
     * @param newSpec the new {@link DataTableSpec} of the input {@link SparkDataPortObject}
     * @param resultRDDName the name of the result RDD
     * @return {@link SparkDataPortObject}
     */
    public static PortObject createSparkPortObject(final SparkDataPortObject inputRDD, final DataTableSpec newSpec,
        final String resultRDDName) {
        return new SparkDataPortObject(new SparkDataTable(inputRDD.getContextID(), resultRDDName, newSpec));
    }

    /**
     * @param inputRDD the original {@link SparkDataPortObject}
     * @param newSpec the new {@link DataTableSpec} of the input {@link SparkDataPortObject}
     * @return the a new {@link SparkDataPortObject} based on the input object and spec
     */
    public static SparkDataPortObject createSparkPortObject(final SparkDataPortObject inputRDD,
        final DataTableSpec newSpec) {
        final SparkDataTable renamedRDD = new SparkDataTable(inputRDD.getContextID(), inputRDD.getTableName(), newSpec);
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
            new SparkDataTable(inputRDD.getContextID(), resultRDDName, inputRDD.getTableSpec()));
    }
}
