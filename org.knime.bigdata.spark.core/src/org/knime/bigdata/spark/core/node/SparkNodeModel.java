/*
 * -------------------------------------------------------------------
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
 * -------------------------------------------------------------------
 *
 * History
 *    23.07.2008 (Tobias Koetter): created
 */

package org.knime.bigdata.spark.core.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.MissingJobException;
import org.knime.bigdata.spark.core.exception.MissingSparkModelHelperException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.JobRunFactory;
import org.knime.bigdata.spark.core.job.ModelJobOutput;
import org.knime.bigdata.spark.core.job.SimpleJobRunFactory;
import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.port.SparkContextProvider;
import org.knime.bigdata.spark.core.port.context.JobServerSparkContextConfig;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.bigdata.spark.core.port.data.SparkData;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.port.model.SparkModel;
import org.knime.bigdata.spark.core.port.model.SparkModelPortObject;
import org.knime.bigdata.spark.core.util.BackgroundTasks;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.config.Config;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Basic class that all NodeModel classes need to extend if they work with Spark data/model/etc objects.
 *
 * @author Tobias Koetter, KNIME GmbH
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public abstract class SparkNodeModel extends NodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkNodeModel.class);

    private static final String CFG_FILE = "settingsFile.xml";

    private static final String CFG_SETTING = "saveInternalsSettings";

    /** Saved as "RDDs" for historical reasons */
    private static final String CFG_SPARK_DATA_OBJECTS = "RDDs";

    /**
     * Key required to load legacy workflows (KNIME Extension for Apache Spark <= v1.3)
     * @since 1.6.0
     */
    private static final String CFG_CONTEXT_LEGACY = "context";

    /**
     * Key required to load current workflows (KNIME Extension for Apache Spark >v1.3)
     * @since 1.6.0
     */
    private static final String CFG_CONTEXT_ID = "contextID";

    /** Saved as "namedRDDs" for historical reasons */
    private static final String CFG_SPARK_DATA_OBJECT_IDS = "namedRDDs";

    private static final String CFG_DELETE_ON_RESET = "deleteRDDsOnReset";

    private final Map<SparkContextID, List<String>> m_sparkDataObjects = new LinkedHashMap<>();

    private static final boolean DEFAULT_DELETE_ON_RESET = true;

    private boolean m_deleteOnReset = DEFAULT_DELETE_ON_RESET;

    private final List<File> m_filesToDeleteAfterExecute = new LinkedList<>();

    private boolean m_automaticHandling = true;

    /**
     * Constructor for SparkNodeModel.
     *
     * @param inPortTypes The input port types.
     * @param outPortTypes The output port types.
     */
    protected SparkNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        this(inPortTypes, outPortTypes, true);
    }

    /**
     * Base constructor for SparkNodeModel.
     *
     * @param inPortTypes The input port types.
     * @param outPortTypes The output port types.
     * @param deleteOnReset <code>true</code> if all {@link SparkData} objects produced by this node model should be
     *            deleted when the node is reset. Always set this flag to <code>false</code> when you return an ingoing
     *            {@link SparkData} object in an output port.
     */
    protected SparkNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes, final boolean deleteOnReset) {
        super(inPortTypes, outPortTypes);
        m_deleteOnReset = deleteOnReset;
    }

    /**
     * This method is called in the finally block of the {@link #execute(PortObject[], ExecutionContext)} method to
     * delete all files that have been registered with the {@link #addFileToDeleteAfterExecute(File)}.
     */
    protected void deleteFilesAfterExecute() {
        for (File toDelete : m_filesToDeleteAfterExecute) {
            try {
                toDelete.delete();
            } catch (Exception e) {
                // do nothing
            }
        }

        m_filesToDeleteAfterExecute.clear();
    }

    /**
     * @param toDelete {@link File} that should be deleted once the node is executed.
     * @see #deleteFilesAfterExecute()
     */
    protected void addFileToDeleteAfterExecute(final File toDelete) {
        m_filesToDeleteAfterExecute.add(toDelete);
    }

    /**
     * @param deleteOnReset <code>true</code> if all {@link SparkData} objects created by this node model should be
     *            deleted when the node is reset.
     */
    protected void setDeleteOnReset(final boolean deleteOnReset) {
        m_deleteOnReset = deleteOnReset;
    }

    /**
     * @return <code>true</code> if all {@link SparkData} objects created by this node model are deleted when the node
     *         is reset.
     */
    protected boolean isDeleteOnReset() {
        return m_deleteOnReset;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
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
        try {
            exec.setMessage("Starting node execution...");
            final PortObject[] portObjects = executeInternal(inData, exec);
            exec.setMessage("Node execution finished.");
            if (portObjects != null && portObjects.length > 0) {
                for (final PortObject portObject : portObjects) {
                    if (portObject instanceof SparkDataPortObject) {
                        final SparkData sparkData = ((SparkDataPortObject)portObject).getData();

                        if (m_automaticHandling) {
                            addSparkDataObject(sparkData);
                        }
                        assert sparkData
                            .getStatistics() != null : "Missing named object statistics for Spark data object found";
                    }
                }
            }
            return portObjects;
        } finally {
            deleteFilesAfterExecute();
        }
    }

    private void addSparkDataObject(final SparkData sparkData) {
        addSparkDataObjects(sparkData.getContextID(), sparkData.getID());
    }

    private void addSparkDataObjects(final SparkContextID context, final String... ids) {
        List<String> idsInContext = m_sparkDataObjects.get(context);
        if (idsInContext == null) {
            idsInContext = new LinkedList<>();
            m_sparkDataObjects.put(context, idsInContext);
        }
        for (String id : ids) {
            idsInContext.add(id);
        }
    }

    /**
     * Can be used to disable automatic handling of produced {@link SparkData} objects. <b>Caution:</b> Disabling
     * automatic handling might result in resource problems in the Spark cluster. {@link SparkData} objects that are added via the
     * {@link #addAdditionalSparkDataObjectsToDelete(SparkContextID, String...)} method are deleted from the Spark context even if
     * automatic deletion is disabled.
     *
     * @param automaticHandling Whether automatic handling of deletion of {@link SparkData} objects should be enabled or not.
     * @see #addAdditionalSparkDataObjectsToDelete(SparkContextID, String...)
     * @since 2.2.0 (renamed from setAutomticRDDHandling)
     */
    protected void setAutomaticSparkDataHandling(final boolean automaticHandling) {
        m_automaticHandling = automaticHandling;
    }

    /**
     * @param context the ID of the Spark context that the data objects belong to
     * @param ids the Spark data object IDs to delete when the node is reset or disposed.
     * @since 2.2.0 (renamed from additionalRDDs2Delete)
     */
    protected final void addAdditionalSparkDataObjectsToDelete(final SparkContextID context, final String... ids) {
        addSparkDataObjects(context, ids);
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
            final Config sparkDataObjectsConfig = config.addConfig(CFG_SPARK_DATA_OBJECTS);
            int idx = 0;
            for (Entry<SparkContextID, List<String>> e : m_sparkDataObjects.entrySet()) {
                final ConfigWO contextConfig = sparkDataObjectsConfig.addConfig(CFG_CONTEXT_ID + idx++);
                final Config contextSettingsConfig = contextConfig.addConfig(CFG_CONTEXT_ID);
                e.getKey().saveToConfigWO(contextSettingsConfig);
                exec.checkCanceled();
                contextConfig.addStringArray(CFG_SPARK_DATA_OBJECT_IDS, e.getValue().toArray(new String[0]));
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
            final Config sparkDataObjectsConfig = config.getConfig(CFG_SPARK_DATA_OBJECTS);
            final int noOfContexts = sparkDataObjectsConfig.getChildCount();
            for (int i = 0; i < noOfContexts; i++) {
                if (sparkDataObjectsConfig.containsKey(CFG_CONTEXT_ID + i)) {
                    final ConfigRO contextConfig = sparkDataObjectsConfig.getConfig(CFG_CONTEXT_ID + i);
                    final Config contextSettingsConfig = contextConfig.getConfig(CFG_CONTEXT_ID);
                    final SparkContextID contextID = SparkContextID.fromConfigRO(contextSettingsConfig);
                    final String[] sparkDataObjectIDs = contextConfig.getStringArray(CFG_SPARK_DATA_OBJECT_IDS);
                    m_sparkDataObjects.put(contextID, new ArrayList<>(Arrays.asList(sparkDataObjectIDs)));
                } else if (sparkDataObjectsConfig.containsKey(CFG_CONTEXT_LEGACY + i)) {
                    // Load legacy workflow (KNIME Extension for Apache Spark <= v1.3)
                    final ConfigRO contextConfig = sparkDataObjectsConfig.getConfig(CFG_CONTEXT_LEGACY + i);
                    final String[] sparkDataObjectIDs = contextConfig.getStringArray(CFG_SPARK_DATA_OBJECT_IDS);
                    m_sparkDataObjects.put(
                        JobServerSparkContextConfig
                            .createSparkContextIDFromLegacyConfig(contextConfig.getConfig(CFG_CONTEXT_LEGACY)),
                        new ArrayList<>(Arrays.asList(sparkDataObjectIDs)));
                }
            }
            loadAdditionalInternals(nodeInternDir, exec);
        } catch (final InvalidSettingsException | RuntimeException e) {
            if(e.getMessage() != null) {
                throw new IOException(e.getMessage(), e);
            } else {
                throw new IOException("Failed to load internals", e);
            }
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
    protected final void saveSettingsTo(final NodeSettingsWO settings) {
        // doing nothing right now, reserved for future use

        saveAdditionalSettingsTo(settings);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected final void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        // currently does nothing. reserved for future use.
        validateAdditionalSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {

        // doing nothing right now, reserved for future use

        loadAdditionalValidatedSettingsFrom(settings);
    }


    /**
     * Override this method to save additional node settings. This method is called by
     * {@link #saveSettingsTo(NodeSettingsWO)} when the current settings need to be saved or transfered to the node's
     * dialog.
     * <p>
     * See {@link #saveSettingsTo(NodeSettingsWO)} for further documentation.
     *
     * @param settings The object to write settings into.
     *
     * @see #loadAdditionalValidatedSettingsFrom(NodeSettingsRO)
     * @see #validateAdditionalSettings(NodeSettingsRO)
     * @see #saveSettingsTo(NodeSettingsWO)
     *
     * @since 2.2.0
     */
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        // empty implementation, expected to be overriden by subclasses
    }

    /**
     * Override this method to validate additional node settings. This method is called by
     * {@link #validateSettings(NodeSettingsRO)} to validate the additional node settings in the passed
     * <code>NodeSettings</code> object.
     * <p>
     * See {@link #validateSettings(NodeSettingsRO)} for further documentation.
     *
     * @param settings The settings to validate.
     * @throws InvalidSettingsException If the validation of the settings failed.
     *
     * @see #validateSettings(NodeSettingsRO)
     * @see #saveAdditionalSettingsTo(NodeSettingsWO)
     * @see #loadAdditionalValidatedSettingsFrom(NodeSettingsRO)
     *
     * @since 2.2.0
     */
    protected void validateAdditionalSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        // empty implementation, expected to be overriden by subclasses
    }


    /**
     * Override this method to load additional validated node settings. This method is called by
     * {@link #loadValidatedSettingsFrom(NodeSettingsRO)} to load additional validated node settings in the passed
     * <code>NodeSettings</code> object.
     * <p>
     * See {@link #loadValidatedSettingsFrom(NodeSettingsRO)} for further documentation.
     *
     * @param settings The settings to read.
     *
     * @throws InvalidSettingsException If a property is not available.
     *
     * @see #saveAdditionalSettingsTo(NodeSettingsWO)
     * @see #validateAdditionalSettings(NodeSettingsRO)
     * @see #loadValidatedSettingsFrom(NodeSettingsRO)
     *
     * @since 2.2.0
     */
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        // empty implementation, expected to be overriden by subclasses
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected final void onDispose() {
        LOGGER.debug("In onDispose() of SparkNodeModel. Calling deleteSparkDataObjects.");
        deleteSparkDataObjects(true);

        onDisposeInternal();
    }

    /**
     * Called when the node is reseted.
     */
    protected void onDisposeInternal() {
        //override if you need to dispose anything when the node is deleted or the workflow closed
    }

    /**
     * {@inheritDoc} Gets called when the node is reset or deleted.
     */
    @Override
    protected final void reset() {
        if (m_deleteOnReset) {
            LOGGER.debug("In reset() of SparkNodeModel. Calling deleteSparkDataObjects.");
            deleteSparkDataObjects(false);
        }
        m_sparkDataObjects.clear();
        resetInternal();
    }

    /**
     * Called when the node is reset.
     */
    protected void resetInternal() {
        //override if you need to reset anything
    }


    private void deleteSparkDataObjects(final boolean onDispose) {
        if (m_deleteOnReset && m_sparkDataObjects != null && !m_sparkDataObjects.isEmpty()) {
            final Map<SparkContextID, String[]> toDelete = collectNamedObjectsToDelete(onDispose);

            if (!toDelete.isEmpty()) {
                BackgroundTasks.run(new DeleteNamedObjectsTask(toDelete));
            }
        }
    }

    private Map<SparkContextID, String[]> collectNamedObjectsToDelete(final boolean onDispose) {
        final Map<SparkContextID, String[]> toDelete = new HashMap<>(m_sparkDataObjects.size());

        for (Entry<SparkContextID, List<String>> e : m_sparkDataObjects.entrySet()) {
            final SparkContextID contextID = e.getKey();
            // mark for deletion if we are either resetting, or disposing and deleteObjectsOnDispose is on
            @SuppressWarnings("rawtypes")
            final SparkContext context = SparkContextManager.getOrCreateSparkContext(contextID);
            final SparkContextConfig contextCfg = context.getConfiguration();

            if (!onDispose || (contextCfg != null && contextCfg.deleteObjectsOnDispose())) {
                toDelete.put(contextID, e.getValue().toArray(new String[0]));
            }
        }
        return toDelete;
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
        final String modelName, final MLlibSettings settings, final ModelJobOutput model)
        throws MissingSparkModelHelperException {
        return new SparkModelPortObject(new SparkModel(getSparkVersion(data), modelName, model.getModel(), settings));
    }

    /**
     * @param data the {@link SparkDataPortObject} to get the {@link SparkContextID} from
     * @param jobId the unique job id of the {@link JobRunFactory}
     * @return the corresponding {@link JobRunFactory}
     * @throws MissingJobException if no job is available for the given {@link SparkContextID} and job id
     */
    public static <I extends JobInput, O extends JobOutput> JobRunFactory<I, O>
        getJobRunFactory(final SparkDataPortObject data, final String jobId) throws MissingJobException {
        return SparkContextUtil.getJobRunFactory(data.getContextID(), jobId);
    }

    /**
     * @param data the {@link SparkDataPortObject} to get the {@link SparkContextID} from
     * @param jobId the unique job id of the {@link JobRunFactory}
     * @return the corresponding {@link SimpleJobRunFactory}
     * @throws MissingJobException if no job is available for the given {@link SparkContextID} and job id
     */
    public static <I extends JobInput> SimpleJobRunFactory<I> getSimpleJobRunFactory(final SparkDataPortObject data,
        final String jobId) throws MissingJobException {
        return SparkContextUtil.getSimpleRunFactory(data.getContextID(), jobId);
    }

    /**
     * @param provider {@link SparkContextProvider}
     * @return the {@link SparkVersion} of the {@link SparkContextProvider}.
     */
    public static SparkVersion getSparkVersion(final SparkContextProvider provider) {
        return SparkContextManager.getOrCreateSparkContext(provider.getContextID()).getSparkVersion();
    }

    /**
     * @param sparkDataPortObject The original {@link SparkDataPortObject} to inherit the Spark context from.
     * @param newSpec The {@link DataTableSpec} of the new {@link SparkDataTable} to wrap.
     * @param newSparkObjectID The ID of the new {@link SparkDataTable} to wrap.
     * @return a new {@link SparkDataPortObject}.
     * @since 2.2.0
     */
    public static PortObject createSparkPortObject(final SparkDataPortObject sparkDataPortObject,
        final DataTableSpec newSpec, final String newSparkObjectID) {

        return new SparkDataPortObject(new SparkDataTable(sparkDataPortObject.getContextID(), newSparkObjectID, newSpec));
    }

    /**
     * @param sparkDataPortObject The original {@link SparkDataPortObject} to inherit the Spark context the table spec
     *            from.
     * @param newSparkObjectID The ID of the new {@link SparkDataTable} to wrap.
     * @return a new {@link SparkDataPortObject}.
     */
    public SparkDataPortObject createSparkPortObject(final SparkDataPortObject sparkDataPortObject,
        final String newSparkObjectID) {
        return new SparkDataPortObject(new SparkDataTable(sparkDataPortObject.getContextID(), newSparkObjectID,
            sparkDataPortObject.getTableSpec()));
    }
}
