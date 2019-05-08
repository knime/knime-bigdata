/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Mar 2, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.context;

import java.util.Objects;
import java.util.Set;

import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectStatistics;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectsController;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.exception.SparkContextNotFoundException;
import org.knime.bigdata.spark.core.jar.JobJar;
import org.knime.bigdata.spark.core.jar.SparkJarRegistry;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.JobRun;
import org.knime.bigdata.spark.core.job.JobWithFilesRun;
import org.knime.bigdata.spark.core.job.SimpleJobRun;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;

/**
 * Superclass for all Spark context implementations. Spark context here means a client-local handle to talk to the
 * (actual) remote Spark context inside the cluster.
 *
 * NOTE: Implementations must be thread-safe.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @param <T> The class from which to read configuration data.
 */
public abstract class SparkContext<T extends SparkContextConfig> implements JobController, NamedObjectsController {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkContext.class);

    /**
     * The ID of the Spark context. Never null, and never changes.
     */
    private final SparkContextID m_contextID;

    /**
     * The current status of the Spark context. Never null.
     */
    private SparkContextStatus m_status;

    /**
     * Holds a reference to the job jar. May be null when the context is {@link SparkContextStatus#NEW} or
     * {@link SparkContextStatus#CONFIGURED}, otherwise it can be assumed to be set.
     */
    private JobJar m_jobJar;

    /**
     * The current configuration of the Spark context. May be null when the context is {@link SparkContextStatus#NEW},
     * otherwise it can be assumed to be set.
     */
    private T m_config;

    /**
     * Possible context states.
     */
    public enum SparkContextStatus {
            /** The context has an ID but no configuration has been provided yet. */
            NEW,

            /** The context has an ID and a configuration. */
            CONFIGURED,

            /**
             * The context is fully configured and the last time someone checked, there was a remote Spark context
             * running
             */
            OPEN
    }

    /**
     * Creates a new Spark context.
     *
     * @param contextID The identfier for this context.
     */
    public SparkContext(final SparkContextID contextID) {
        m_contextID = contextID;
        m_status = SparkContextStatus.NEW;
    }

    /**
     * This method returns the current status of this client-local handle for a remote Spark context. This status may be
     * completely out of sync with the actual status of the remote context. Note that due to concurrent access from
     * multiple threads, the status may be different after invoking this method. In other words: It almost never makes
     * sense to use this method.
     *
     * @return the current status of this context.
     */
    public final synchronized SparkContextStatus getStatus() {
        return m_status;
    }

    /**
     * @return the configuration of this Spark context
     */
    public final synchronized T getConfiguration() {
        return m_config;
    }

    /**
     * Ensures that the Spark context is open, creating a context if necessary and createRemoteContext is true.
     *
     * @param createRemoteContext If true, a non-existent Spark context will be created. Otherwise, a non-existent Spark
     *            context leads to a {@link KNIMESparkException}.
     * @param exec an {@link ExecutionMonitor} to track the progress of opening the context.
     * @return true when a new context was created, false, if a a context with the same name already existed in the
     *         cluster.
     *
     * @throws SparkContextNotFoundException if Spark context was non-existent and createRemoteContext=false.
     * @throws CanceledExecutionException if the execution was canceled by the user.
     * @throws KNIMESparkException if something went wrong while creating the Spark context, or if context was not in
     *             state configured.
     *
     */
    public final synchronized boolean ensureOpened(final boolean createRemoteContext, final ExecutionMonitor exec)
            throws SparkContextNotFoundException, CanceledExecutionException, KNIMESparkException {
            switch (getStatus()) {
                case NEW:
                    throw new KNIMESparkException("Spark context needs to be configured before opening.");
                case CONFIGURED:
                    return open(createRemoteContext, exec);
                default: // this is actually OPEN
                    // all is good, but we did not actually create a new context
                    exec.setProgress(1.0);
                    return false;
            }
        }

    /**
     * Ensures that no matching remote Spark context exists, destroying it if necessary.
     *
     * @return true if a remote Spark context was actually destroyed, false if no remote Spark context existed.
     * @throws KNIMESparkException Thrown if anything went wrong while destroying an existing remote Spark context.
     */
    public final synchronized boolean ensureDestroyed() throws KNIMESparkException {

        boolean contextWasDestroyed = false;

        switch (getStatus()) {
            case NEW:
                // cannot do anything useful without a configuration
                throw new RuntimeException(
                    String.format("Cannot destroy unconfigured Spark context. This is a bug.", getStatus()));
            case CONFIGURED:
            case OPEN:
                destroy();
                contextWasDestroyed = true;
                break;
        }

        return contextWasDestroyed;
    }

    /**
     * Ensures that this context is configured, and returns whether the given configuration was applied or not. When
     * this method returns true, it can be assumed that the given configuration was applied and the context is in state
     * {@link SparkContextStatus#CONFIGURED} or {@link SparkContextStatus#OPEN}. If the method returns false, nothing as
     * changed and the context is still in its previous state.
     *
     * <p>
     * NOTE: It is never possible to change the {@link SparkContextID} of a context. In this case this method does
     * nothing and returns false.
     * </p>
     *
     * <p>
     * <ul>
     * <li>If the current status ({@link #getStatus()}) is {@link SparkContextStatus#NEW}, the given configuration will
     * be applied and the context will be {@link SparkContextStatus#CONFIGURED}.</li>
     * <li>If the current status ({@link #getStatus()}) is {@link SparkContextStatus#CONFIGURED}, the given
     * configuration will only be applied if overwriteExistingConfig=true. Otherwise this method does nothing and
     * returns false. Either way, the context stays {@link SparkContextStatus#CONFIGURED}.</li>
     * <li>If the current status ({@link #getStatus()}) is {@link SparkContextStatus#OPEN}, then the given configuration
     * will only be applied if overwriteExistingConfig=true, otherwise this method does nothing and returns false.
     * Moreover, some configuration changes may require destroying a remote context in order to be applied. If this is
     * the case, this method only destroys the remote context if destroyIfNecessary=true, otherwise it does nothing and
     * returns false. If the configuration was applied the context will usually be in state
     * {@link SparkContextStatus#CONFIGURED}, however if the given configuration was identical to the current one, the
     * context remains {@link SparkContextStatus#OPEN}.</li>
     * </p>
     *
     * @param newConfig The new configuration to apply.
     * @param overwriteExistingConfig Whether a pre-existing context configuration should be overwritten.
     * @param destroyIfNecessary Whether an existing remote context shall be destroyed (if necessary required to apply
     *            the new configuration).
     * @return true if the given configuration was successfully applied, false otherwise.
     * @throws KNIMESparkException If something went wrong while destroying the context (only happens if
     *             destroyIfNecessary=true).
     */
    public final synchronized boolean ensureConfigured(final T newConfig,
        final boolean overwriteExistingConfig, final boolean destroyIfNecessary) throws KNIMESparkException {

        // we can never change the id of an existing context
        if (!newConfig.getSparkContextID().equals(getID())) {
            return false;
        }

        boolean toReturn = false;
        boolean doApply = false;

        switch (getStatus()) {
            case NEW:
                doApply = toReturn = true;
                break;
            case CONFIGURED:
                doApply = toReturn = newConfig.equals(m_config) || overwriteExistingConfig;
                break;
            default: // OPEN
                final boolean canReconfigureWithoutDestroy = canReconfigureWithoutDestroy(newConfig);

                // make sure we only apply the config (and switch to CONFIGURED) if the new config is different from
                // the existing one
                doApply = !newConfig.equals(m_config) && overwriteExistingConfig
                    && (canReconfigureWithoutDestroy || destroyIfNecessary);
                toReturn = doApply || newConfig.equals(m_config);

                if (doApply && !canReconfigureWithoutDestroy) {
                    ensureDestroyed();
                }
                break;
        }

        if (doApply) {
            m_config = newConfig;
            m_jobJar = null;

            final SparkContextStatus newStatus;
            switch (getStatus()) {
                case NEW:
                    newStatus = SparkContextStatus.CONFIGURED;
                    break;
                default:
                    newStatus = getStatus();
                    break;
            }
            setStatus(newStatus);
        }

        return toReturn;
    }

    /**
     * Compares the given configuration the current one and determines whether it can be applied without destroying the
     * current remote Spark context.
     *
     * @param newConfig The new configuration to compare the current one against.
     * @return true, if the new configuration can be applied without destroying the remote Spark context, false
     *         otherwise.
     */
    protected boolean canReconfigureWithoutDestroy(final T newConfig) {
        if (m_config == null) {
            return true;
        }
        return m_config.getSparkVersion().equals(newConfig.getSparkVersion())
            && (m_config.useCustomSparkSettings() == newConfig.useCustomSparkSettings())
            && ((m_config.useCustomSparkSettings())
                ? Objects.equals(m_config.getCustomSparkSettings(), newConfig.getCustomSparkSettings()) : true);
    }

    /**
     * @return the {@link SparkContextID}
     */
    public final SparkContextID getID() {
        return m_contextID;
    }

    /**
     * Provides the job jar. This method can only be invoked on a context is {@link SparkContextStatus#CONFIGURED} or
     * {@link SparkContextStatus#OPEN}.
     *
     * @return the job jar that contains the Spark job class for the configured Spark version.
     *
     * @throws KNIMESparkException if no Spark jobs were found for the configured Spark version.
     */
    protected final JobJar getJobJar() throws KNIMESparkException {
        if (getStatus() == SparkContextStatus.NEW) {
            throw new RuntimeException("Cannot get job jar for unconfigured Spark context. This is a bug.");
        }

        if (m_jobJar == null || !m_jobJar.getJarFile().exists()) {
            m_jobJar = SparkJarRegistry.getJobJar(getConfiguration().getSparkVersion());
        }

        if (m_jobJar == null) {
            throw new KNIMESparkException(String.format("No Spark jobs for Spark version %s found.",
                getConfiguration().getSparkVersion().getLabel()));
        }

        return m_jobJar;
    }

    /**
     * Updates the status to the given status. Subclasses are encouraged to override this method to update their state
     * accordingly, but they must make sure to invoke the super method.
     *
     * @param newStatus the new status of the context
     * @throws KNIMESparkException
     */
    protected void setStatus(final SparkContextStatus newStatus) throws KNIMESparkException {
        if (newStatus != getStatus()) {
            LOGGER.info(String.format("Spark context %s changed status from %s to %s", getID().toString(),
                m_status.toString(), newStatus.toString()));
            m_status = newStatus;
        }
    }

    /**
     * This method equivalent to invoking {@link #ensureConfigured(SparkContextConfig, boolean, boolean)} with the
     * boolean destroyIfNecessary=false.
     *
     * @param newConfig The new configuration to apply.
     * @param overwriteExistingConfig Whether a pre-existing context config should be overwritten.
     * @return true the given config was successfully applied, false otherwise.
     */
    public final synchronized boolean ensureConfigured(final T newConfig,
        final boolean overwriteExistingConfig) {
        try {
            return ensureConfigured(newConfig, overwriteExistingConfig, false);
        } catch (KNIMESparkException e) {
            // should never happen because we are not actually destroying a pre-existing remote context
            return false;
        }
    }

    /**
     * @return the Spark version supported by this context.
     */
    public final synchronized SparkVersion getSparkVersion() {
        if (m_config == null) {
            throw new RuntimeException(String.format(
                "Trying to get Spark version of Spark context which is in status: %s. This is a bug.", getStatus()));
        }
        return m_config.getSparkVersion();
    }

    /**
     * This method is invoked to while trying to run Spark jobs. Subclasses must provide an implementation of the the
     * {@link JobController} interface. Implementations can assume that the context is {@link SparkContextStatus#OPEN}
     * when this method is invoked.
     *
     * @return the {@link JobController} to use to run a Spark job.
     */
    protected abstract JobController getJobController();

    /**
     * This method is invoked when Spark nodes manage their named objects in Spark. Subclasses must provide an
     * implementation of the the {@link NamedObjectsController} interface. Implementations can assume that the context
     * is {@link SparkContextStatus#OPEN} when this method is invoked.
     *
     * @return the {@link NamedObjectsController} to use manage named objects.
     */
    protected abstract NamedObjectsController getNamedObjectsController();

    /**
     * {@inheritDoc}
     */
    @Override
    public final <O extends JobOutput> O startJobAndWaitForResult(final JobWithFilesRun<?, O> fileJob,
        final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {

        try {
            final JobController jobController;
            synchronized (this) {
                // this does not open a new context, it only checks that one exists and throws
                // an exception otherwise
                ensureOpened(false, new ExecutionMonitor());
                jobController = getJobController();
            }

            return jobController.startJobAndWaitForResult(fileJob, exec);
        } catch (SparkContextNotFoundException e) {
            setStatus(SparkContextStatus.CONFIGURED);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final <O extends JobOutput> O startJobAndWaitForResult(final JobRun<?, O> job, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {

        try {
            final JobController jobController;
            synchronized (this) {
                // this does not open a new context, it only checks that one exists and throws
                // an exception otherwise
                ensureOpened(false, new ExecutionMonitor());
                jobController = getJobController();
            }

            return jobController.startJobAndWaitForResult(job, exec);
        } catch (SparkContextNotFoundException e) {
            setStatus(SparkContextStatus.CONFIGURED);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void startJobAndWaitForResult(final SimpleJobRun<?> job, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {
        try {
            final JobController jobController;
            synchronized (this) {
                // this does not open a new context, it only checks that one exists and throws
                // an exception otherwise
                ensureOpened(false, new ExecutionMonitor());
                jobController = getJobController();
            }

            jobController.startJobAndWaitForResult(job, exec);
        } catch (SparkContextNotFoundException e) {
            setStatus(SparkContextStatus.CONFIGURED);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Set<String> getNamedObjects() throws KNIMESparkException {
        try {
            final NamedObjectsController namedObjectsController;
            synchronized (this) {
                ensureOpened(false, new ExecutionMonitor());
                namedObjectsController = getNamedObjectsController();
            }

            return namedObjectsController.getNamedObjects();
        } catch (SparkContextNotFoundException e) {
            setStatus(SparkContextStatus.CONFIGURED);
            throw e;
        } catch (CanceledExecutionException e) {
            return null; // never happens
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void deleteNamedObjects(final Set<String> namedObjects) throws KNIMESparkException {
        try {
            final NamedObjectsController namedObjectsController;
            synchronized (this) {
                ensureOpened(false, new ExecutionMonitor());
                namedObjectsController = getNamedObjectsController();
            }

            namedObjectsController.deleteNamedObjects(namedObjects);
        } catch (SparkContextNotFoundException e) {
            setStatus(SparkContextStatus.CONFIGURED);
            throw e;
        } catch (CanceledExecutionException e) {
            // never happens
        }
    }

    @Override
    public final <NOS extends NamedObjectStatistics> NOS getNamedObjectStatistics(final String namedObject) {
        return getNamedObjectsController().getNamedObjectStatistics(namedObject);
    }

    /**
     * Opens the Spark context, creating a a new remote context if necessary and createRemoteContext is true.
     * Implementations can assume that the context is {@link SparkContextStatus#CONFIGURED} when this method is invoked
     * and that the method is invoked in a thread-safe manner.
     *
     * @param createRemoteContext If true, a non-existent Spark context will be created. Otherwise, a non-existent Spark
     *            context leads to a {@link SparkContextNotFoundException}. Setting this parameter to false is useful in
     *            situations where you want to sync the status (see {@link #getStatus()}) to (re)attach to an existing
     *            remote Spark context, but not necessarily create a new one.
     * @param exec an {@link ExecutionMonitor} to track the progress of opening the context.
     * @return true when a new context was created, false, if a a context with the same name already existed in the
     *         cluster.
     *
     * @throws KNIMESparkException Thrown if something went wrong while creating a Spark context, or if context was not
     *             in state configured.
     * @throws CanceledExecutionException
     * @throws SparkContextNotFoundException Thrown if Spark context was non-existent and createRemoteContext=false
     */
    protected abstract boolean open(final boolean createRemoteContext, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException, SparkContextNotFoundException;

    /**
     * Destroys the Spark context within the cluster and frees up all resources. Implementations can assume that the
     * context is {@link SparkContextStatus#CONFIGURED} or {@link SparkContextStatus#OPEN} when this method is invoked,
     * and that the method is invoked in a thread-safe manner.
     *
     * @throws SparkContextNotFoundException Thrown if no remote Spark context actually existed.
     * @throws KNIMESparkException Thrown if anything else went wrong while destroying the remote Spark context.
     */
    protected abstract void destroy() throws KNIMESparkException;

    /**
     * @return A HTML description of this context without HTML and BODY tags
     */
    public abstract String getHTMLDescription();

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized String toString() {
        return "SparkContext[" + m_contextID.toString() + "]";
    }
}
