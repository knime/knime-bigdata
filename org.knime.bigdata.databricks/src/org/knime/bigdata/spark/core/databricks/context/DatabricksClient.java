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
 *   Created on Aug 14, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.core.databricks.context;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.knime.bigdata.database.databricks.TableAccessControllException;
import org.knime.bigdata.databricks.rest.DatabricksRESTClient;
import org.knime.bigdata.databricks.rest.clusters.Cluster;
import org.knime.bigdata.databricks.rest.clusters.ClusterAPI;
import org.knime.bigdata.databricks.rest.clusters.ClusterInfo;
import org.knime.bigdata.databricks.rest.clusters.ClusterState;
import org.knime.bigdata.databricks.rest.commands.Command;
import org.knime.bigdata.databricks.rest.commands.CommandCancel;
import org.knime.bigdata.databricks.rest.commands.CommandExecute;
import org.knime.bigdata.databricks.rest.commands.CommandState;
import org.knime.bigdata.databricks.rest.commands.CommandsAPI;
import org.knime.bigdata.databricks.rest.contexts.Context;
import org.knime.bigdata.databricks.rest.contexts.ContextCreate;
import org.knime.bigdata.databricks.rest.contexts.ContextDestroy;
import org.knime.bigdata.databricks.rest.contexts.ContextState;
import org.knime.bigdata.databricks.rest.contexts.ContextsAPI;
import org.knime.bigdata.databricks.rest.libraries.ClusterLibraryStatus;
import org.knime.bigdata.databricks.rest.libraries.LibrariesAPI;
import org.knime.bigdata.databricks.rest.libraries.Library;
import org.knime.bigdata.databricks.rest.libraries.LibraryInstallStatus;
import org.knime.bigdata.databricks.rest.libraries.LibraryStatus;
import org.knime.bigdata.databricks.rest.libraries.UnInstallLibrary;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.exception.SparkClusterNotFoundException;
import org.knime.bigdata.spark.core.exception.SparkContextNotFoundException;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.ThreadLocalHTTPAuthenticator;

/**
 * High level client implementation for Databricks.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksClient {
    private static final NodeLogger LOG = NodeLogger.getLogger(DatabricksClient.class);

    private static final long START_CLUSTER_TIMEOUT = 300 * 1000L; // = 5 minutes
    private static final long INSTALL_JOB_JAR_TIMEOUT = 300 * 1000L; // = 5 minutes
    private static final String INIT_DATABRICKS_CONTEXT = "def dsc = %s.createContext(sc, \"%s\", %b)";
    private static final String RUN_ON_DATABRICKS_CONTEXT = "dsc.run(\"%s\")";
    private static final String CONTEXT_LANG = "scala";

    private final DatabricksSparkContextConfig m_config;
    private final ClusterAPI m_clusterAPI;
    private final LibrariesAPI m_libraryAPI;
    private final ContextsAPI m_contextsAPI;
    private final CommandsAPI m_commandsAPI;

    /**
     * Default constructor.
     * @param config context configuration to use
     * @throws UnsupportedEncodingException if user or password can not be encoded in UTF-8
     */
    DatabricksClient(final DatabricksSparkContextConfig config) throws UnsupportedEncodingException {
        m_config = config;

        if (config.useToken()) {
            m_clusterAPI = DatabricksRESTClient.create(config.getDatabricksUrl(), ClusterAPI.class,
                config.getAuthToken(), config.getReceiveTimeoutSeconds()*1000, config.getConnectionTimeoutSeconds()*1000);
            m_libraryAPI = DatabricksRESTClient.create(config.getDatabricksUrl(), LibrariesAPI.class,
                config.getAuthToken(), config.getReceiveTimeoutSeconds()*1000, config.getConnectionTimeoutSeconds()*1000);
            m_contextsAPI = DatabricksRESTClient.create(config.getDatabricksUrl(), ContextsAPI.class,
                config.getAuthToken(), config.getReceiveTimeoutSeconds()*1000, config.getConnectionTimeoutSeconds()*1000);
            m_commandsAPI = DatabricksRESTClient.create(config.getDatabricksUrl(), CommandsAPI.class,
                config.getAuthToken(), config.getReceiveTimeoutSeconds()*1000, config.getConnectionTimeoutSeconds()*1000);
        } else {
            m_clusterAPI = DatabricksRESTClient.create(config.getDatabricksUrl(), ClusterAPI.class, config.getUser(),
                config.getPassword(), config.getReceiveTimeoutSeconds()*1000, config.getConnectionTimeoutSeconds()*1000);
            m_libraryAPI = DatabricksRESTClient.create(config.getDatabricksUrl(), LibrariesAPI.class, config.getUser(),
                config.getPassword(), config.getReceiveTimeoutSeconds()*1000, config.getConnectionTimeoutSeconds()*1000);
            m_contextsAPI = DatabricksRESTClient.create(config.getDatabricksUrl(), ContextsAPI.class, config.getUser(),
                config.getPassword(), config.getReceiveTimeoutSeconds()*1000, config.getConnectionTimeoutSeconds()*1000);
            m_commandsAPI = DatabricksRESTClient.create(config.getDatabricksUrl(), CommandsAPI.class, config.getUser(),
                config.getPassword(), config.getReceiveTimeoutSeconds()*1000, config.getConnectionTimeoutSeconds()*1000);
        }
    }

    /**
     * Remove job jar(s) from a given cluster using the libraries API.
     * Note: A running cluster must be restarted after removing the libraries.
     *
     * @throws KNIMESparkException
     * @throws InterruptedException
     * @throws CanceledExecutionException
     */
    void removeJobJarFromCluster(final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
        final String clusterId = m_config.getClusterId();

        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            LibraryStatus lib = findJobJarOnCluster();
            if (lib != null && waitForLibraryUninstall(lib)) {
                LOG.debug("Uninstalling job jar " + lib.library.jar + " from cluster " + clusterId + " on Databricks");
                m_libraryAPI.uninstall(UnInstallLibrary.create(clusterId, Arrays.asList(lib.library)));

                // wait for uninstall! otherwise uninstall + cluster start overlap and the library stays in pending
                // state until next cluster restart...
                final long timeout = System.currentTimeMillis() + INSTALL_JOB_JAR_TIMEOUT;
                lib = findJobJarOnCluster();
                while (waitForLibraryUninstall(lib) && System.currentTimeMillis() < timeout) {
                    sleep(m_config.getJobCheckFrequencySeconds() * 1000l, exec);
                    lib = findJobJarOnCluster();
                }

                if (lib != null && lib.status != null) {
                    LOG.warn("Job jar uninstall failed, library in unknown state: " + lib.status);
                } else if (System.currentTimeMillis() > timeout) {
                    LOG.warn("Timeout waiting for job jar uninstall.");
                }
            }

        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CanceledExecutionException("Execution canceled after thread interruption.");
        } catch (final FileNotFoundException e) {
            throw new SparkClusterNotFoundException(clusterId);
        } catch (final IOException e) {
            throw new KNIMESparkException("Unable to uninstall old job jar: " + e.getMessage(), e);
        }
    }

    /**
     * Decides if we should wait for library uninstall.
     *
     * @param status library status to check
     * @return <code>true</code> if we should wait for uninstall
     */
    private static boolean waitForLibraryUninstall(final LibraryStatus status) {
        if (status == null || status.status == null) {
            return false;
        } else {
            switch (status.status) {
                case PENDING:
                case RESOLVING:
                case INSTALLING:
                case INSTALLED:
                    return true;

                case FAILED:
                case UNINSTALL_ON_RESTART:
                default:
                    return false;
            }
        }
    }

    /**
     * Install job jar(s) on a given cluster using the libraries API.
     *
     * @throws KNIMESparkException
     */
    void installJobJarOnCluster(final ExecutionMonitor exec, final String remoteJobJar) throws KNIMESparkException, CanceledExecutionException {
        final String clusterId = m_config.getClusterId();

        if (exec != null) {
            exec.setMessage("Installing job library on cluster " + m_config.getClusterId());
        }

        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            final UnInstallLibrary req = UnInstallLibrary.create(clusterId, Arrays.asList(Library.create(remoteJobJar)));
            m_libraryAPI.install(req);

            final long timeout = System.currentTimeMillis() + INSTALL_JOB_JAR_TIMEOUT;
            LibraryStatus lib = findJobJarOnCluster();
            while (waitForLibraryInstall(lib) && System.currentTimeMillis() < timeout) {
                sleep(m_config.getJobCheckFrequencySeconds() * 1000l, exec);
                lib = findJobJarOnCluster();
            }

            if (lib == null) {
                throw new KNIMESparkException("Unable to find job jar library on cluster " + clusterId);
            } else if (lib.status != LibraryInstallStatus.INSTALLED) {
                throw new KNIMESparkException("Job JAR library in unknown state: " + lib.status);
            } else if (System.currentTimeMillis() > timeout) {
                throw new KNIMESparkException(
                    "Timeout installing job jar on cluster (current state: " + lib.status + ")");
            } else {
                LOG.info("Job jar on cluster " + clusterId + " on Databricks is in " + lib.status + " state.");
            }

        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CanceledExecutionException("Execution cancled after thread interruption.");
        } catch (final FileNotFoundException e) {
            throw new SparkClusterNotFoundException(clusterId);
        } catch (final IOException e) {
            throw new KNIMESparkException("Unable to install job jar in Databricks cluster: " + e.getMessage(), e);
        }
    }

    /**
     * Decides if we should wait for library installation.
     *
     * @param status library status to check
     * @return <code>true</code> if we should wait for installation
     */
    private static boolean waitForLibraryInstall(final LibraryStatus status) {
        if (status == null || status.status == null) {
            return true;
        } else {
            switch (status.status) {
                case PENDING:
                case RESOLVING:
                case INSTALLING:
                    return true;

                case INSTALLED:
                case FAILED:
                case UNINSTALL_ON_RESTART:
                default:
                    return false;
            }
        }
    }

    /**
     * Searches for jar libraries ending with {@link DatabricksSparkContext.JOB_JAR_FILENAME}.
     *
     * @return library ending with job jar filename or <code>null</code>
     * @throws KNIMESparkException
     * @throws IOException
     */
    private LibraryStatus findJobJarOnCluster() throws KNIMESparkException, IOException {
        final String clusterId = m_config.getClusterId();

        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            final ClusterLibraryStatus clusterState = m_libraryAPI.getClusterStatus(clusterId);
            if (clusterState.library_statuses != null) {
                for (LibraryStatus lib : clusterState.library_statuses) {
                    if (lib.library.jar != null && lib.library.jar.endsWith(DatabricksSparkContext.JOB_JAR_FILENAME)) {
                        return lib;
                    }
                }
            }
            return null;

        } catch (final FileNotFoundException e) {
            throw new SparkClusterNotFoundException(clusterId);
        }
    }

    /**
     * Start cluster and wait until cluster is in state {@link ClusterState.RUNNING}.
     * Note: All job jars are removed from the cluster if it is in {@link ClusterState.TERMINATED} state.
     *
     * @param exec execution monitor to cancel this operation
     * @return current cluster state
     * @throws SparkClusterNotFoundException if cluster is missing
     * @throws KNIMESparkException on failures
     * @throws CanceledExecutionException if execution was canceled or thread was interrupted
     */
    ClusterState startOrConnectCluster(final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {

        final String clusterId = m_config.getClusterId();

        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            ClusterInfo clusterInfo = m_clusterAPI.getCluster(clusterId);

            switch(clusterInfo.state) {
                case PENDING:
                case RUNNING:
                case RESTARTING:
                case RESIZING:
                    // nothing to do
                    break;

                case TERMINATED:
                    // remove a possible old job jar from the cluster to avoid an additional cluster restart
                    removeJobJarFromCluster(exec);
                    LOG.info("Starting cluster " + clusterId + " on Databricks.");
                    if (exec != null) {
                        exec.setMessage("Starting cluster");
                    }
                    m_clusterAPI.start(Cluster.create(clusterId));
                    break;

                case ERROR:
                case TERMINATING:
                case UNKNOWN:
                    throw new KNIMESparkException("Cluster is in unknown state: " + clusterInfo.state);
            }

            // wait until started
            final long timeout = System.currentTimeMillis() + START_CLUSTER_TIMEOUT;
            while (clusterInfo.state != ClusterState.RUNNING && System.currentTimeMillis() < timeout) {
                sleep(m_config.getJobCheckFrequencySeconds() * 1000l, exec);
                clusterInfo = m_clusterAPI.getCluster(clusterId);
            }

            if (System.currentTimeMillis() > timeout) {
                throw new KNIMESparkException(
                    "Timeout waiting for cluster state running (current state: " + clusterInfo.state + ")");
            } else if (clusterInfo.state != ClusterState.RUNNING) {
                throw new KNIMESparkException("Cluster is in unknown state: " + clusterInfo.state);
            } else {
                LOG.info("Cluster " + clusterId + " on Databricks is in " + clusterInfo.state);
                return clusterInfo.state;
            }

        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CanceledExecutionException("Execution cancled after thread interruption.");
        } catch (final FileNotFoundException e) {
            throw new SparkClusterNotFoundException(clusterId);
        } catch (final IOException e) {
            throw new KNIMESparkException("Unable to start cluster: " + e.getMessage(), e);
        }
    }

    /**
     * Try to detect table access control mode: Check if cluster configuration contains allowedLanguages setting without
     * scala.
     *
     * @throws SparkClusterNotFoundException
     */
    private boolean checkTableAccessControlSetting() throws SparkClusterNotFoundException {
        final String clusterId = m_config.getClusterId();

        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            final ClusterInfo clusterInfo = m_clusterAPI.getCluster(clusterId);
            if (clusterInfo.spark_conf != null) {
                return clusterInfo.spark_conf.entrySet()
                    .stream()
                    .anyMatch(e ->
                        (e.getKey().equalsIgnoreCase("spark.databricks.repl.allowedLanguages")
                            && !e.getValue().toLowerCase().contains("scala"))
                        || (e.getKey().equalsIgnoreCase("spark.databricks.acl.sqlOnly")
                            && e.getValue().equalsIgnoreCase("true")));
            }
        } catch (final FileNotFoundException e) {
            throw new SparkClusterNotFoundException(clusterId);
        } catch (final IOException e) {
            LOG.warn("Unable to fetch cluster configuration: " + e.getMessage(), e);
        }

        return false;
    }

    /**
     * Check if cluster is in {@link ClusterState#RUNNING} state.
     *
     * @return {@code true} if cluster is in {@link ClusterState#RUNNING} state.
     * @throws KNIMESparkException on connection failures
     */
    boolean isClusterRunning() throws KNIMESparkException {
        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            return m_clusterAPI.getCluster(m_config.getClusterId()).state == ClusterState.RUNNING;
        } catch (IOException e) {
            throw new KNIMESparkException("Unable to fetch cluster state: " + e.getMessage(), e);
        }
    }

    /**
     * Terminates the remote cluster.
     *
     * @throws SparkClusterNotFoundException if cluster is missing
     * @throws KNIMESparkException on failures
     */
    void terminateCluster() throws KNIMESparkException {
        final String clusterId = m_config.getClusterId();

        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            LOG.info("Terminating cluster " + clusterId + " on Databricks");
            m_clusterAPI.delete(Cluster.create(clusterId));
        } catch (final FileNotFoundException e) {
            throw new SparkClusterNotFoundException(clusterId);
        } catch (final IOException e) {
            throw new KNIMESparkException("Unable to stop cluster: " + e.getMessage(), e);
        }
    }

    /**
     * Create a new execution context on cluster and initialize databricks spark context.
     *
     * @param jobClassName class name of databricks spark job
     * @param stagingArea URI or path of staging area
     * @param stagingAreaIsPath <code>true</code> if staging area is path in default Hadoop FS
     * @param exec execution monitor to check
     * @return identifier of new context
     * @throws KNIMESparkException
     * @throws CanceledExecutionException
     */
    String createContext(final String jobClassName, final String stagingArea, final boolean stagingAreaIsPath,
        final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {

        if (exec != null) {
            exec.setMessage("Starting context on cluster");
        }

        if (checkTableAccessControlSetting()) {
            LOG.warn("Table Access Control detected, see the advanced tab in the configuration dialog to disable the spark context.");
        }

        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            // create context and wait until in running state
            final long start = System.currentTimeMillis();
            final long timeout = start + START_CLUSTER_TIMEOUT;
            Context context = m_contextsAPI.create(ContextCreate.create(m_config.getClusterId(), CONTEXT_LANG));
            sleep(100, exec); // wait at least 100ms to avoid invalid responses from Databricks REST API...
            context = m_contextsAPI.status(m_config.getClusterId(), context.id);
            while (context.status != ContextState.Running && System.currentTimeMillis() < timeout) {
                sleep(m_config.getJobCheckFrequencySeconds() * 1000l, exec);
                context = m_contextsAPI.status(m_config.getClusterId(), context.id);
             }

            if (System.currentTimeMillis() > timeout) {
                throw new KNIMESparkException(
                    "Timeout waiting for context state running (current state: " + context.status + ")");
            } else if (context.status != ContextState.Running) {
                throw new KNIMESparkException("Context is in unknown state: " + context.status);
            }

            // initialize job runner (databricks spark context)
            final String cmd = String.format(INIT_DATABRICKS_CONTEXT, jobClassName, stagingArea, stagingAreaIsPath);
            executeCommand(context.id, cmd, exec);

            LOG.info("Spark execution context " + context.id + " on Databricks cluster " + m_config.getClusterId()
                + " created in " + (System.currentTimeMillis() - start) + "ms.");

            return context.id;

        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CanceledExecutionException("Execution cancled after thread interruption.");
        } catch (final IOException e) {
            throw new KNIMESparkException("Unable to run job on cluster: " + e.getMessage(), e);
        }
    }

    /**
     * Destroy execution context on cluster.
     *
     * @param contextId context to destroy
     * @throws KNIMESparkException on failures
     */
    void destroyContext(final String contextId) throws KNIMESparkException {
        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            final Context context = m_contextsAPI.status(m_config.getClusterId(), contextId);
            if (context != null && context.status == ContextState.Running) {
                m_contextsAPI.destroy(ContextDestroy.create(m_config.getClusterId(), contextId));
                LOG.info("Spark execution context " + context.id + " on Databricks cluster " + m_config.getClusterId()
                    + " destroyed.");
            }
        } catch (final FileNotFoundException e) {
            // context already destroyed
        } catch (final IOException e) {
            throw new KNIMESparkException("Unable to destroy context on cluster: " + e.getMessage(), e);
        }
    }

    private void executeCommand(final String contextId, final String cmd, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {

        if (exec != null) {
            exec.setMessage("Running command on cluster");
        }

        Command command = null;
        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            LOG.debug("Running command '" + cmd + "' via execution context " + contextId + " on Databricks cluster "
                + m_config.getClusterId() + ".");
            final long start = System.currentTimeMillis();
            command = m_commandsAPI.execute(CommandExecute.create(m_config.getClusterId(), contextId, CONTEXT_LANG, cmd));
            sleep(100, exec); // wait 100ms before the command was started on cluster and status becomes available
            command = m_commandsAPI.status(m_config.getClusterId(), contextId, command.id);
            while(waitForCommand(command)) {
                sleep(m_config.getJobCheckFrequencySeconds() * 1000l, exec);
                command = m_commandsAPI.status(m_config.getClusterId(), contextId, command.id);
            }

            LOG.debug("Command '" + cmd + "' via execution context " + contextId + " on Databricks cluster "
                + m_config.getClusterId() + " finished after " + (System.currentTimeMillis() - start) + "ms with state "
                + (command == null ? null : command.status) + ".");

            if (command == null) {
                throw new KNIMESparkException("Command is in unknown state");
            } else if (command.status != CommandState.Finished) {
                throw new KNIMESparkException("Command is in unknown state: " + command.status);
            }

        } catch (final CanceledExecutionException e) {
            cancelCommand(contextId, command);
            throw e;
        } catch (final InterruptedException e) {
            cancelCommand(contextId, command);
            Thread.currentThread().interrupt();
            throw new CanceledExecutionException("Execution cancled after thread interruption.");
        } catch (final FileNotFoundException e) { // cluster or driver restart
            throw new SparkContextNotFoundException();
        } catch (final IOException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("Server error: UnauthorizedCommandException")) {
                throw new TableAccessControllException(e);
            } else {
                throw new KNIMESparkException("Unable to run command on cluster: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Try to cancel given command.
     *
     * @param contextId
     * @param command
     */
    private void cancelCommand(final String contextId, final Command command) {
        if (command != null && command.id != null) {
            try {
                LOG.debug("Cancel command " + command.id + " via execution context " + contextId + " on Databricks cluster "
                        + m_config.getClusterId() + ".");
                m_commandsAPI.cancel(CommandCancel.create(m_config.getClusterId(), contextId, command.id));

            } catch (IOException e) {
                LOG.warn("Unable to cancel command: " + command.id);
            }
        }
    }

    /**
     * Decides if we should wait for command that is in Queued or Running state.
     *
     * @param command command with current state
     * @return <code>true</code> if we should wait for the command
     */
    private static boolean waitForCommand(final Command command) {
        if (command != null && command.status != null) {
            switch (command.status) {
                case Queued:
                case Running:
                    return true;
                case Cancelled:
                case Cancelling:
                case Error:
                case Finished:
                default:
                    return false;
            }
        } else {
            return false;
        }
    }

    /**
     * Run a Spark job using an existing execution context.
     *
     * @param jobClassName class name of the job to run
     * @param jobJar URI of job jar
     * @param jobId unique identifier of the job
     * @param sparkSideStagingAreaURI URI to access the staging area on Spark side
     * @param exec execution monitor to cancel this operation
     * @throws SparkClusterNotFoundException if cluster is missing
     * @throws KNIMESparkException on failures
     * @throws CanceledExecutionException if execution was canceled or thread was interrupted
     */
    void run(final String contextId, final String jobId, final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
        final String cmd = String.format(RUN_ON_DATABRICKS_CONTEXT, jobId);
        executeCommand(contextId, cmd, exec);
    }

    /**
     * Sleep for given time of milliseconds and check execution monitor every 250ms for cancellation.
     *
     * @param timeToWait time to wait in milliseconds
     * @param exec execution monitor to check
     * @throws CanceledExecutionException
     * @throws InterruptedException
     */
    private static void sleep(final long timeToWait, final ExecutionMonitor exec) throws CanceledExecutionException, InterruptedException {
        final long end = System.currentTimeMillis() + timeToWait;
        while (System.currentTimeMillis() < end) {
            if (exec != null) {
                exec.checkCanceled();
            }
            Thread.sleep(250);
        }
    }
}
