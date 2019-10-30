package org.knime.bigdata.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.core.runtime.Platform;
import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.core.node.workflow.CredentialsStore;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.util.FileUtil;

/**
 * Reads flow variables from a CSV file. See {@link TestflowVariable} for definitions and descriptions.
 * 
 * Location of flowvariables.csv: If FLOWVARS ENV is not set, this janitor searches in
 * {@link #DEFAULT_PATH_WORKFLOW} or {@link #DEFAULT_PATH_WORKSPACE} for flowvariables.csv.
 * 
 * For the connector nodes that support credentials (HDFS/webHDFS/httpFS, Hive, Impala, Create Spark Context)
 * additional credentials variables will be automatically injected. The credentials are built from the
 * provided username/password flow variables.
 * 
 * Minimum node settings <-> variables:
 *   HDFS nodes:
 *     - useworkflowcredentials = hdfs.useCredentials
 *     - workflowcredentials = hdfs.credentialsName
 *     - user = hdfs.user (optional, backward compatibility)
 *     - host = hostname
 *     - authenticationmethod = hdfs.authMethod
 *
 *   Hive/Impala nodes:
 *     - kerberos = hive.useKerberos
 *     - credentials_name = hive.credentialsName
 *     - username = hive.username (optional, backward compatibility)
 *     - password = hive.password (optional, backward compatibility)
 *     - default-connection.hostname = hostname
 *     - default-connection.databaseName = hive.databasename
 *     - parameter.parameter = hive.parameter
 *
 *   SSH node:
 *     - useworkflowcredentials = ssh.useCredentials
 *     - workflowcredentials = ssh.credentialsName
 *     - user = ssh.username (optional, backward compatibility)
 *     - host = hostname
 *     - authenticationmethod = ssh.authMethod
 *
 *   Spark Create Context node:
 *     - spark.version
 *     - spark.contextIdScheme
 *     - spark.settingsOverride
 *     - spark.settingsCustom
 *     - spark.sjs.* or spark.local.* or spark.livy.*
 *
 * @author Sascha Wolke, KNIME GmbH
 * @author Bjoern Lohrmann, KNIME GmbH
 *
 */
public class FlowVariableReader {

    /** Default CSV file path if FLOWVARS is not set in ENV. */
    public final static String DEFAULT_PATH_WORKFLOW = "knime://knime.workflow/../flowvariables.csv";

    /** Default flowvariables.csv path in workspace. */
    public final static String DEFAULT_PATH_WORKSPACE = Platform.getLocation().append("/flowvariables.csv").toString();

    /**
     * Reads flow variable definitions from a CSV file and puts them into a map. It also performs some validation and
     * generates additional credentials flow variables for HDFS, Hive, Impala, SSH and Spark Jobserver connector nodes.
     * 
     * @return a map with flow variables to inject into the workflow.
     * @throws Exception
     */
    public static Map<String, FlowVariable> readFromCsv() throws Exception {
        final Map<String, FlowVariable> flowVariables = new HashMap<>();

        try (final BufferedReader reader = new BufferedReader(new FileReader(getInputFile()))) {

            // Read lines into string flow variables, ignore variables with
            // empty values.
            String line = reader.readLine(); // skip header
            while ((line = reader.readLine()) != null) {
                if (!StringUtils.isBlank(line) && !line.startsWith("#")) {
                    final String kv[] = line.split(",", 2);
                    
                    // validate the flow variable name
                    final TestflowVariable flowVar = TestflowVariable.fromName(kv[0]);
                    final String value = (kv.length == 1) ? "" : kv[1];
                    flowVar.validateValue(value);
                    
                    if (flowVar.isString() || flowVar.isBoolean()) {
                        flowVariables.put(kv[0], new FlowVariable(kv[0], value));
                    } else if (flowVar.isInt()) {
                        flowVariables.put(kv[0], new FlowVariable(kv[0], Integer.parseInt(value)));
                    }
                }
            }
        }
        
        ensureRequiredFlowVariables(flowVariables);

        if (flowVariables.containsKey(TestflowVariable.HOSTNAME.getName())) {
            addCredentialsFlowVariable("hdfs", TestflowVariable.isTrue(TestflowVariable.HDFS_USECREDENTIALS, flowVariables), false, flowVariables);
    
            addCredentialsFlowVariable("hive", !TestflowVariable.isTrue(TestflowVariable.HIVE_USE_KERBEROS, flowVariables), true, flowVariables);
    
            addCredentialsFlowVariable("impala", !TestflowVariable.isTrue(TestflowVariable.IMPALA_USE_KERBEROS, flowVariables), true,
                flowVariables);
    
            addCredentialsFlowVariable("ssh", true, true, flowVariables);
        }

        if (TestflowVariable.stringEquals(TestflowVariable.SPARK_CONTEXTIDSCHEME,
            SparkContextIDScheme.SPARK_LOCAL.toString(), flowVariables)) {

            // no special things to do for local spark

        } else if (TestflowVariable.stringEquals(TestflowVariable.SPARK_CONTEXTIDSCHEME,
            SparkContextIDScheme.SPARK_JOBSERVER.toString(), flowVariables)) {
            
            final boolean sparkUseCredentials =
                flowVariables.containsKey(TestflowVariable.SPARK_SJS_AUTHMETHOD.getName())
                    && flowVariables.get(TestflowVariable.SPARK_SJS_AUTHMETHOD.getName()).getStringValue()
                        .equalsIgnoreCase("CREDENTIALS");
            addCredentialsFlowVariable("spark.sjs", sparkUseCredentials, true, flowVariables);

        } else if (TestflowVariable.stringEquals(TestflowVariable.SPARK_CONTEXTIDSCHEME,
            SparkContextIDScheme.SPARK_LIVY.toString(), flowVariables)) {

            addSchemeHostAndPortVariable(TestflowVariable.SPARK_LIVY_URL, flowVariables);

        } else if (TestflowVariable.stringEquals(TestflowVariable.SPARK_CONTEXTIDSCHEME,
            SparkContextIDScheme.SPARK_DATABRICKS.toString(), flowVariables)) {

            addSchemeHostAndPortVariable(TestflowVariable.SPARK_DATABRICKS_URL, flowVariables);

            if (has(TestflowVariable.SPARK_DATABRICKS_TOKEN, flowVariables)) {
                addCredentialsTokenFlowVariable("spark.databricks", true, "token", flowVariables);
            } else {
                addCredentialsFlowVariable("spark.databricks", true, true, flowVariables);
            }

        } else if (has(TestflowVariable.SPARK_CONTEXTIDSCHEME, flowVariables)) {
            throw new IllegalArgumentException("Unknown spark scheme: "
                + flowVariables.get(TestflowVariable.SPARK_CONTEXTIDSCHEME.getName()).getStringValue());
        }

        return flowVariables;
    }
    
    private static void ensureRequiredFlowVariables(Map<String, FlowVariable> flowVariables) throws IllegalArgumentException {
        // if the "global" hostname is set, then we can assume to be connecting to something that is not local big data environment
        if (has(TestflowVariable.HOSTNAME, flowVariables)) {
            // HDFS is mandatory when the 
            ensureHas(TestflowVariable.HDFS_AUTH_METHOD, flowVariables);
            ensureHas(TestflowVariable.HDFS_URL, flowVariables);
            ensureHas(TestflowVariable.HDFS_WEBHDFS_PORT, flowVariables);
            ensureHas(TestflowVariable.HDFS_USECREDENTIALS, flowVariables);
            if (TestflowVariable.isTrue(TestflowVariable.HDFS_USECREDENTIALS, flowVariables)) {
                ensureHas(TestflowVariable.HDFS_USERNAME, flowVariables);
            }
            
            // Hive is mandatory
            ensureHas(TestflowVariable.HIVE_DATABASENAME, flowVariables);
            ensureHas(TestflowVariable.HIVE_PARAMETER, flowVariables);
            ensureHas(TestflowVariable.HIVE_PORT, flowVariables);
            ensureHas(TestflowVariable.HIVE_USE_KERBEROS, flowVariables);
            if (!TestflowVariable.isTrue(TestflowVariable.HIVE_USE_KERBEROS, flowVariables)) {
                ensureHas(TestflowVariable.HIVE_USERNAME, flowVariables);
                ensureHas(TestflowVariable.HIVE_PASSWORD, flowVariables);
            }
            
            // SSH is mandatory
            ensureHas(TestflowVariable.SSH_PORT, flowVariables);
            ensureHas(TestflowVariable.SSH_USERNAME, flowVariables);
            ensureHas(TestflowVariable.SSH_PASSWORD, flowVariables);
            ensureHas(TestflowVariable.SSH_KEYFILE, flowVariables);
            
            // Impala is optional, but if anything for Impala is defined, everything must be defined.
            if (has(TestflowVariable.IMPALA_DATABASENAME, flowVariables)
                || has(TestflowVariable.IMPALA_PARAMETER, flowVariables)
                || has(TestflowVariable.IMPALA_USE_KERBEROS, flowVariables)
                || has(TestflowVariable.IMPALA_USERNAME, flowVariables)
                || has(TestflowVariable.IMPALA_PASSWORD, flowVariables)) {
                
                ensureHas(TestflowVariable.IMPALA_DATABASENAME, flowVariables);
                ensureHas(TestflowVariable.IMPALA_PARAMETER, flowVariables);
                ensureHas(TestflowVariable.IMPALA_USE_KERBEROS, flowVariables);
                
                if (!TestflowVariable.isTrue(TestflowVariable.IMPALA_USE_KERBEROS, flowVariables)) {
                    ensureHas(TestflowVariable.IMPALA_USERNAME, flowVariables);
                    ensureHas(TestflowVariable.IMPALA_PASSWORD, flowVariables);
                }
            }
        }
        
        // Spark is optional, but if Spark scheme is defined, everything must be defined.
        if (has(TestflowVariable.SPARK_CONTEXTIDSCHEME, flowVariables)) {
            ensureHas(TestflowVariable.SPARK_CONTEXTIDSCHEME, flowVariables);
        
            if (TestflowVariable.stringEquals(TestflowVariable.SPARK_CONTEXTIDSCHEME,
                SparkContextIDScheme.SPARK_LOCAL.toString(), flowVariables)) {

                ensureHas(TestflowVariable.SPARK_LOCAL_CONTEXTNAME, flowVariables);
                ensureHas(TestflowVariable.SPARK_LOCAL_THREADS, flowVariables);
                ensureHas(TestflowVariable.SPARK_LOCAL_SQLSUPPORT, flowVariables);
                if (TestflowVariable.stringEquals(TestflowVariable.SPARK_LOCAL_SQLSUPPORT, "HIVEQL_WITH_JDBC", flowVariables)) {
                    ensureHas(TestflowVariable.SPARK_LOCAL_THRIFTSERVERPORT, flowVariables);
                }
                ensureHas(TestflowVariable.SPARK_LOCAL_USEHIVEDATAFOLDER, flowVariables);
                if (TestflowVariable.isTrue(TestflowVariable.SPARK_LOCAL_USEHIVEDATAFOLDER, flowVariables)) {
                    ensureHas(TestflowVariable.SPARK_LOCAL_HIVEDATAFOLDER, flowVariables);
                }
                ensureHas(TestflowVariable.SPARK_SETTINGSOVERRIDE, flowVariables);
                ensureHas(TestflowVariable.SPARK_SETTINGSCUSTOM, flowVariables);
            } else if (TestflowVariable.stringEquals(TestflowVariable.SPARK_CONTEXTIDSCHEME,
                SparkContextIDScheme.SPARK_JOBSERVER.toString(), flowVariables)) {
                
                ensureHas(TestflowVariable.SPARK_VERSION, flowVariables);
                ensureHas(TestflowVariable.SPARK_SJS_URL, flowVariables);
                ensureHas(TestflowVariable.SPARK_SJS_CONTEXTNAME, flowVariables);
                ensureHas(TestflowVariable.SPARK_SJS_AUTHMETHOD, flowVariables);
                if (TestflowVariable.stringEqualsIgnoreCase(TestflowVariable.SPARK_SJS_AUTHMETHOD, "CREDENTIALS", flowVariables)) {
                    ensureHas(TestflowVariable.SPARK_SJS_USERNAME, flowVariables);
                    ensureHas(TestflowVariable.SPARK_SJS_PASSWORD, flowVariables);
                    ensureHas(TestflowVariable.SPARK_SJS_RECEIVETIMEOUT, flowVariables);
                }
                ensureHas(TestflowVariable.SPARK_SETTINGSOVERRIDE, flowVariables);
                ensureHas(TestflowVariable.SPARK_SETTINGSCUSTOM, flowVariables);
            } else if (TestflowVariable.stringEquals(TestflowVariable.SPARK_CONTEXTIDSCHEME,
                SparkContextIDScheme.SPARK_LIVY.toString(), flowVariables)) {
                
                ensureHas(TestflowVariable.SPARK_VERSION, flowVariables);
                ensureHas(TestflowVariable.SPARK_LIVY_URL, flowVariables);
                ensureHas(TestflowVariable.SPARK_LIVY_AUTHMETHOD, flowVariables);
                ensureHas(TestflowVariable.SPARK_LIVY_SETSTAGINGAREAFOLDER, flowVariables);
                if (TestflowVariable.isTrue(TestflowVariable.SPARK_LIVY_SETSTAGINGAREAFOLDER, flowVariables)) {
                    ensureHas(TestflowVariable.SPARK_LIVY_STAGINGAREAFOLDER, flowVariables);
                }
                ensureHas(TestflowVariable.SPARK_LIVY_CONNECTTIMEOUT, flowVariables);
                ensureHas(TestflowVariable.SPARK_LIVY_RESPONSETIMEOUT, flowVariables);
                ensureHas(TestflowVariable.SPARK_SETTINGSOVERRIDE, flowVariables);
                ensureHas(TestflowVariable.SPARK_SETTINGSCUSTOM, flowVariables);
            } else if (TestflowVariable.stringEquals(TestflowVariable.SPARK_CONTEXTIDSCHEME,
                SparkContextIDScheme.SPARK_DATABRICKS.toString(), flowVariables)) {

                ensureHas(TestflowVariable.SPARK_VERSION, flowVariables);
                ensureHas(TestflowVariable.SPARK_DATABRICKS_URL, flowVariables);
                ensureHas(TestflowVariable.SPARK_DATABRICKS_CLUSTER_ID, flowVariables);
                ensureHas(TestflowVariable.SPARK_DATABRICKS_WORKSPACE_ID, flowVariables);
                final boolean hasToken = has(TestflowVariable.SPARK_DATABRICKS_TOKEN, flowVariables);
                final boolean hasUserAndPw = has(TestflowVariable.SPARK_DATABRICKS_USERNAME, flowVariables)
                    && has(TestflowVariable.SPARK_DATABRICKS_PASSWORD, flowVariables);
                if (!hasToken && !hasUserAndPw) {
                    throw new IllegalArgumentException("Invalid databricks authentication settings: token or username+password required.");
                }
                ensureHas(TestflowVariable.SPARK_DATABRICKS_SETSTAGINGAREAFOLDER, flowVariables);
                if (TestflowVariable.isTrue(TestflowVariable.SPARK_DATABRICKS_SETSTAGINGAREAFOLDER, flowVariables)) {
                    ensureHas(TestflowVariable.SPARK_DATABRICKS_STAGINGAREAFOLDER, flowVariables);
                }
                ensureHas(TestflowVariable.SPARK_DATABRICKS_CONNECTIONTIMEOUT, flowVariables);
                ensureHas(TestflowVariable.SPARK_DATABRICKS_RECEIVETIMEOUT, flowVariables);
            }
        }

    }
    
    private static void ensureHas(TestflowVariable flowVar, Map<String, FlowVariable> flowVariables) {
        if (!has(flowVar, flowVariables)) {
            throw new IllegalArgumentException("Missing flow variable definition in csv: " + flowVar.getName());
        }
    }
    
    private static boolean has(TestflowVariable flowVar, Map<String, FlowVariable> flowVariables) {
        return flowVariables.containsKey(flowVar.getName());
    }

    /**
     * Create a [prefix].credentials and [prefix].credentialsName flow variable with given username and password
     * variable.
     *
     * @param prefix hdfs/hive/impala/ssh/spark
     * @param realCredentials create real or dummy credentials (username might be empty in this case)
     * @param hasPassword Whether there is a password flow variable under the given prefix. If yes, this will be used to
     *            generate the credentials. Otherwise an empty password will be used.
     * @param flowVariables map with all flow variables
     * @throws Exception if username or password missing
     */
    private static void addCredentialsFlowVariable(final String prefix, final boolean realCredentials,
        final boolean hasPassword, final Map<String, FlowVariable> flowVariables) throws Exception {
        
        final String credsVar = prefix + ".credentials";
        final String credsNameVar = prefix + ".credentialsName";
        final String usernameVar = prefix + ".username";
        final String passwordVar = prefix + ".password";

        String username = "dummy";
        String password = "dummy";
        if (realCredentials) {
            if (!flowVariables.containsKey(usernameVar)) {
                throw new IllegalArgumentException("Missing flow variable definition in csv: " + usernameVar);
            }
            username = flowVariables.get(usernameVar).getStringValue();

            if (hasPassword) {
                if (!flowVariables.containsKey(passwordVar)) {
                    throw new IllegalArgumentException("Missing flow variable definition in csv: " + passwordVar);
                }
                password = flowVariables.get(passwordVar).getStringValue();
            } else {
                password = "";
            }

        }
        flowVariables.put(usernameVar, new FlowVariable(usernameVar, username));
        flowVariables.put(passwordVar, new FlowVariable(passwordVar, password));
        
        flowVariables.put(credsNameVar, new FlowVariable(credsNameVar, credsVar));
        flowVariables.put(credsVar,
            CredentialsStore.newCredentialsFlowVariable(credsVar, username, password, false, false));
    }

    /**
     * Create a [prefix].credentials and [prefix].credentialsName flow variable with given token variable.
     *
     * @param prefix databricks/...
     * @param realCredentials create real or dummy credentials (username might be empty in this case)
     * @param username username to use
     * @param flowVariables map with all flow variables
     * @throws Exception if username or password missing
     */
    private static void addCredentialsTokenFlowVariable(final String prefix, final boolean realCredentials,
        final String username, final Map<String, FlowVariable> flowVariables) throws Exception {

        final String credsVar = prefix + ".credentials";
        final String credsNameVar = prefix + ".credentialsName";
        final String tokenVar = prefix + ".token";

        String password = "dummy";
        if (realCredentials) {
            if (!flowVariables.containsKey(tokenVar)) {
                throw new IllegalArgumentException("Missing flow variable definition in csv for credentials: " + tokenVar);
            }
            password = flowVariables.get(tokenVar).getStringValue();
        }
        flowVariables.put(tokenVar, new FlowVariable(tokenVar, password));

        flowVariables.put(credsNameVar, new FlowVariable(credsNameVar, credsVar));
        flowVariables.put(credsVar,
            CredentialsStore.newCredentialsFlowVariable(credsVar, username, password, false, false));
    }

    /**
     * Create a [prefix].scheme/hostname/port flow variable from url variable.
     *
     * @param flowVar Variable contains an URL. Used as prefix in [prefix].scheme/hostname/port.
     * @param flowVariables map with all flow variables
     */
    private static void addSchemeHostAndPortVariable(final TestflowVariable flowVar, Map<String, FlowVariable> flowVariables) {
        try {
            final String prefix = flowVar.getName();
            final URI url = new URI(flowVariables.get(prefix).getStringValue());
            flowVariables.put(prefix + ".scheme", new FlowVariable(prefix + ".scheme", url.getScheme()));
            flowVariables.put(prefix + ".hostname", new FlowVariable(prefix + ".hostname", url.getHost()));
            flowVariables.put(prefix + ".port", new FlowVariable(prefix + ".port", url.getPort()));
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Unable to parse URL from " + flowVar.getName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Searches for flowvariables.csv in FLOWVARS ENV, {@link #DEFAULT_PATH_WORKFLOW} or
     * {@link #DEFAULT_PATH_WORKSPACE}.
     * 
     * @return File if found
     * @throws FileNotFoundException - If no location contains CSV file.
     * @throws MalformedURLException - If {@link #DEFAULT_PATH_WORKFLOW} has wrong format.
     */
    private static File getInputFile() throws FileNotFoundException, MalformedURLException {
        if (System.getenv("FLOWVARS") != null) {
            final File env = new File(System.getenv("FLOWVARS"));
            if (env.exists() && env.canRead()) {
                return env;
            }
        }

        final File workflow = FileUtil.getFileFromURL(new URL(DEFAULT_PATH_WORKFLOW));
        if (workflow.exists() && workflow.canRead()) {
            return workflow;
        }

        final File workspace = new File(DEFAULT_PATH_WORKSPACE);
        if (workspace.exists() && workspace.canRead()) {
            return workspace;
        }

        throw new FileNotFoundException("Unable to locate flowvariables.csv");
    }
}
