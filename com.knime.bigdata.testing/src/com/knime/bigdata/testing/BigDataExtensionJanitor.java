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
 * ---------------------------------------------------------------------
 *
 * History
 *   Nov 7, 2016 (Sascha Wolke, KNIME.com): created
 */
package com.knime.bigdata.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.CredentialsStore;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.util.FileUtil;
import org.knime.testing.core.TestrunJanitor;

/**
 * Provides big data flow variables based on a CSV file. CSV contains string key/value pairs and a header line.
 *
 * Additional, KRB5_CONFIG can be set in ENV and system property java.security.krb5.conf will be provided.
 *
 * Location of flowvariables.csv:
 *    If FLOWVARS ENV is not set, this janitor searches in {@link BigDataExtensionJanitor#DEFAULT_PATH_WORKFLOW} or
 *    {@link #DEFAULT_PATH_WORKSPACE} for flowvariables.csv.
 *
 *
 * Special values of entries in CSV file:
 *   hdfs.authMethod:
 *     - Password (use hdfs.user or credentials (see hdfs.useCredentials))
 *     - Kerberos
 *
 *   hdfs.useCredentials:
 *     - true (creates hdfs.credentials variable with hdfs.user and hdfs.password)
 *     - false (use username from hdfs.user, backward compatibility)
 *
 *   hive/impala.useKerberos:
 *     - true/false
 *
 *   ssh.authMethod:
 *     - Password
 *     - Keyfile
 *
 *   ssh.useCredentials:
 *     - true (creates ssh.credentials variable with ssh.username and ssh.password)
 *     - false (use username from ssh.username, backward compatibility)
 *
 *   ssh.port:
 *     - gets converted into integer flow variable
 *
 *   spark.authMethod:
 *     - NONE
 *     - CREDENTIALS (creates spark.credentials variable with spark.username and spark.password)
 *
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
 *     - v1_6.jobServerUrl = spark.jobserver.url
 *     - v1_6.authentication.credentials = spark.credentialsName
 *     - v1_6.authentication.selectType = spark.authMethod
 *     - v1_6.sparkVersion = spark.version
 *     - v1_6.spark.contextName = spark.context
 *
 * @author Sascha Wolke, KNIME.com
 */
public class BigDataExtensionJanitor extends TestrunJanitor {
    private final static NodeLogger LOGGER = NodeLogger.getLogger(BigDataExtensionJanitor.class);

    public final static String DEFAULT_VALUE = "execute-testflow-conf-node";

	/** Integer variables. */
    public final static String INT_VARIABLES[] = { "ssh.port" };

    /** Default CSV file path if FLOWVARS is not set in ENV. */
    public final String DEFAULT_PATH_WORKFLOW = "knime://knime.workflow/../flowvariables.csv";

    /** Default flowvariables.csv path in workspace. */
    public final String DEFAULT_PATH_WORKSPACE = Platform.getLocation().append("/flowvariables.csv").toString();

    private final List<FlowVariable> m_flowVariables;

    /** Default constructor. */
    public BigDataExtensionJanitor() {
        m_flowVariables = new ArrayList<>();

        m_flowVariables.add(new FlowVariable("hostname",DEFAULT_VALUE));

        m_flowVariables.add(new FlowVariable("hdfs.authMethod", "Password"));
        m_flowVariables.add(new FlowVariable("hdfs.useCredentials", "true"));
		m_flowVariables.add(new FlowVariable("hdfs.credentialsName", "hdfs.credentials"));
        m_flowVariables.add(new FlowVariable("hdfs.url", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("hdfs.user", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("hdfs.password", DEFAULT_VALUE));

        m_flowVariables.add(new FlowVariable("hive.databasename", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("hive.parameter", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("hive.useKerberos", "false"));
		m_flowVariables.add(new FlowVariable("hive.credentialsName", "hive.credentials"));
        m_flowVariables.add(new FlowVariable("hive.username", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("hive.password", DEFAULT_VALUE));

        m_flowVariables.add(new FlowVariable("impala.databasename", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("impala.parameter", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("impala.useKerberos", "false"));
		m_flowVariables.add(new FlowVariable("impala.credentialsName", "impala.credentials"));
        m_flowVariables.add(new FlowVariable("impala.username", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("impala.password", DEFAULT_VALUE));

        m_flowVariables.add(new FlowVariable("ssh.authMethod", "Password"));
        m_flowVariables.add(new FlowVariable("ssh.useCredentials", "true"));
		m_flowVariables.add(new FlowVariable("ssh.credentialsName", "ssh.credentials"));
        m_flowVariables.add(new FlowVariable("ssh.username", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("ssh.password", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("ssh.keyfile", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("ssh.port", 22));

        m_flowVariables.add(new FlowVariable("spark.version", "1.6"));
        m_flowVariables.add(new FlowVariable("spark.context", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("spark.jobserver.url", "http://execute-testflow-conf-node:123/"));
        m_flowVariables.add(new FlowVariable("spark.settings.override", "false"));
        m_flowVariables.add(new FlowVariable("spark.settings.custom", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("spark.authMethod", "NONE"));
		m_flowVariables.add(new FlowVariable("spark.credentialsName", "spark.credentials"));
        m_flowVariables.add(new FlowVariable("spark.username", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("spark.password", DEFAULT_VALUE));

        m_flowVariables.add(new FlowVariable("ssh.username", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("ssh.password", DEFAULT_VALUE));
        m_flowVariables.add(new FlowVariable("ssh.port", 22));

        try {
	        m_flowVariables.add(CredentialsStore.newCredentialsFlowVariable("hdfs.credentials", DEFAULT_VALUE, DEFAULT_VALUE, false, false));
	        m_flowVariables.add(CredentialsStore.newCredentialsFlowVariable("hive.credentials", DEFAULT_VALUE, DEFAULT_VALUE, false, false));
	        m_flowVariables.add(CredentialsStore.newCredentialsFlowVariable("impala.credentials", DEFAULT_VALUE, DEFAULT_VALUE, false, false));
	        m_flowVariables.add(CredentialsStore.newCredentialsFlowVariable("ssh.credentials", DEFAULT_VALUE, DEFAULT_VALUE, false, false));
	        m_flowVariables.add(CredentialsStore.newCredentialsFlowVariable("spark.credentials", DEFAULT_VALUE, DEFAULT_VALUE, false, false));
		} catch (final InvalidSettingsException e) {
			LOGGER.error("Failure creating default credeantials flow variables: " + e, e);
		}

        Collections.reverse(m_flowVariables);

		final String kerberosConfig = System.getenv("KRB5_CONFIG");
		if (kerberosConfig != null && !System.getProperties().contains("java.security.krb5.conf")) {
			LOGGER.info("Setting system property java.security.krb5.conf = " + kerberosConfig);
			System.setProperty("java.security.krb5.conf", kerberosConfig);
		}
    }

    @Override
    public List<FlowVariable> getFlowVariables() {
        return m_flowVariables;
    }

    /**
     * Searches for flowvariables.csv in FLOWVARS ENV, {@link #DEFAULT_PATH_WORKFLOW} or {@link #DEFAULT_PATH_WORKSPACE}.
     * @return File if found
     * @throws FileNotFoundException - If no location contains CSV file.
     * @throws MalformedURLException - If {@link #DEFAULT_PATH_WORKFLOW} has wrong format.
     */
    private File getInputFile() throws FileNotFoundException, MalformedURLException {
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

    @Override
    public void before() throws Exception {
        try (final BufferedReader reader = new BufferedReader(new FileReader(getInputFile()));) {
            final HashMap<String, FlowVariable> flowVariables = new HashMap<>();

			// Read lines into string flow variables, ignore variables with
			// empty values.
			String line = reader.readLine(); // skip header
			while ((line = reader.readLine()) != null) {
				if (!StringUtils.isBlank(line) && !line.startsWith("#")) {
					final String kv[] = line.split(",", 2);

					if (isIntVariable(kv[0])) {
						if (kv.length == 1 || StringUtils.isEmpty(kv[1])) {
							throw new RuntimeException("Integer value " + kv[0] + " can't be empty!");
						} else {
							flowVariables.put(kv[0], new FlowVariable(kv[0], Integer.parseInt(kv[1])));
						}

					} else {
						if (kv.length == 1 || StringUtils.isEmpty(kv[1])) {
							flowVariables.put(kv[0], new FlowVariable(kv[0], ""));
						} else {
							flowVariables.put(kv[0], new FlowVariable(kv[0], kv[1]));
						}
					}
				}
			}


			final boolean hdfsUseCredentials = flowVariables.containsKey("hdfs.useCredentials")
					&& flowVariables.get("hdfs.useCredentials").getStringValue().equalsIgnoreCase("true");
			addCredentialsFlowVariable("hdfs", "user", hdfsUseCredentials, flowVariables);

			final boolean hiveUseCredentials = flowVariables.containsKey("hive.useKerberos")
					&& flowVariables.get("hive.useKerberos").getStringValue().equalsIgnoreCase("false");
			addCredentialsFlowVariable("hive", "username", hiveUseCredentials, flowVariables);

			final boolean impalaUseCredentials = flowVariables.containsKey("impala.useKerberos")
					&& flowVariables.get("impala.useKerberos").getStringValue().equalsIgnoreCase("false");
			addCredentialsFlowVariable("impala", "username", impalaUseCredentials, flowVariables);

			final boolean sshUseCredentials = flowVariables.containsKey("ssh.useCredentials")
					&& flowVariables.get("ssh.useCredentials").getStringValue().equalsIgnoreCase("true");
			addCredentialsFlowVariable("ssh", "username", sshUseCredentials, flowVariables);

			final boolean sparkUseCredentials = flowVariables.containsKey("spark.authMethod")
					&& flowVariables.get("spark.authMethod").getStringValue().equalsIgnoreCase("CREDENTIALS");
			addCredentialsFlowVariable("spark", "username", sparkUseCredentials, flowVariables);


            m_flowVariables.clear();
            m_flowVariables.addAll(flowVariables.values());
            m_flowVariables.sort(new Comparator<FlowVariable>() {
				@Override
				public int compare(FlowVariable a, FlowVariable b) {
					return b.getName().compareTo(a.getName());
				}
			});
        }
    }

    /** @return true if given name is in {@link #INT_VARIABLES} */
    private boolean isIntVariable(final String name) {
		for (final String var : INT_VARIABLES) {
    		if (name.equalsIgnoreCase(var)) {
    			return true;
    		}
    	}

    	return false;
    }

    /**
     * Create a [prefix].credentials and [prefix].credentialsName flow variable with given
     * username and password variable.
     *
     * @param prefix hdfs/hive/impala/ssh/spark
     * @param usernameVariableName user or username
     * @param realCredentials create real or dummy credentials (username might be empty in this case)
     * @param flowVariables map with all flow variables
     * @throws Exception if username or password missing
     */
	private void addCredentialsFlowVariable(final String prefix, final String usernameVariableName,
			final boolean realCredentials, final HashMap<String, FlowVariable> flowVariables) throws Exception {

		flowVariables.put(prefix + ".credentialsName",
				new FlowVariable(prefix + ".credentialsName", prefix + ".credentials"));

		if (!realCredentials) {
			flowVariables.put(prefix + "." + usernameVariableName,
					new FlowVariable(prefix + "." + usernameVariableName, "dummy"));
			flowVariables.put(prefix + ".password",
					new FlowVariable(prefix + ".password", "dummy"));
			flowVariables.put(prefix + ".credentials",
					CredentialsStore.newCredentialsFlowVariable(prefix + ".credentials", "dummy", "dummy", false, false));

		} else if (!flowVariables.containsKey(prefix + "." + usernameVariableName)
				|| StringUtils.isBlank(flowVariables.get(prefix + "." + usernameVariableName).getStringValue())) {
			throw new RuntimeException(
					"Can't create " + prefix + " credentials flow variable without " + prefix + "." + usernameVariableName + "!");

		} else if (!flowVariables.containsKey(prefix + ".password")) { // might be empty
			throw new RuntimeException(
					"Can't create " + prefix + " credentials flow variable without " + prefix + ".password!");

		} else {
			final String username = flowVariables.get(prefix + "." + usernameVariableName).getStringValue();
			final String password = flowVariables.get(prefix + ".password").getStringValue();
			flowVariables.put(prefix + ".credentials",
					CredentialsStore.newCredentialsFlowVariable(prefix + ".credentials", username, password, false, false));
		}
    }

    @Override
    public void after() throws Exception {
    	//nothing to do
    }

    @Override
    public String getName() {
        return "Big Data Extension flowvariables.csv ";
    }

    @Override
    public String getID() {
        return "com.knime.bigdata.testing.BigDataExtensionJanitor";
    }

    @Override
    public String getDescription() {
        return "Loads flowvariables.csv from path provided by environment FLOWVARS variable, relativ to current workflow or in root workspace directory.";
    }
}
