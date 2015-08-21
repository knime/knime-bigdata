/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on 26.06.2015 by koetter
 */
package com.knime.bigdata.spark.port.context;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;

import com.knime.bigdata.spark.jobserver.client.KNIMEConfigContainer;

/**
 * Class that holds all information about a SparkContext that is used in KNIME e.g. the id
 * of the context and the requested resources.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class KNIMESparkContext implements Serializable {

    private static final String CFG_HOST = "host";
    private static final String CFG_PORT = "port";
    private static final String CFG_PROTOCOL = "protocol";
    private static final String CFG_USER = "user";
    private static final String CFG_PASSWORD = "password";
    private static final String CFG_ID = "id";
    private static final String CFG_CORES = "noOfCores";
    private static final String CFG_MEMORY = "memoryPerNode";

    private static final char[] MY =
            "3}acc80479[7b@05be9378K}168335832P§9276b76@2eb9$a\\23-c0a397a%ee'e35!89afFfA64#8bB8GRl".toCharArray();

    private static final long serialVersionUID = 1L;

    private final String m_host;

    private final int m_port;

    private final String m_user;

    private final char[] m_pass;

    private final String m_contextName;

    private final int m_numCpuCores;

    private final String m_memPerNode;

    private final String m_protocol;


    /**
     * create spark context container with default values
     */
    public KNIMESparkContext() {
        this(KNIMEConfigContainer.CONTEXT_NAME, KNIMEConfigContainer.m_config.getInt("spark.numCPUCores"),
            KNIMEConfigContainer.m_config.getString("spark.memPerNode"));
    }

    /**
     * @param contextName
     * @param numCpuCores
     * @param memPerNode
     */
    public KNIMESparkContext(final String contextName, final int numCpuCores, final String memPerNode) {
        this(KNIMEConfigContainer.m_config.getString("spark.jobServer"),
            KNIMEConfigContainer.m_config.getString("spark.jobServerProtocol"),
            KNIMEConfigContainer.m_config.getInt("spark.jobServerPort"),
            KNIMEConfigContainer.m_config.getString("spark.userName"),
            KNIMEConfigContainer.m_config.hasPath("spark.password") ?
                KNIMEConfigContainer.m_config.getString("spark.password").toCharArray() : null, contextName,
                numCpuCores, memPerNode);
    }

    /**
     * @param host the job server host
     * @param protocol the connection protocol to use
     * @param port the job server port
     * @param user the name of the user
     * @param aPassphrase password
     * @param contextName the id of the Spark context
     * @param memPerNode the memory settings per node
     * @param numCpuCores the number of cpu cores per node
     */
    public KNIMESparkContext(final String host, final String protocol, final int port, final String user,
        final char[] aPassphrase, final String contextName, final int numCpuCores, final String memPerNode) {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("host must not be empty");
        }
        if (port < 0) {
            throw new IllegalArgumentException("Port must be positive");
        }
        if (protocol == null || protocol.trim().isEmpty()) {
            throw new IllegalArgumentException("protocol must not be empty");
        }
        if (user == null || user.isEmpty()) {
            throw new IllegalArgumentException("user must not be empty");
        }
        if (contextName == null || contextName.isEmpty()) {
            throw new IllegalArgumentException("contextName must not be empty");
        }
        if (numCpuCores < 0) {
            throw new IllegalArgumentException("Number of cpu cores must be positive");
        }
        if (memPerNode == null || memPerNode.isEmpty()) {
            throw new IllegalArgumentException("memPerNode must not be empty");
        }
        m_host = host;
        m_protocol = protocol.trim();
        m_port = port;
        m_user = user;
        m_pass = aPassphrase;
        m_contextName = contextName;
        m_numCpuCores = numCpuCores;
        m_memPerNode = memPerNode;
    }

    /**
     * @param conf the {@link ConfigRO} to read from
     * @throws InvalidSettingsException
     */
    public KNIMESparkContext(final ConfigRO conf) throws InvalidSettingsException {
        this(conf.getString(CFG_HOST), conf.getString(CFG_PROTOCOL), conf.getInt(CFG_PORT), conf.getString(CFG_USER),
            demix(conf.getPassword(CFG_PASSWORD, String.valueOf(MY))), conf.getString(CFG_ID), conf.getInt(CFG_CORES),
            conf.getString(CFG_MEMORY));
    }

    /**
     * @param conf the {@link ConfigWO} to write to
     */
    public void save(final ConfigWO conf) {
        conf.addString(CFG_HOST, m_host);
        conf.addString(CFG_PROTOCOL, m_protocol);
        conf.addInt(CFG_PORT, m_port);
        conf.addString(CFG_USER, m_user);
        conf.addPassword(CFG_PASSWORD, String.valueOf(MY), mix(m_pass));
        conf.addString(CFG_ID, m_contextName);
        conf.addInt(CFG_CORES, m_numCpuCores);
        conf.addString(CFG_MEMORY, m_memPerNode);
    }

    /**
     * @param password
     * @return
     */
    private static char[] demix(final String p) {
        if (p == null) {
            return null;
        }
        final char[] cs = p.toCharArray();
        ArrayUtils.reverse(cs, 0, cs.length);
        return cs;
    }

    /**
     * @param password
     * @return
     */
    private static String mix(final char[] p) {
        if (p == null) {
            return null;
        }
        final char[] cs = Arrays.copyOf(p, p.length);
        ArrayUtils.reverse(cs, 0, cs.length);
        return String.valueOf(cs);
    }

    /**
     * @return the host of the job server
     */
    public String getHost() {
        return m_host;
    }

    /**
     * @return the port of the job server
     */
    public int getPort() {
        return m_port;
    }

    /**
     * @return the connection protocol to use e.g. http or https
     */
    public String getProtocol() {
        return m_protocol;
    }

    /**
     * @return the user
     */
    public String getUser() {
        return m_user;
    }

    /**
     * @return the password (might be <code>null</code>)
     */
    public char[] getPassword() {
        return m_pass;
    }

    /**
     * The id of the SparkContext.
     * @return the id of the SparkContext
     */
    public String getContextName() {
        return m_contextName;
    }

    /**
     * @return the numCpuCores
     */
    public int getNumCpuCores() {
        return m_numCpuCores;
    }

    /**
     * @return the memPerNode
     */
    public String getMemPerNode() {
        return m_memPerNode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + m_contextName.hashCode();
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final KNIMESparkContext other = (KNIMESparkContext)obj;
        if (!m_contextName.equals(other.m_contextName)) {
            return false;
        }
        return true;
    }

    /**
     * @param context {@link KNIMESparkContext} to check for compatibility
     * @return <code>true</code> if the contexts are compatible and can talk to each other
     */
    public boolean compatible(final KNIMESparkContext context) {
        return this.equals(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("KNIMESparkContext [m_host=");
        builder.append(m_host);
        builder.append(", m_port=");
        builder.append(m_port);
        builder.append(", m_user=");
        builder.append(m_user);
        builder.append(", m_contextName=");
        builder.append(m_contextName);
        builder.append(", m_numCpuCores=");
        builder.append(m_numCpuCores);
        builder.append(", m_memPerNode=");
        builder.append(m_memPerNode);
        builder.append("]");
        return builder.toString();
    }

    /**
     * @return the HTML description of this context without HTML and BODY tags
     */
    public String getHTMLDescription() {
        StringBuilder buf = new StringBuilder();
        buf.append("<strong>Job Server</strong><hr>");
        buf.append("<strong>Host:</strong>&nbsp;&nbsp;<tt>" + getHost() + "</tt><br>");
        buf.append("<strong>Protocol:</strong>&nbsp;&nbsp;<tt>" + getProtocol() + "</tt><br>");
        buf.append("<strong>Port:</strong>&nbsp;&nbsp;<tt>" + getPort() + "</tt><br>");
        buf.append("<strong>User:</strong>&nbsp;&nbsp;<tt>" + getUser() + "</tt><br><br>");
        buf.append("<strong>Context</strong><hr>");
        buf.append("<strong>ID:</strong>&nbsp;&nbsp;<tt>" + getContextName() + "</tt><br>");
        buf.append("<strong>Number of cores:</strong>&nbsp;&nbsp;<tt>");
        final int numCpuCores = getNumCpuCores();
        buf.append(numCpuCores < 0 ? "unknown" : numCpuCores);
        buf.append("</tt><br>");
        buf.append("<strong>Memory:</strong>&nbsp;&nbsp;<tt>" + getMemPerNode() + "</tt><br>");
        return buf.toString();
    }

}