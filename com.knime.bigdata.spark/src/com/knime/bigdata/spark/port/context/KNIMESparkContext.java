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
    private static final String CFG_USER = "user";
    private static final String CFG_PASSWORD = "password";
    private static final String CFG_ID = "id";
    private static final String CFG_CORES = "noOfCores";
    private static final String CFG_MEMORY = "memoryPerNode";

    private static final long serialVersionUID = 1L;

    private final String m_host;

    private final int m_port;

    private final String m_user;

    private final char[] m_pass;

    private final String m_contextName;

    private final int m_numCpuCores;

    private final String m_memPerNode;


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
            KNIMEConfigContainer.m_config.getInt("spark.jobServerPort"),
            KNIMEConfigContainer.m_config.getString("spark.userName"),
            KNIMEConfigContainer.m_config.getString("spark.password").toCharArray(),contextName, numCpuCores, memPerNode);
    }

    /**
     * @param host the job server host
     * @param port the job server port
     * @param user the name of the user
     * @param aPassphrase password
     * @param contextName the id of the Spark context
     * @param memPerNode the memory settings per node
     * @param numCpuCores the number of cpu cores per node
     */
    public KNIMESparkContext(final String host, final int port, final String user, final char[] aPassphrase, final String contextName,
        final int numCpuCores, final String memPerNode) {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("host must not be empty");
        }
        if (port < 0) {
            throw new IllegalArgumentException("Port must be positive");
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
        this(conf.getString(CFG_HOST), conf.getInt(CFG_PORT), conf.getString(CFG_USER), conf.getString(CFG_PASSWORD).toCharArray(), conf.getString(CFG_ID),
            conf.getInt(CFG_CORES), conf.getString(CFG_MEMORY));
    }

    /**
     * @param conf the {@link ConfigWO} to write to
     */
    public void save(final ConfigWO conf) {
        conf.addString(CFG_HOST, m_host);
        conf.addInt(CFG_PORT, m_port);
        conf.addString(CFG_USER, m_user);
        conf.addString(CFG_PASSWORD, String.valueOf(m_pass));

        conf.addString(CFG_ID, m_contextName);
        conf.addInt(CFG_CORES, m_numCpuCores);
        conf.addString(CFG_MEMORY, m_memPerNode);
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
     * @return the user
     */
    public String getUser() {
        return m_user;
    }

    /**
     * @return the user
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