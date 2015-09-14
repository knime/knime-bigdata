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
 *   Created on 03.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.context;

import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.preferences.KNIMEConfigContainer;

/**
 * Settings model that transfers Spark context information between the node model and its dialog.
 * @author Tobias Koetter, KNIME.com
 */
public class ContextSettings {

    private final SettingsModelString m_protocol = createProtocolModel();

    private final SettingsModelString m_host = createHostModel();

    private final SettingsModelInteger m_port = createPortModel();

    private final SettingsModelString m_user = createUserModel();

    private final SettingsModelString m_pwd = createPasswordModel();

    private final SettingsModelString m_contextName = createIDModel();

    private final SettingsModelInteger m_noOfCores = createNoOfCoresModel();

    private final SettingsModelString m_memory = createMemoryModel();

    private final SettingsModelInteger m_jobTimeout = createJobTimeoutModel();

    private final SettingsModelInteger m_jobCheckFrequency = createJobCheckFrequencyModel();

    private final SettingsModelBoolean m_deleteRDDsOnDispose = createDeleteRDDsOnDisposeModel();

    private static final char[] MY =
            "3O}accA80479[7b@05b9378K}18358QLG32P§92JZW76b76@2eb9$a\\23-c0a397a%ee'e35!89afFfA64#8bB8GRl".toCharArray();

    private final SettingsModel[] m_models = new SettingsModel[] {m_protocol, m_host, m_port, m_user, m_contextName, m_noOfCores,
        m_memory, m_jobTimeout, m_jobCheckFrequency, m_deleteRDDsOnDispose};
    /**
     * @return the context id model
     */
    static SettingsModelString createHostModel() {
        return new SettingsModelString("host", KNIMEConfigContainer.getJobServer());
    }

    /**
     * @return the cpu cores model
     */
    static SettingsModelInteger createPortModel() {
        return new SettingsModelIntegerBounded("port", KNIMEConfigContainer.getJobServerPort(),
            0, Integer.MAX_VALUE);
    }

    /**
     * @return the user model
     */
    static  SettingsModelString createUserModel() {
        return new SettingsModelString("user", KNIMEConfigContainer.getUserName());
    }

    /**
     * @return the user model
     */
    static  SettingsModelString createPasswordModel() {
        final char[] pwd = KNIMEConfigContainer.getPassword();
        return new SettingsModelString("password", pwd == null ? null : pwd.toString());
    }

    /**
     * @return the context id model
     */
    static SettingsModelString createIDModel() {
        return new SettingsModelString("contextName", KNIMEConfigContainer.getSparkContext());
    }

    /**
     * @return the protocol model
     */
    static  SettingsModelString createProtocolModel() {
        return new SettingsModelString("protocol", KNIMEConfigContainer.getMemoryPerNode());
    }

    /**
     * @return the memory model
     */
    static  SettingsModelString createMemoryModel() {
        return new SettingsModelString("memPerNode", KNIMEConfigContainer.getMemoryPerNode());
    }

    /**
     * @return the cpu cores model
     */
    static SettingsModelInteger createNoOfCoresModel() {
        return new SettingsModelIntegerBounded("numCPUCores", KNIMEConfigContainer.getNumOfCPUCores(),
            1, Integer.MAX_VALUE);
    }

    /**
     * @return the job check frequency model
     */
    static SettingsModelInteger createJobTimeoutModel() {
        return new SettingsModelIntegerBounded("sparkJobTimeout",
            KNIMEConfigContainer.getJobTimeout(), 1, Integer.MAX_VALUE);
    }

    /**
     * @return the job check frequency model
     */
    static SettingsModelInteger createJobCheckFrequencyModel() {
        return new SettingsModelIntegerBounded("sparkJobCheckFrequency",
            KNIMEConfigContainer.getJobCheckFrequency(), 1, Integer.MAX_VALUE);
    }

    static SettingsModelBoolean createDeleteRDDsOnDisposeModel() {
        return new SettingsModelBoolean("deleteRDDsOnDispose", KNIMEConfigContainer.deleteRDDsOnDispose());
    }

    /**
     * @return the host
     */
    public String getHost() {
        return m_host.getStringValue();
    }

    /**
     * @return the port
     */
    public int getPort() {
        return m_port.getIntValue();
    }

    /**
     * @return the protocol to use to connect to the server
     */
    public String getProtocol() {
        return "https";
    }

    /**
     * @return the user
     */
    public String getUser() {
        return m_user.getStringValue();
    }

    /**
     * @return the password
     */
    public char[] getPassword() {
        final String pwd = m_pwd.getStringValue();
        return pwd == null ? null : pwd.toCharArray();
    }

    /**
     * @return the id
     */
    public String getContextName() {
        return m_contextName.getStringValue();
    }

    /**
     * @return the noOfCores
     */
    public int getNoOfCores() {
        return m_noOfCores.getIntValue();
    }

    /**
     * @return the memory
     */
    public String getMemory() {
        return m_memory.getStringValue();
    }

    /**
     * @return the Spark job timeout in seconds
     */
    public int getJobTimeout() {
        return m_jobTimeout.getIntValue();
    }

    /**
     * @return the Spark job timeout check frequency in seconds
     */
    public int getJobCheckFrequency() {
        return m_jobCheckFrequency.getIntValue();
    }

    /**
     * @return the deleteRDDsOnDispose
     */
    public boolean getDeleteRDDsOnDispose() {
        return m_deleteRDDsOnDispose.getBooleanValue();
    }


    /**
     * @param password
     * @return
     */
    private static String demix(final String p) {
        if (p == null) {
            return null;
        }
        final char[] cs = p.toCharArray();
        ArrayUtils.reverse(cs, 0, cs.length);
        return cs.toString();
    }

    /**
     * @param password
     * @return
     */
    private static String mix(final String p) {
        if (p == null) {
            return null;
        }
        final char[] c = p.toCharArray();
        final char[] cs = Arrays.copyOf(c, c.length);
        ArrayUtils.reverse(cs, 0, cs.length);
        return String.valueOf(cs);
    }

    /**
     * @param settings the NodeSettingsWO to write to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        for (SettingsModel m : m_models) {
            m.saveSettingsTo(settings);
        }
        settings.addPassword(m_pwd.getKey(), String.valueOf(MY), mix(m_pwd.getStringValue()));
    }

    /**
     * @param settings the NodeSettingsRO to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        for (SettingsModel m : m_models) {
            m.validateSettings(settings);
        }
    }

    /**
     * @param settings the NodeSettingsRO to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        for (SettingsModel m : m_models) {
            m.loadSettingsFrom(settings);
        }
        m_pwd.setStringValue(demix(settings.getPassword(m_pwd.getKey(), String.valueOf(MY), null)));
    }

    /**
     * @return the KNIMESparkContext object with the specified settings
     */
    public KNIMESparkContext createContext() {
        return new KNIMESparkContext(getHost(), getProtocol(), getPort(), getUser(), getPassword(), getContextName(),
            getNoOfCores(), getMemory(), getJobCheckFrequency(), getJobTimeout(), getDeleteRDDsOnDispose());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("ContextSettings [host=");
        builder.append(getHost());
        builder.append(", port=");
        builder.append(getPort());
        builder.append(", user=");
        builder.append(getUser());
        builder.append(", contextName=");
        builder.append(getContextName());
        builder.append(", noOfCores=");
        builder.append(getNoOfCores());
        builder.append(", memory=");
        builder.append(getMemory());
        builder.append(", jobTimeout=");
        builder.append(getJobTimeout());
        builder.append(", jobCheckFrequency=");
        builder.append(getJobCheckFrequency());
        builder.append(", deleteRDDsOnDispose=");
        builder.append(getDeleteRDDsOnDispose());
        builder.append("]");
        return builder.toString();
    }
}
