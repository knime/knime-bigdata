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

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import com.knime.bigdata.spark.jobserver.client.KNIMEConfigContainer;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;

/**
 * Settings model that transfers Spark context information between the node model and its dialog.
 * @author Tobias Koetter, KNIME.com
 */
public class ContextSettings {

    private final SettingsModelString m_host = createHostModel();

    private final SettingsModelInteger m_port = createPortModel();

    private final SettingsModelString m_user = createUserModel();

    private final SettingsModelString m_password = createPasswordModel();

    private final SettingsModelString m_contextName = createIDModel();

    private final SettingsModelInteger m_noOfCores = createNoOfCoresModel();

    private final SettingsModelString m_memory = createMemoryModel();

    private final SettingsModel[] m_models = new SettingsModel[] {m_host, m_port, m_user, m_contextName, m_noOfCores, m_memory};
    /**
     * @return the context id model
     */
    static SettingsModelString createHostModel() {
        return new SettingsModelString("host", KNIMEConfigContainer.m_config.getString("spark.jobServer"));
    }

    /**
     * @return the cpu cores model
     */
    static SettingsModelInteger createPortModel() {
        return new SettingsModelIntegerBounded("port", KNIMEConfigContainer.m_config.getInt("spark.jobServerPort"),
            0, Integer.MAX_VALUE);
    }

    /**
     * @return the user model
     */
    static  SettingsModelString createUserModel() {
        return new SettingsModelString("user", KNIMEConfigContainer.m_config.getString("spark.userName"));
    }

    /**
     * @return the user model
     */
    static  SettingsModelString createPasswordModel() {
        return new SettingsModelString("password", KNIMEConfigContainer.m_config.getString("spark.password"));
    }

    /**
     * @return the context id model
     */
    static SettingsModelString createIDModel() {
        return new SettingsModelString("contextName", KNIMEConfigContainer.m_config.getString("spark.contextName"));
    }

    /**
     * @return the memory model
     */
    static  SettingsModelString createMemoryModel() {
        return new SettingsModelString("memPerNode", KNIMEConfigContainer.m_config.getString("spark.memPerNode"));
    }

    /**
     * @return the cpu cores model
     */
    static SettingsModelInteger createNoOfCoresModel() {
        return new SettingsModelIntegerBounded("numCPUCores", KNIMEConfigContainer.m_config.getInt("spark.numCPUCores"),
            0, Integer.MAX_VALUE);
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
     * @return the user
     */
    public String getUser() {
        return m_user.getStringValue();
    }

    /**
     * @return the password
     */
    public char[] getPassword() {
        return m_password.getStringValue().toCharArray();
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
     * @param settings the NodeSettingsWO to write to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        for (SettingsModel m : m_models) {
            m.saveSettingsTo(settings);
        }
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
    }

    /**
     * @return the KNIMESparkContext object with the specified settings
     */
    public KNIMESparkContext createContext() {
        return new KNIMESparkContext(getHost(), getPort(), getUser(), getPassword(), getContextName(), getNoOfCores(), getMemory());
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
        builder.append("]");
        return builder.toString();
    }
}
