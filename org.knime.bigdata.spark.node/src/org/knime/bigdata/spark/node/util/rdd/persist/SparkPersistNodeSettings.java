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
 *   Created on Nov 8, 2018 by bjoern
 */
package org.knime.bigdata.spark.node.util.rdd.persist;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class SparkPersistNodeSettings {

    private final SettingsModelString m_storageLevel = createStorageLevelModel();

    private final SettingsModelBoolean m_useDisk = createUseDiskModel();

    private final SettingsModelBoolean m_useMemory = createUseMemoryModel();

    private final SettingsModelBoolean m_useOffHeap = createUseOffHeapModel();

    private final SettingsModelBoolean m_deserialized = createDeserializedModel();

    private final SettingsModelInteger m_replication = createReplicationModel();

    public SparkPersistNodeSettings() {
        m_storageLevel.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                updateEnabledness();
            }
        });
    }

    /**
     * @return the storageLevel
     */
    public SettingsModelString getStorageLevelModel() {
        return m_storageLevel;
    }

    public String getStorageLevel() {
        return m_storageLevel.getStringValue();
    }

    /**
     * @return the useDisk
     */
    public SettingsModelBoolean getUseDiskModel() {
        return m_useDisk;
    }

    public boolean useDisk() {
        return m_useDisk.getBooleanValue();
    }

    /**
     * @return the useMemory
     */
    public SettingsModelBoolean getUseMemoryModel() {
        return m_useMemory;
    }

    public boolean useMemory() {
        return m_useMemory.getBooleanValue();
    }

    /**
     * @return the useOffHeap
     */
    public SettingsModelBoolean getUseOffHeapModel() {
        return m_useOffHeap;
    }

    public boolean useOffHeap() {
        return m_useOffHeap.getBooleanValue();
    }

    /**
     * @return the deserialized
     */
    public SettingsModelBoolean getDeserializedModel() {
        return m_deserialized;
    }

    public boolean shallDeserialize() {
        return m_deserialized.getBooleanValue();
    }

    /**
     * @return the replication
     */
    public SettingsModelInteger getReplicationModel() {
        return m_replication;
    }

    public int getReplication() {
        return m_replication.getIntValue();
    }

    /**
     * @return the Spark persistence level model
     */
    static SettingsModelString createStorageLevelModel() {
        return new SettingsModelString("storageLevel", PersistenceOption.getDefault().getActionCommand());
    }

    /**
     * @return the use disk model
     */
    static SettingsModelBoolean createUseDiskModel() {
        final SettingsModelBoolean model = new SettingsModelBoolean("useDisk", false);
        model.setEnabled(false);
        return model;
    }

    /**
     * @return the use memory model
     */
    static SettingsModelBoolean createUseMemoryModel() {
        final SettingsModelBoolean model = new SettingsModelBoolean("useMemory", false);
        model.setEnabled(false);
        return model;
    }

    /**
     * @return the use off heap model
     */
    static SettingsModelBoolean createUseOffHeapModel() {
        final SettingsModelBoolean model = new SettingsModelBoolean("useOffHeap", false);
        model.setEnabled(false);
        return model;
    }

    /**
     * @return the use disk model
     */
    static SettingsModelBoolean createDeserializedModel() {
        final SettingsModelBoolean model = new SettingsModelBoolean("deserialized", false);
        model.setEnabled(false);
        return model;
    }

    /**
     * @return the use disk model
     */
    static SettingsModelInteger createReplicationModel() {
        final SettingsModelIntegerBounded model =
            new SettingsModelIntegerBounded("replication", 2, 1, Integer.MAX_VALUE);
        model.setEnabled(false);
        return model;
    }

    public void updateEnabledness() {
        final boolean custom = PersistenceOption.CUSTOM.getActionCommand().equals(m_storageLevel.getStringValue());
        m_useDisk.setEnabled(custom);
        m_useMemory.setEnabled(custom);
        m_useOffHeap.setEnabled(custom);
        m_deserialized.setEnabled(custom);
        m_replication.setEnabled(custom);
    }

    /**
     * Saves the the settings of this instance to the given {@link NodeSettingsWO}.
     *
     * @param settings the NodeSettingsWO to write to.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_storageLevel.saveSettingsTo(settings);
        m_useDisk.saveSettingsTo(settings);
        m_useMemory.saveSettingsTo(settings);
        m_useOffHeap.saveSettingsTo(settings);
        m_deserialized.saveSettingsTo(settings);
        m_replication.saveSettingsTo(settings);
    }

    /**
     * Validates the settings in the given {@link NodeSettingsRO}.
     *
     * @param settings the NodeSettingsRO to validate.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        String level = ((SettingsModelString)m_storageLevel.createCloneWithValidatedValue(settings)).getStringValue();
        if (level == null || level.isEmpty()) {
            throw new InvalidSettingsException("storage level must not be empty");
        }
        if (PersistenceOption.valueOf(level) == null) {
            throw new InvalidSettingsException("Invalid storage level: " + level);
        }
        m_useDisk.validateSettings(settings);
        m_useMemory.validateSettings(settings);
        m_useOffHeap.validateSettings(settings);
        m_deserialized.validateSettings(settings);
        m_replication.validateSettings(settings);
    }

    /**
     * @param settings the NodeSettingsRO to read from.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_storageLevel.loadSettingsFrom(settings);
        m_useDisk.loadSettingsFrom(settings);
        m_useMemory.loadSettingsFrom(settings);
        m_useOffHeap.loadSettingsFrom(settings);
        m_deserialized.loadSettingsFrom(settings);
        m_replication.loadSettingsFrom(settings);
    }
}
