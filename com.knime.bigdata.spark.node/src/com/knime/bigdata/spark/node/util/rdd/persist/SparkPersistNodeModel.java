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
 *   Created on 28.08.2015 by koetter
 */
package com.knime.bigdata.spark.node.util.rdd.persist;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.job.SimpleJobRunFactory;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkPersistNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkPersistNodeModel.class.getCanonicalName();

    private final SettingsModelString m_storageLevel = createStorageLevelModel();

    private final SettingsModelBoolean m_useDisk = createUseDiskModel();
    private final SettingsModelBoolean m_useMemory = createUseMemoryModel();
    private final SettingsModelBoolean m_useOffHeap = createUseOffHeapModel();
    private final SettingsModelBoolean m_deserialized = createDeserializedModel();
    private final SettingsModelInteger m_replication = createReplicationModel();

    SparkPersistNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{SparkDataPortObject.TYPE}, false);
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
        final SettingsModelIntegerBounded model = new SettingsModelIntegerBounded("replication", 2, 1, Integer.MAX_VALUE);
        model.setEnabled(false);
        return model;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return inSpecs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rdd = (SparkDataPortObject)inData[0];
        final SparkContextID contextID = rdd.getContextID();
        final String level = m_storageLevel.getStringValue();
        final PersistenceOption option = PersistenceOption.getOption(level);
        final boolean useDisk;
        final boolean useMemory;
        final boolean useOffHeap;
        final boolean deserialized;
        final int replication;
        if (PersistenceOption.CUSTOM.equals(option)) {
            useDisk = m_useDisk.getBooleanValue();
            useMemory = m_useMemory.getBooleanValue();
            useOffHeap = m_useOffHeap.getBooleanValue();
            deserialized = m_deserialized.getBooleanValue();
            replication = m_replication.getIntValue();
        } else {
            useDisk = option.useDisk();
            useMemory = option.useMemory();
            useOffHeap = option.useOffHeap();
            deserialized = option.isDeserialized();
            replication = option.getReplication();
        }
        final PersistJobInput input =
                new PersistJobInput(rdd.getTableName(), useDisk, useMemory, useOffHeap, deserialized, replication);
        final SimpleJobRunFactory<PersistJobInput> runFactory = getSimpleJobRunFactory(rdd, JOB_ID);
        runFactory.createRun(input).run(contextID, exec);
        return inData;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_storageLevel.saveSettingsTo(settings);
        m_useDisk.saveSettingsTo(settings);
        m_useMemory.saveSettingsTo(settings);
        m_useOffHeap.saveSettingsTo(settings);
        m_deserialized.saveSettingsTo(settings);
        m_replication.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
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
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_storageLevel.loadSettingsFrom(settings);
        m_useDisk.loadSettingsFrom(settings);
        m_useMemory.loadSettingsFrom(settings);
        m_useOffHeap.loadSettingsFrom(settings);
        m_deserialized.loadSettingsFrom(settings);
        m_replication.loadSettingsFrom(settings);
    }

}
