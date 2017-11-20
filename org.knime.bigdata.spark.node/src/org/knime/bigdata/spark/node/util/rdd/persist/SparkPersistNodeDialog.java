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
 *   Created on 28.08.2015 by koetter
 */
package com.knime.bigdata.spark.node.util.rdd.persist;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkPersistNodeDialog extends DefaultNodeSettingsPane {

    SparkPersistNodeDialog() {
        final SettingsModelString level = SparkPersistNodeModel.createStorageLevelModel();
        final SettingsModelBoolean useDisk = SparkPersistNodeModel.createUseDiskModel();
        final SettingsModelBoolean useMemory = SparkPersistNodeModel.createUseMemoryModel();
        final SettingsModelBoolean useOffHeap = SparkPersistNodeModel.createUseOffHeapModel();
        final SettingsModelBoolean deserialized = SparkPersistNodeModel.createDeserializedModel();
        final SettingsModelInteger replication = SparkPersistNodeModel.createReplicationModel();
        level.addChangeListener(new ChangeListener() {

            @Override
            public void stateChanged(final ChangeEvent e) {
                final boolean custom = PersistenceOption.CUSTOM.getActionCommand().equals(level.getStringValue());
                useDisk.setEnabled(custom);
                useMemory.setEnabled(custom);
                useOffHeap.setEnabled(custom);
                deserialized.setEnabled(custom);
                replication.setEnabled(custom);
            }
        });
        addDialogComponent(new DialogComponentButtonGroup(level, "Storage level:", true, PersistenceOption.values()));
        createNewGroup(" Custom Storage Parameter ");
        setHorizontalPlacement(true);
        addDialogComponent(new DialogComponentBoolean(useDisk, "Use disk"));
        addDialogComponent(new DialogComponentBoolean(useMemory, "Use memory"));
        addDialogComponent(new DialogComponentBoolean(useOffHeap, "Use off heap"));
        setHorizontalPlacement(false);
        setHorizontalPlacement(true);
        addDialogComponent(new DialogComponentBoolean(deserialized, "Deserialized"));
        addDialogComponent(new DialogComponentNumber(replication, "Replication:", 1));
    }

}
