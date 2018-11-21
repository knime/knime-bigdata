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
package org.knime.bigdata.spark.node.util.rdd.persist;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkPersistNodeDialog extends DefaultNodeSettingsPane {

    private final SparkPersistNodeSettings m_settings = new SparkPersistNodeSettings();

    SparkPersistNodeDialog() {
        addDialogComponent(new DialogComponentButtonGroup(m_settings.getStorageLevelModel(), "Storage level:", true, PersistenceOption.values()));
        createNewGroup(" Custom Storage Parameter ");
        setHorizontalPlacement(true);
        addDialogComponent(new DialogComponentBoolean(m_settings.getUseDiskModel(), "Use disk"));
        addDialogComponent(new DialogComponentBoolean(m_settings.getUseMemoryModel(), "Use memory"));
        addDialogComponent(new DialogComponentBoolean(m_settings.getUseOffHeapModel(), "Use off heap"));
        setHorizontalPlacement(false);
        setHorizontalPlacement(true);
        addDialogComponent(new DialogComponentBoolean(m_settings.getDeserializedModel(), "Deserialized"));
        addDialogComponent(new DialogComponentNumber(m_settings.getReplicationModel(), "Replication:", 1));
    }

}
