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
 *   Created on 07.08.2014 by koetter
 */
package com.knime.bigdata.hdfs.node.filesettings;

import org.knime.core.data.uri.URIDataValue;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentString;

/**
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
public class SetFilePermissionNodeDialog extends DefaultNodeSettingsPane {
    /**
     * Constructor.
     */
    @SuppressWarnings("unchecked")
    public SetFilePermissionNodeDialog() {
        addDialogComponent(new DialogComponentColumnNameSelection(SetFilePermissionNodeModel.createURIColModel(),
            "URI column: ", 1, true, URIDataValue.class));
        addDialogComponent(new DialogComponentString(SetFilePermissionNodeModel.createFilePermissionModel(),
            "Unix style file permission: ", true, 10));
    }
}
