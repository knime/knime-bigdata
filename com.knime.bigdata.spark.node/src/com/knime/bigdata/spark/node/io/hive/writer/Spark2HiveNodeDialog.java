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
 *   Created on 07.06.2015 by koetter
 */
package com.knime.bigdata.spark.node.io.hive.writer;

import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentString;

import com.knime.bigdata.spark.core.node.SparkDefaultNodeSettingsPane;

/**
 *
 * @author koetter
 */
public class Spark2HiveNodeDialog extends SparkDefaultNodeSettingsPane {

    /**
     *
     */
    public Spark2HiveNodeDialog() {
        addDialogComponent(new DialogComponentString(Spark2HiveNodeModel.createTableNameModel(),
            "Table name: ", true, 20));
        addDialogComponent(new DialogComponentBoolean(Spark2HiveNodeModel.createDropExistingModel(),
            "Drop existing table"));
    }
}
