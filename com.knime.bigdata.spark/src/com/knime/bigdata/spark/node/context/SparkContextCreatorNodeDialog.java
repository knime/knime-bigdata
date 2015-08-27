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

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentString;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class SparkContextCreatorNodeDialog extends DefaultNodeSettingsPane {

    /**
     *
     */
    SparkContextCreatorNodeDialog() {
        createNewGroup("Job Server");
        addDialogComponent(new DialogComponentString(ContextSettings.createHostModel(), "Host: "));
        addDialogComponent(new DialogComponentNumber(ContextSettings.createPortModel(), "Port: ", 1));
        addDialogComponent(new DialogComponentString(ContextSettings.createUserModel(), "User: "));
        createNewGroup("Spark Context");
        addDialogComponent(new DialogComponentString(ContextSettings.createIDModel(), "Context name: "));
        addDialogComponent(new DialogComponentString(ContextSettings.createMemoryModel(), "Memory: "));
        addDialogComponent(new DialogComponentNumber(ContextSettings.createNoOfCoresModel(), "Number of cores: ", 1));
        addDialogComponent(new DialogComponentNumber(ContextSettings.createJobTimeoutModel(),
            "Spark job timeout (seconds): ", 1));
        addDialogComponent(new DialogComponentNumber(ContextSettings.createJobCheckFrequencyModel(),
            "Spark job check frequency (Seconds): ", 1));
    }
}
