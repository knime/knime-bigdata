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
 *   Created on Sep 05, 2016 by Sascha
 */
package org.knime.bigdata.spark.node.io.genericdatasource.writer.json;

import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeDialog;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeFactory;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeModel;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceSettings;
import org.knime.core.node.NodeDialogPane;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2JsonNodeFactory extends Spark2GenericDataSourceNodeFactory<Spark2GenericDataSourceNodeModel<Spark2GenericDataSourceSettings>, Spark2GenericDataSourceSettings> {
    private final static String FORMAT = "json";
    private final static boolean HAS_PARTITIONING = true;
    private final static boolean HAS_DRIVER = false;

    @Override
    public Spark2GenericDataSourceSettings getSettings() {
        return new Spark2GenericDataSourceSettings(FORMAT, SparkVersion.V_1_5, HAS_PARTITIONING, HAS_DRIVER);
    }

    @Override
    public Spark2GenericDataSourceNodeModel<Spark2GenericDataSourceSettings> createNodeModel() {
        return new Spark2GenericDataSourceNodeModel<>(getSettings());
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new Spark2GenericDataSourceNodeDialog<>(getSettings());
    }
}
