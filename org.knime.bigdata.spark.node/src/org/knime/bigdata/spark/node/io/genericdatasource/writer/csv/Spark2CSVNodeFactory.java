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
package org.knime.bigdata.spark.node.io.genericdatasource.writer.csv;

import org.knime.core.node.NodeDialogPane;

import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeFactory;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeModel;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2CSVNodeFactory extends Spark2GenericDataSourceNodeFactory<Spark2GenericDataSourceNodeModel<Spark2CSVSettings>, Spark2CSVSettings> {
    private final static String FORMAT = "com.databricks.spark.csv";
    private final static boolean HAS_PARTITIONING = true;
    private final static boolean HAS_DRIVER = true;

    @Override
    public Spark2CSVSettings getSettings() {
        return new Spark2CSVSettings(FORMAT, SparkVersion.V_1_5, HAS_PARTITIONING, HAS_DRIVER);
    }

    @Override
    public Spark2GenericDataSourceNodeModel<Spark2CSVSettings> createNodeModel() {
        return new Spark2GenericDataSourceNodeModel<>(getSettings());
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new Spark2CSVNodeDialog(getSettings());
    }
}
