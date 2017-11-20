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
package com.knime.bigdata.spark.node.io.genericdatasource.reader.csv;

import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkNodeFactory;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkNodeModel;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.csv.CSV2SparkNodeDialog;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.csv.CSV2SparkSettings;
import org.knime.core.node.NodeDialogPane;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class CSV2SparkNodeFactory extends GenericDataSource2SparkNodeFactory<GenericDataSource2SparkNodeModel<CSV2SparkSettings>, CSV2SparkSettings> {
    private final static String FORMAT = "com.databricks.spark.csv";
    private final static boolean HAS_DRIVER = true;

    @Override
    public CSV2SparkSettings getSettings() {
        return new CSV2SparkSettings(FORMAT, SparkVersion.V_1_5, HAS_DRIVER);
    }

    @Override
    public GenericDataSource2SparkNodeModel<CSV2SparkSettings> createNodeModel() {
        return new GenericDataSource2SparkNodeModel<>(getSettings(), true);
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new CSV2SparkNodeDialog(getSettings());
    }
}
