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
 *   Created on Aug 10, 2016 by sascha
 */
package com.knime.bigdata.spark.node.io.genericdatasource.reader.avro;

import org.knime.core.node.NodeDialogPane;

import com.knime.bigdata.spark.core.version.SparkPluginVersion;
import com.knime.bigdata.spark.core.version.SparkVersion;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkNodeDialog;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkNodeModel;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkSettings;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Avro2SparkNodeFactory2 extends GenericDataSource2SparkNodeFactory<GenericDataSource2SparkNodeModel<GenericDataSource2SparkSettings>, GenericDataSource2SparkSettings> {
    private final static String FORMAT = "com.databricks.spark.avro";
    private final static boolean HAS_DRIVER = true;

    @Override
    public GenericDataSource2SparkSettings getSettings() {
        return new GenericDataSource2SparkSettings(FORMAT, SparkVersion.V_1_5, HAS_DRIVER, SparkPluginVersion.VERSION_CURRENT);
    }

    @Override
    public GenericDataSource2SparkNodeModel<GenericDataSource2SparkSettings> createNodeModel() {
        return new GenericDataSource2SparkNodeModel<>(getSettings(), false);
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new GenericDataSource2SparkNodeDialog<>(getSettings());
    }
}
