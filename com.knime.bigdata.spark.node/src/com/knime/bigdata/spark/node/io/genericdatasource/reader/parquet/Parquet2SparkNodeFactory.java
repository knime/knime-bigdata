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
 *   Created on Aug 10, 2016 by sascha
 */
package com.knime.bigdata.spark.node.io.genericdatasource.reader.parquet;

import org.knime.core.node.NodeDialogPane;

import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkNodeDialog;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkNodeModel;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkSettings;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Parquet2SparkNodeFactory extends GenericDataSource2SparkNodeFactory<GenericDataSource2SparkNodeModel<GenericDataSource2SparkSettings>, GenericDataSource2SparkSettings> {
    private final static String FORMAT = "parquet";
    private final static boolean HAS_DRIVER = false;

    @Override
    public GenericDataSource2SparkSettings getSettings() {
        return new GenericDataSource2SparkSettings(FORMAT, HAS_DRIVER);
    }

    @Override
    public GenericDataSource2SparkNodeModel<GenericDataSource2SparkSettings> createNodeModel() {
        return new GenericDataSource2SparkNodeModel<GenericDataSource2SparkSettings>(getSettings());
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new GenericDataSource2SparkNodeDialog<GenericDataSource2SparkSettings>(getSettings());
    }
}
