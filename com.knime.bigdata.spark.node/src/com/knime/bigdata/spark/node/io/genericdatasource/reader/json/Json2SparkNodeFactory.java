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
 *   Created on Sep 05, 2016 by Sascha
 */
package com.knime.bigdata.spark.node.io.genericdatasource.reader.json;

import org.knime.core.node.NodeDialogPane;

import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkNodeFactory;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkNodeModel;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Json2SparkNodeFactory extends GenericDataSource2SparkNodeFactory<GenericDataSource2SparkNodeModel<Json2SparkSettings>, Json2SparkSettings> {
    private final static String FORMAT = "json";
    private final static boolean HAS_DRIVER = false;

    @Override
    public Json2SparkSettings getSettings() {
        return new Json2SparkSettings(FORMAT, HAS_DRIVER);
    }

    @Override
    public GenericDataSource2SparkNodeModel<Json2SparkSettings> createNodeModel() {
        return new GenericDataSource2SparkNodeModel<Json2SparkSettings>(getSettings());
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new Json2SparkNodeDialog(getSettings());
    }
}
