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
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.json;

import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.GenericDataSource2SparkNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.GenericDataSource2SparkNodeModel3;
import org.knime.core.node.NodeDialogPane;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Json2SparkNodeFactory3 extends
    GenericDataSource2SparkNodeFactory3<GenericDataSource2SparkNodeModel3<Json2SparkSettings3>, Json2SparkSettings3> {

    private static final String FORMAT = "json";
    private static final boolean HAS_DRIVER = false;

    @Override
    public Json2SparkSettings3 getSettings() {
        return new Json2SparkSettings3(FORMAT, SparkVersion.V_1_5, HAS_DRIVER, PORTS_CONFIGURATION);
    }

    @Override
    public GenericDataSource2SparkNodeModel3<Json2SparkSettings3> createNodeModel() {
        return new GenericDataSource2SparkNodeModel3<>(PORTS_CONFIGURATION, getSettings());
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new Json2SparkNodeDialog3(getSettings());
    }
}
