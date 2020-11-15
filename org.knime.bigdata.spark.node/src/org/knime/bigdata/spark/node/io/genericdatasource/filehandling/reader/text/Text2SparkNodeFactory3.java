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
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.text;

import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.GenericDataSource2SparkNodeDialog3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.GenericDataSource2SparkNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.GenericDataSource2SparkNodeModel3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.GenericDataSource2SparkSettings3;
import org.knime.core.node.NodeDialogPane;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Text2SparkNodeFactory3 extends
    GenericDataSource2SparkNodeFactory3<GenericDataSource2SparkNodeModel3<GenericDataSource2SparkSettings3>, GenericDataSource2SparkSettings3> {

    private static final String FORMAT = "text";
    private static final boolean HAS_DRIVER = false;

    @Override
    public GenericDataSource2SparkSettings3 getSettings() {
        return new GenericDataSource2SparkSettings3(FORMAT, SparkVersion.V_1_6, HAS_DRIVER, PORTS_CONFIGURATION);
    }

    @Override
    public GenericDataSource2SparkNodeModel3<GenericDataSource2SparkSettings3> createNodeModel() {
        return new GenericDataSource2SparkNodeModel3<>(PORTS_CONFIGURATION, getSettings());
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new GenericDataSource2SparkNodeDialog3<>(getSettings());
    }
}
