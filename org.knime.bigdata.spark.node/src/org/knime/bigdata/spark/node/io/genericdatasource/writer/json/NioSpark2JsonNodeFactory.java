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
import org.knime.bigdata.spark.node.io.genericdatasource.writer.NioSpark2GenericDataSourceNodeDialog;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.NioSpark2GenericDataSourceNodeFactory;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.NioSpark2GenericDataSourceNodeModel;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.NioSpark2GenericDataSourceSettings;
import org.knime.core.node.NodeDialogPane;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class NioSpark2JsonNodeFactory extends
    NioSpark2GenericDataSourceNodeFactory<NioSpark2GenericDataSourceNodeModel<NioSpark2GenericDataSourceSettings>, NioSpark2GenericDataSourceSettings> {

    private static final String FORMAT = "json";
    private static final boolean HAS_PARTITIONING = true;
    private static final boolean HAS_DRIVER = false;

    @Override
    public NioSpark2GenericDataSourceSettings getSettings() {
        return new NioSpark2GenericDataSourceSettings(FORMAT, SparkVersion.V_1_5, HAS_PARTITIONING, HAS_DRIVER, PORTS_CONFIGURATION);
    }

    @Override
    public NioSpark2GenericDataSourceNodeModel<NioSpark2GenericDataSourceSettings> createNodeModel() {
        return new NioSpark2GenericDataSourceNodeModel<>(PORTS_CONFIGURATION, getSettings());
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new NioSpark2GenericDataSourceNodeDialog<>(getSettings());
    }
}
