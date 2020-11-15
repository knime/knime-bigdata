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
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.orc;

import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.Spark2GenericDataSourceNodeDialog3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.Spark2GenericDataSourceNodeFactory3;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.Spark2GenericDataSourceNodeModel3;
import org.knime.core.node.NodeDialogPane;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2OrcNodeFactory3 extends
    Spark2GenericDataSourceNodeFactory3<Spark2GenericDataSourceNodeModel3<Spark2OrcSettings3>, Spark2OrcSettings3> {

    private static final String FORMAT = "orc";
    private static final boolean HAS_PARTITIONING = true;
    private static final boolean HAS_DRIVER = false;

    @Override
    public Spark2OrcSettings3 getSettings() {
        return new Spark2OrcSettings3(FORMAT, SparkVersion.V_1_5, HAS_PARTITIONING, HAS_DRIVER, PORTS_CONFIGURATION);
    }

    @Override
    public Spark2GenericDataSourceNodeModel3<Spark2OrcSettings3> createNodeModel() {
        return new Spark2OrcNodeModel3(PORTS_CONFIGURATION, getSettings());
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new Spark2GenericDataSourceNodeDialog3<>(getSettings());
    }
}
