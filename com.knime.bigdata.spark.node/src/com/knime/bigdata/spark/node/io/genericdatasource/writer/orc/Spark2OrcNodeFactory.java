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
package com.knime.bigdata.spark.node.io.genericdatasource.writer.orc;

import org.knime.core.node.NodeDialogPane;

import com.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeDialog;
import com.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeFactory;
import com.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeModel;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2OrcNodeFactory extends Spark2GenericDataSourceNodeFactory<Spark2GenericDataSourceNodeModel<Spark2OrcSettings>, Spark2OrcSettings> {
    private final static String FORMAT = "orc";
    private final static boolean HAS_PARTITIONING = true;
    private final static boolean HAS_DRIVER = false;

    @Override
    public Spark2OrcSettings getSettings() {
        return new Spark2OrcSettings(FORMAT, HAS_PARTITIONING, HAS_DRIVER);
    }

    @Override
    public Spark2GenericDataSourceNodeModel<Spark2OrcSettings> createNodeModel() {
        return new Spark2GenericDataSourceNodeModel<Spark2OrcSettings>(getSettings());
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new Spark2GenericDataSourceNodeDialog<Spark2OrcSettings>(getSettings());
    }
}
