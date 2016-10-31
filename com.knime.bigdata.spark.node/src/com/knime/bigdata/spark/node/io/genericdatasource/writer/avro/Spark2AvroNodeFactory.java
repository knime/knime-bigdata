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
package com.knime.bigdata.spark.node.io.genericdatasource.writer.avro;

import com.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeDialog;
import com.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeFactory;
import com.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeModel;
import com.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceSettings;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2AvroNodeFactory extends Spark2GenericDataSourceNodeFactory<Spark2GenericDataSourceNodeModel<Spark2GenericDataSourceSettings>, Spark2GenericDataSourceSettings> {
    private static final String FORMAT = "com.databricks.spark.avro";
    private final static boolean HAS_PARTITIONING = true;
    private final static boolean HAS_DRIVER = true;

    @Override
    public Spark2GenericDataSourceSettings getSettings() {
        return new Spark2GenericDataSourceSettings(FORMAT, HAS_PARTITIONING, HAS_DRIVER);
    }

    @Override
    public Spark2GenericDataSourceNodeModel<Spark2GenericDataSourceSettings> createNodeModel() {
        return new Spark2GenericDataSourceNodeModel<>(getSettings());
    }

    @Override
    protected Spark2GenericDataSourceNodeDialog<Spark2GenericDataSourceSettings> createNodeDialogPane() {
        return new Spark2GenericDataSourceNodeDialog<>(getSettings());
    }
}
