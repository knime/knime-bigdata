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
 */
package org.knime.bigdata.spark.node.io.genericdatasource.writer.text;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeModel;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceSettings;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.StringValue;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2TextNodeModel extends Spark2GenericDataSourceNodeModel<Spark2GenericDataSourceSettings> {

    Spark2TextNodeModel(final Spark2GenericDataSourceSettings settings) {
        super(settings);
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rdd = (SparkDataPortObject) inData[1];
        final DataTableSpec tableSpec = rdd.getTableSpec();

        if (tableSpec.getNumColumns() != 1 || !tableSpec.getColumnSpec(0).getType().isCompatible(StringValue.class)) {
            throw new InvalidSettingsException("Text writer requires exactly one string column.");
        }

        return super.executeInternal(inData, exec);
    }
}