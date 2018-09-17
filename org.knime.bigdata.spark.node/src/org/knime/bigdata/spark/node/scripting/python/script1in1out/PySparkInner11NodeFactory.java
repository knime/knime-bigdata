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
 *   Created on 22.08.2018 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark.node.scripting.python.script1in1out;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.node.scripting.python.PySparkNodeDialog;
import org.knime.bigdata.spark.node.scripting.python.PySparkNodeModel;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.port.PortType;

/**
 * The Factory for a 1 in 1 out PySpark node
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class PySparkInner11NodeFactory extends DefaultSparkNodeFactory<PySparkNodeModel> {

    /**
     * Creates a PySpark Node Factory
     */
    public PySparkInner11NodeFactory() {
        super("misc/python");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PySparkNodeModel createNodeModel() {

        return new PySparkNodeModel(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new PySparkNodeDialog(1,1);
    }

}
