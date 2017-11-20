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
 *   Created on 26.06.2015 by koetter
 */
package org.knime.bigdata.spark.node.io.table.reader;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;

/**
 *
 * @author koetter
 */
public class Table2SparkNodeFactory2 extends DefaultSparkNodeFactory<Table2SparkNodeModel> {

    /**
     * Constructor.
     */
    public Table2SparkNodeFactory2() {
        super("io/read");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Table2SparkNodeModel createNodeModel() {
        return new Table2SparkNodeModel(false);
    }
}
