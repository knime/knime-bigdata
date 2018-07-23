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
 *   Created on 23.07.2018 by bjoern.lohrmann
 */
package org.knime.bigdata.testing.node.create;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;

/**
 * Node factory for the "Create Big Data Test Environment" node.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class CreateTestEnvironmentNodeFactory extends DefaultSparkNodeFactory<CreateTestEnvironmentNodeModel> {

    /**
     * Constructor.
     */
    public CreateTestEnvironmentNodeFactory() {
        super("");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CreateTestEnvironmentNodeModel createNodeModel() {
        return new CreateTestEnvironmentNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean hasDialog() {
        return false;
    }
}
