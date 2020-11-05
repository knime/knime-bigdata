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

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.database.port.DBSessionPortObject;

/**
 * Node factory for the "Create Big Data Test Environment" node using a Hive {@link DBSessionPortObject} and a
 * file system {@link ConnectionInformationPortObject} output port.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class CreateTestEnvironmentNodeFactory2 extends DefaultSparkNodeFactory<CreateTestEnvironmentNodeModel2> {

    /**
     * Constructor.
     */
    public CreateTestEnvironmentNodeFactory2() {
        super("");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CreateTestEnvironmentNodeModel2 createNodeModel() {
        return new CreateTestEnvironmentNodeModel2();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean hasDialog() {
        return false;
    }
}
