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
 *   Created on 03.07.2015 by koetter
 */
package org.knime.bigdata.spark.local.node.create;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;

/**
 * Node factory for the "Create Local Big Data Environment" node.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LocalEnvironmentCreatorNodeFactory2 extends DefaultSparkNodeFactory<LocalEnvironmentCreatorNodeModel2> {

    /**
     * Constructor.
     */
    public LocalEnvironmentCreatorNodeFactory2() {
        super("");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalEnvironmentCreatorNodeModel2 createNodeModel() {
        return new LocalEnvironmentCreatorNodeModel2();
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
        return new LocalEnvironmentCreatorNodeDialog();
    }
}
