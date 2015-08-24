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
 *   Created on 23.08.2015 by koetter
 */
package com.knime.bigdata.spark.port.model.interpreter;

import java.io.Serializable;

import javax.swing.JComponent;

import com.knime.bigdata.spark.port.model.SparkModel;

/**
 * {@link SparkModelInterpreter} implementation that returns a single HTML panel with the
 * description of the {@link SparkModel}.
 * @author Tobias Koetter, KNIME.com
 * @param <M> the {@link SparkModel}
 */
public abstract class HTMLModelInterpreter<M extends SparkModel<? extends Serializable>>
implements SparkModelInterpreter<M> {

    private static final long serialVersionUID = 1L;

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews(final M model) {
        final String htmlDesc = getHTMLDescription(model);
        return new JComponent[] {new HTMLModelView(model, htmlDesc)};
    }

    /**
     * @param model the {@link SparkModel}
     * @return the HTML description of the model
     */
    protected abstract String getHTMLDescription(M model);
}
