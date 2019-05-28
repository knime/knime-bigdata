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
 *   Created on 23.08.2015 by koetter
 */
package org.knime.bigdata.spark.core.port.model.interpreter;

import java.text.NumberFormat;
import java.util.Locale;

import javax.swing.JComponent;

import org.knime.bigdata.spark.core.port.model.MLlibModel;
import org.knime.bigdata.spark.core.port.model.ModelInterpreter;
import org.knime.bigdata.spark.core.port.model.SparkModel;

/**
 * {@link ModelInterpreter} implementation that returns a single HTML panel with the
 * description of the model.
 *
 * @author Tobias Koetter, KNIME.com
 * @param <T> The type of Spark model to interpret.
 */
public abstract class HTMLModelInterpreter<T extends SparkModel> implements ModelInterpreter<T> {

    private static final long serialVersionUID = 1L;

    /** @return Standard number formatter. */
    public static final NumberFormat getNumberFormat() {
        return NumberFormat.getNumberInstance(Locale.US);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews(final T sparkModel) {
        final String htmlDesc = getHTMLDescription(sparkModel);
        return new JComponent[] {new HTMLModelView(sparkModel, htmlDesc)};
    }

    /**
     * @param sparkModel the {@link MLlibModel}
     * @return the HTML description of the model
     */
    protected abstract String getHTMLDescription(T sparkModel);
}
