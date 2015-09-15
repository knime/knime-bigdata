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
 *   Created on 21.07.2015 by koetter
 */
package com.knime.bigdata.spark.port.model.interpreter;

import java.io.Serializable;

import javax.swing.JComponent;

import com.knime.bigdata.spark.port.model.SparkModel;

/**
 * Interface that describes a {@link SparkModel} interpreter.
 *
 * @author Tobias Koetter, KNIME.com
 * @param <M> the model
 */
public interface SparkModelInterpreter <M extends SparkModel<? extends Serializable>> extends Serializable {

    /**
     * @return the unique name of the model
     */
    public String getModelName();

    /**
     * @param model the model
     * @return the {@link JComponent} views of the model
     */
    public JComponent[] getViews(M model);

    /**
     * @param model the model
     * @return summary of the model to show in the port tooltip
     */
    public String getSummary(M model);
}
