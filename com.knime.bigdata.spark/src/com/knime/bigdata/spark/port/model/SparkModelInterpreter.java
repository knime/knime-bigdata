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
package com.knime.bigdata.spark.port.model;

import java.io.Serializable;

/**
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
     * @return the string description of the model. Can contain html formatting information
     */
    public String getDescription(M model);

    /**
     * @param model the model
     * @return summary of the model to show in the port tooltip
     */
    public String getSummary(M model);
}
