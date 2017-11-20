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
 *   Created on 21.07.2015 by koetter
 */
package org.knime.bigdata.spark.core.port.model;

import java.io.Serializable;

import javax.swing.JComponent;

/**
 * Interface that describes a {@link SparkModel} interpreter.
 *
 * @author Tobias Koetter, KNIME.com
 */
public interface ModelInterpreter extends Serializable {

    /**
     * @return the unique name of the model
     */
    public String getModelName();

    /**
     * @param sparkModel the {@link SparkModel}
     * @return the {@link JComponent} views of the model
     */
    public JComponent[] getViews(SparkModel sparkModel);

    /**
     * @param sparkModel the {@link SparkModel}
     * @return summary of the model to show in the port tooltip
     */
    public String getSummary(SparkModel sparkModel);
}
