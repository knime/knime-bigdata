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
 *   Created on 02.11.2015 by koetter
 */
package com.knime.bigdata.spark.node.pmml.converter;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.pmml.PMMLPortObject;

import com.knime.bigdata.spark.core.port.model.SparkModel;

/**
 * Interface of a SparkModel to PMML converter.
 *
 * @author Tobias Koetter, KNIME.com
 */
public interface PMMLPortObjectFactory {

    /**
     * @return the unique name of the model
     */
    public String getModelName();

    /**
     * @param sparkModel {@link SparkModel} to convert
     * @return {@link PMMLPortObject}
     * @throws InvalidSettingsException if the model is invalid
     */
    public PMMLPortObject convert(final SparkModel sparkModel) throws InvalidSettingsException;
}