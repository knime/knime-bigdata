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
 *   Created on 02.11.2015 by koetter
 */
package com.knime.bigdata.spark.node.pmml.converter.impl;

import java.io.Serializable;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.pmml.PMMLPortObject;

import com.knime.bigdata.spark.port.model.SparkModel;

/**
 * Interface of a SparkModel to PMML converter.
 *
 * @author Tobias Koetter, KNIME.com
 * @param <M> Spark model
 */
public interface SparkModel2PMML<M extends Serializable> {

    /**
     * @return the supported Spark model class
     */
    public Class<M> getSupportedModelClass();

    /**
     * @param sparkModel {@link SparkModel} to convert
     * @return {@link PMMLPortObject}
     * @throws InvalidSettingsException if the model is invalid
     */
    public PMMLPortObject convert(final SparkModel<M> sparkModel) throws InvalidSettingsException;
}