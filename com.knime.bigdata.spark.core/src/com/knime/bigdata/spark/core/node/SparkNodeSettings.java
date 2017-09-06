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
 *   Created on Sep 6, 2017 by bjoern
 */
package com.knime.bigdata.spark.core.node;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 *
 * @author bjoern
 */
public class SparkNodeSettings {



    /**
     * Read the expected values from the settings object, without assigning them
     * to the internal variables!
     *
     * @param settings the object to read the value(s) from
     * @throws InvalidSettingsException if the value(s) in the settings object
     *             are invalid.
     */
    public void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {


    }

    /**
     * Read value(s) of this settings model from the configuration object. If
     * the value is not stored in the config, an exception will be thrown. <br>
     * NOTE: Don't call this method directly, rather call loadSettingsFrom.<br>
     *
     * @param settings The {@link org.knime.core.node.NodeSettings} to read
     *            from.
     * @throws InvalidSettingsException if load fails.
     */
    public void loadSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {

    }


    /**
     * Write value(s) of this settings model to configuration object.<br>
     * NOTE: Don't call this method directly, rather call saveSettingsTo.
     *
     * @param settings The {@link org.knime.core.node.NodeSettings} to write
     *            into.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {

    }


}
