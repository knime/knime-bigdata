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
 *   Created on 4 Jul 2018 by Marc Bux, KNIME AG, Zurich, Switzerland
 */
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.orc;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.Spark2GenericDataSourceNodeModel3;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;

/**
 *
 * @author Marc Bux, KNIME AG, Zurich, Switzerland
 */
public class Spark2OrcNodeModel3 extends Spark2GenericDataSourceNodeModel3<Spark2OrcSettings3> {
    // these special characters are known to cause issues with Spark to ORC and ORC to Spark nodes (see BD-701)
    private final static String PROBLEMATIC_SPECIAL_CHARACTERS = "<>:-";

    private final static String SPECIAL_CHARACTERS_WARNING = "Problematic special characters in column name";

    private boolean m_containsSpecialChars = false;

    Spark2OrcNodeModel3(final PortsConfiguration portsConfig, final Spark2OrcSettings3 settings) {
        super(portsConfig, settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        m_containsSpecialChars = false;
        // iterate over column names; if a problematic special character is found, display a warning and set the special character flag
        for (PortObjectSpec portSpec : inSpecs) {
            if (portSpec instanceof SparkDataPortObjectSpec) {
                SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec)portSpec;
                for (String column : sparkSpec.getTableSpec().getColumnNames()) {
                    if (StringUtils.containsAny(column, PROBLEMATIC_SPECIAL_CHARACTERS)) {
                        setWarningMessage(SPECIAL_CHARACTERS_WARNING + " \"" + column + "\"");
                        m_containsSpecialChars = true;
                        break;
                    }
                }
                break;
            }
        }
        return super.configureInternal(inSpecs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        try {
            return super.executeInternal(inData, exec);
        } catch (KNIMESparkException e) {
            // if the special character flag was set during configuration, add some more spice to the error message for debugging
            if (m_containsSpecialChars) {
                throw new KNIMESparkException(
                    e.getMessage() + " This might be caused by special characters in column names.");
            }
            throw e;
        }
    }

}
