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
 *   Created on 24.06.2016 by oole
 */
package org.knime.bigdata.spark.node.io.impala.reader;

import org.knime.bigdata.impala.utility.ImpalaUtility;
import org.knime.bigdata.spark.node.io.hive.reader.Hive2SparkNodeModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.database.DatabasePortObjectSpec;

/**
 *
 * @author Ole Ostergaard, KNIME.com
 */
public class Impala2SparkNodeModel extends Hive2SparkNodeModel {

    /**
     * Default constructor.
     * @param optionalSparkPort true if input spark context port is optional
     */
    public Impala2SparkNodeModel(final boolean optionalSparkPort) {
        super(optionalSparkPort);
    }

    @Override
    protected void checkDatabaseIdentifier(final DatabasePortObjectSpec spec) throws InvalidSettingsException {
        if (!ImpalaUtility.DATABASE_IDENTIFIER.equals(spec.getDatabaseIdentifier())) {
            throw new InvalidSettingsException("Input must be an Impala connection");
        }
    }
}
