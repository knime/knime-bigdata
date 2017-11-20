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
 *   Created on Oct 10, 2016 by Sascha Wolke, KNIME.com
 */
package org.knime.bigdata.spark.node.io.genericdatasource.writer.orc;

import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceJobInput;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceSettings;

/**
 * ORC writer settings.
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2OrcSettings extends Spark2GenericDataSourceSettings {

    /** @see Spark2GenericDataSourceSettings#Spark2GenericDataSourceSettings(String, boolean, boolean) */
    public Spark2OrcSettings(final String format, final SparkVersion minSparkVersion, final boolean supportsPartitioning, final boolean hasDriver) {
        super(format, minSparkVersion, supportsPartitioning, hasDriver);
    }

    @Override
    protected Spark2GenericDataSourceSettings newInstance() {
        return new Spark2OrcSettings(getFormat(), getMinSparkVersion(), supportsPartitioning(), hasDriver());
    }

    @Override
    public void addWriterOptions(final Spark2GenericDataSourceJobInput jobInput) {
        jobInput.setUseHiveContext(true);
    }
}
