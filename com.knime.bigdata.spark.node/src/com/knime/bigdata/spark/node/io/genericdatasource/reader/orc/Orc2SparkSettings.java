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
 *   Created on Oct 10, 2016 by Sascha Wolke, KNIME.com
 */
package com.knime.bigdata.spark.node.io.genericdatasource.reader.orc;

import org.osgi.framework.Version;

import com.knime.bigdata.spark.core.version.SparkPluginVersion;
import com.knime.bigdata.spark.core.version.SparkVersion;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkJobInput;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkSettings;

/**
 * ORC reader specific settings.
 * @author Sascha Wolke, KNIME.com
 */
public class Orc2SparkSettings extends GenericDataSource2SparkSettings {

    /** @see GenericDataSource2SparkSettings#GenericDataSource2SparkSettings(String, boolean) */
    public Orc2SparkSettings(final String format, final SparkVersion minSparkVersion, final boolean hasDriver, final Version knimeSparkExecutorVersion) {
        super(format, minSparkVersion, hasDriver, knimeSparkExecutorVersion);
    }

    @Override
    protected GenericDataSource2SparkSettings newInstance() {
        return new Orc2SparkSettings(getFormat(), getMinSparkVersion(), hasDriver(), SparkPluginVersion.VERSION_CURRENT);
    }

    @Override
    public void addReaderOptions(final GenericDataSource2SparkJobInput jobInput) {
        jobInput.setUseHiveContext(true);
    }
}
