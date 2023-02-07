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
 *   Created on Apr 26, 2016 by bjoern
 */
package org.knime.bigdata.spark.local.jobs.prepare;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.DataType;
import org.knime.bigdata.spark.core.context.util.PrepareContextJobInput;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark3_3.api.NamedObjects;
import org.knime.bigdata.spark3_3.api.SimpleSparkJob;
import org.knime.bigdata.spark3_3.api.TypeConverters;
import org.knime.bigdata.spark3_3.base.Spark_3_3_CustomUDFProvider;

/**
 * Spark job to prepare a newly-created local Spark context.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class PrepareLocalSparkContextJob implements SimpleSparkJob<PrepareContextJobInput> {

    private static final long serialVersionUID = 5767134504557370285L;

    /**
     * {@inheritDoc}
     */
    @Override
    public void runJob(final SparkContext sparkContext, final PrepareContextJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        TypeConverters.ensureConvertersInitialized(input.<DataType>getTypeConverters());
        Spark_3_3_CustomUDFProvider.registerCustomUDFs(sparkContext);
    }
}
