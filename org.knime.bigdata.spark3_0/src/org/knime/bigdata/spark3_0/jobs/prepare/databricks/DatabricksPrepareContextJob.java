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
package org.knime.bigdata.spark3_0.jobs.prepare.databricks;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.DataType;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksPrepareContextJobInput;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksPrepareContextJobOutput;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksSparkSideStagingArea;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksStagingAreaTester;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.jar.JobJarDescriptor;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark3_0.api.DistributedFileUtils;
import org.knime.bigdata.spark3_0.api.NamedObjects;
import org.knime.bigdata.spark3_0.api.SparkConfigUtil;
import org.knime.bigdata.spark3_0.api.SparkJob;
import org.knime.bigdata.spark3_0.api.TypeConverters;
import org.knime.bigdata.spark3_0.base.Spark_3_0_CustomUDFProvider;
import org.knime.bigdata.spark3_0.jobs.prepare.ValidationUtil;

import scala.Tuple2;

/**
 * Context preparation job used after a Spark context was started in Databricks Workspace.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class DatabricksPrepareContextJob implements SparkJob<DatabricksPrepareContextJobInput, DatabricksPrepareContextJobOutput> {

    private static final long serialVersionUID = 5767134504557370285L;

    @Override
    public DatabricksPrepareContextJobOutput runJob(final SparkContext sparkContext, final DatabricksPrepareContextJobInput input,
        final NamedObjects namedObjects) throws Exception {

        DistributedFileUtils.ensureInitialized(DatabricksSparkSideStagingArea.SINGLETON_INSTANCE);

        sparkContext.addSparkListener(new KNIMEDatabricksSparkListener());

        try {
            JobJarDescriptor jobJarInfo =
                JobJarDescriptor.load(this.getClass().getClassLoader().getResourceAsStream(JobJarDescriptor.FILE_NAME));

            ValidationUtil.validateSparkVersion(sparkContext, input);
            ValidationUtil.validateKNIMEPluginVersion(input, jobJarInfo);
        } catch (IOException e) {
            throw new KNIMESparkException(
                "Spark context was probably not created with KNIME Extension for Apache Spark (or an old version of it). Please destroy and reopen this Spark context or use a different one.",
                e);
        }

        TypeConverters.ensureConvertersInitialized(input.<DataType> getTypeConverters());
        Spark_3_0_CustomUDFProvider.registerCustomUDFs(sparkContext);

        final String testfileName = validateStagingAreaAccess(input.getTestfileName());
        final String sparkWebUI = sparkContext.uiWebUrl().getOrElse(null);
        final Map<String, String> sparkConf =
            Arrays.stream(sparkContext.conf().getAll()).collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
        return new DatabricksPrepareContextJobOutput(sparkWebUI, sparkConf, testfileName,
            SparkConfigUtil.adaptiveExecutionEnabled(sparkContext));
    }

    private static String validateStagingAreaAccess(final String testfileName) throws KNIMESparkException {
        try {
            DatabricksStagingAreaTester.validateTestfileContent(DatabricksSparkSideStagingArea.SINGLETON_INSTANCE, testfileName);
            return DatabricksStagingAreaTester.writeTestfileContent(DatabricksSparkSideStagingArea.SINGLETON_INSTANCE);
        } catch (Exception e) {
            throw new KNIMESparkException("Staging area access from inside Spark failed: " + e.getMessage(), e);
        } finally {
            DatabricksSparkSideStagingArea.SINGLETON_INSTANCE.deleteSafely(testfileName);
        }
    }
}
