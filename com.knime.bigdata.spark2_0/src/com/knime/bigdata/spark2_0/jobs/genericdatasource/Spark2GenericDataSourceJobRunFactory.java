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
 *   Created on Aug 11, 2016 by sascha
 */
package com.knime.bigdata.spark2_0.jobs.genericdatasource;

import java.io.File;
import java.util.List;

import com.knime.bigdata.spark.core.jar.bundle.BundleGroupSparkJarRegistry;
import com.knime.bigdata.spark.core.job.DefaultJobWithFilesRun;
import com.knime.bigdata.spark.core.job.DefaultJobWithFilesRunFactory;
import com.knime.bigdata.spark.core.job.EmptyJobOutput;
import com.knime.bigdata.spark.core.job.JobWithFilesRun;
import com.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import com.knime.bigdata.spark.core.version.SparkVersion;
import com.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceJobInput;
import com.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeModel;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2GenericDataSourceJobRunFactory extends DefaultJobWithFilesRunFactory<Spark2GenericDataSourceJobInput, EmptyJobOutput> {


    /** Constructor */
    public Spark2GenericDataSourceJobRunFactory() {
        super(Spark2GenericDataSourceNodeModel.JOB_ID);
    }

    @Override
    public JobWithFilesRun<Spark2GenericDataSourceJobInput, EmptyJobOutput> createRun(final Spark2GenericDataSourceJobInput input,
            final List<File> localFiles) {

        if (input.uploadDriver()) {
            localFiles.addAll(BundleGroupSparkJarRegistry.getBundledDriverJars(SparkVersion.V_2_0, input.getFormat()));
        }

        return new DefaultJobWithFilesRun<>(input, Spark2GenericDataSourceJob.class, EmptyJobOutput.class,
                                            localFiles, FileLifetime.CONTEXT, true);
    }
}
