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
 *   Created on 27.04.2016 by koetter
 */
package com.knime.bigdata.spark1_6.base;

import java.util.LinkedList;
import java.util.List;

import com.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import com.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import com.knime.bigdata.spark.core.job.DefaultJobWithFilesRunFactory;
import com.knime.bigdata.spark.core.job.DefaultSimpleJobRunFactory;
import com.knime.bigdata.spark.core.job.JobRunFactory;
import com.knime.bigdata.spark1_6_cdh5_9.api.Spark_1_6_CDH5_9_CompatibilityChecker;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark_1_6_CDH5_9_JobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /**
     * Constructor.
     */
    public Spark_1_6_CDH5_9_JobRunFactoryProvider() {
        super(Spark_1_6_CDH5_9_CompatibilityChecker.INSTANCE, copyJobRunFactoriesFromSpark1_6());
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
    private static JobRunFactory<?, ?>[] copyJobRunFactoriesFromSpark1_6() {
        final Spark_1_6_JobRunFactoryProvider spark16Provider = new Spark_1_6_JobRunFactoryProvider();

        final List<JobRunFactory<?, ?>> ret = new LinkedList<>();

        final ClassLoader cdh59ClassLoader = Spark_1_6_CDH5_9_CompatibilityChecker.class.getClassLoader();

        for (JobRunFactory<?, ?> orig : spark16Provider.get()) {
            final JobRunFactory<?, ?> clone;

            if (orig instanceof DefaultJobWithFilesRunFactory) {
                final DefaultJobWithFilesRunFactory origCasted = (DefaultJobWithFilesRunFactory) orig;

                clone = new DefaultJobWithFilesRunFactory(origCasted.getJobID(),
                    origCasted.getJobClass(),
                    origCasted.getJobOutputClass(),
                    cdh59ClassLoader,
                    origCasted.getFilesLifetime(),
                    origCasted.useInputFileCopyCache());
            } else if (orig instanceof DefaultSimpleJobRunFactory) {
                clone = orig;
            } else if (orig instanceof DefaultJobRunFactory) {
                final DefaultJobRunFactory origCasted = (DefaultJobRunFactory) orig;
                clone = new DefaultJobRunFactory(origCasted.getJobID(),
                    origCasted.getJobClass(),
                    origCasted.getJobOutputClass(),
                    cdh59ClassLoader);
            } else {
                throw new RuntimeException("Unknown job run run factory type: " + orig.getClass().getName());
            }

            ret.add(clone);
        }

        return ret.toArray(new JobRunFactory<?, ?>[ret.size()]);
    }
}
