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
 *   Created on May 5, 2016 by bjoern
 */
package com.knime.bigdata.spark2_0.jobs.scripting.java;

import java.io.File;
import java.util.List;

import com.knime.bigdata.spark.core.job.DefaultJobWithFilesRun;
import com.knime.bigdata.spark.core.job.DefaultJobWithFilesRunFactory;
import com.knime.bigdata.spark.core.job.JobWithFilesRun;
import com.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import com.knime.bigdata.spark.node.scripting.java.AbstractSparkDataFrameJavaSnippetNodeModel;
import com.knime.bigdata.spark.node.scripting.java.util.JavaSnippetJobInput;
import com.knime.bigdata.spark.node.scripting.java.util.JavaSnippetJobOutput;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class JavaDataFrameSnippetJobRunFactory extends DefaultJobWithFilesRunFactory<JavaSnippetJobInput, JavaSnippetJobOutput> {

    /**
     * Constructor
     */
    public JavaDataFrameSnippetJobRunFactory() {
        super(AbstractSparkDataFrameJavaSnippetNodeModel.JOB_ID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobWithFilesRun<JavaSnippetJobInput, JavaSnippetJobOutput> createRun(final JavaSnippetJobInput input,
        final List<File> localFiles) {

        // once the jar files with snippets have been added to the Spark-side classloaders, they must stay
        // there until the context is destroyed. Hence we use FileLifetime.CONTEXT here
        return new DefaultJobWithFilesRun<JavaSnippetJobInput, JavaSnippetJobOutput>(input, JavaDataFrameSnippetJob.class,
            JavaSnippetJobOutput.class, localFiles, FileLifetime.CONTEXT, true);
    }
}
