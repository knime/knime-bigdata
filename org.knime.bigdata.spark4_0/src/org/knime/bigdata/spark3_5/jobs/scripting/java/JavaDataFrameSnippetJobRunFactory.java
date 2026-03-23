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
 *   Created on May 5, 2016 by bjoern
 */
package org.knime.bigdata.spark3_5.jobs.scripting.java;

import org.knime.bigdata.spark.core.job.DefaultJobWithFilesRunFactory;
import org.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import org.knime.bigdata.spark.node.scripting.java.AbstractSparkDataFrameJavaSnippetNodeModel;
import org.knime.bigdata.spark.node.scripting.java.util.JavaSnippetJobInput;
import org.knime.bigdata.spark.node.scripting.java.util.JavaSnippetJobOutput;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class JavaDataFrameSnippetJobRunFactory
    extends DefaultJobWithFilesRunFactory<JavaSnippetJobInput, JavaSnippetJobOutput> {

    /**
     * Constructor.
     */
    public JavaDataFrameSnippetJobRunFactory() {
        // once the jar files with snippets have been added to the Spark-side classloaders, they must stay
        // there until the context is destroyed. Hence we use FileLifetime.CONTEXT here
        super(AbstractSparkDataFrameJavaSnippetNodeModel.JOB_ID, JavaDataFrameSnippetJob.class,
            JavaSnippetJobOutput.class, FileLifetime.CONTEXT, true);
    }
}
