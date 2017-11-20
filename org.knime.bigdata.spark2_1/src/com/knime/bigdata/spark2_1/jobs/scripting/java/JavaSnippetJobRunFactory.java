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
package com.knime.bigdata.spark2_1.jobs.scripting.java;

import com.knime.bigdata.spark.core.job.DefaultJobWithFilesRunFactory;
import com.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import com.knime.bigdata.spark.node.scripting.java.AbstractSparkJavaSnippetNodeModel;
import com.knime.bigdata.spark.node.scripting.java.util.JavaSnippetJobInput;
import com.knime.bigdata.spark.node.scripting.java.util.JavaSnippetJobOutput;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class JavaSnippetJobRunFactory extends DefaultJobWithFilesRunFactory<JavaSnippetJobInput, JavaSnippetJobOutput> {

    /**
     * Constructor.
     */
    public JavaSnippetJobRunFactory() {
        super(AbstractSparkJavaSnippetNodeModel.JOB_ID, JavaSnippetJob.class, JavaSnippetJobOutput.class,
            FileLifetime.CONTEXT, true);
    }
}
