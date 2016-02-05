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
 *   Created on Jan 26, 2016 by bjoern
 */
package com.knime.bigdata.spark.node.scripting.java.util;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.util.FileUtil;

import com.knime.bigdata.spark.jobserver.client.DataUploader;
import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.jar.JarPacker;
import com.knime.bigdata.spark.jobserver.jobs.SparkJavaSnippetJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkJavaSnippetTask implements Serializable {

    private static final long serialVersionUID = 4676466419417472047L;

    private final KNIMESparkContext context;

    private final SparkRDD inputRDD1;

    private final SparkRDD inputRDD2;

    private final Map<String, byte[]> bytecode;

    private final String snippetClass;

    private final String resultRDD;

    private SparkJavaSnippetJarCacheInfo jarCacheInfo;

    private HashMap<String, Object> flowVariableValues;

    /**
     * Creates a new task that handles the execution of a Java snippet in Spark.
     *
     * @param context The KNIMESparkContext from within to operate.
     * @param inputRDD1 First input RDD (can be null for source nodes).
     * @param inputRDD2 Second input RDD (must be null for sink and source nodes, can be null for inner nodes).
     * @param bytecode A map of class names to compiled byte code.
     * @param snippetClass The name of the class to run in Spark.
     * @param resultRDDName Name of the resulting RDD (must be null for sink nodes).
     * @param flowVariableValues Map of java field name to value.
     */
    public SparkJavaSnippetTask(final KNIMESparkContext context, final SparkRDD inputRDD1, final SparkRDD inputRDD2,
        final Map<String, byte[]> bytecode, final String snippetClass, final String resultRDDName,
        final SparkJavaSnippetJarCacheInfo cacheInfo, final HashMap<String, Object> flowVariableValues) {

        this.context = context;
        this.inputRDD1 = inputRDD1;
        this.inputRDD2 = inputRDD2;
        this.bytecode = bytecode;
        this.snippetClass = snippetClass;
        this.resultRDD = resultRDDName;
        this.flowVariableValues = flowVariableValues;

        if (cacheInfo == null) {
            jarCacheInfo =
                new SparkJavaSnippetJarCacheInfo(new HashMap<String, String>(), getLocalSnippetJarFileName());
        } else {
            jarCacheInfo = cacheInfo;
        }
    }

    private String params2Json() throws GenericKnimeSparkException {

        final List<String> inputParams = new LinkedList<>();

        if (inputRDD1 != null) {
            inputParams.add(SparkJavaSnippetJob.PARAM_INPUT_TABLE_KEY1);
            inputParams.add(inputRDD1.getID());
        }

        if (inputRDD2 != null) {
            inputParams.add(SparkJavaSnippetJob.PARAM_INPUT_TABLE_KEY2);
            inputParams.add(inputRDD2.getID());
        }

        inputParams.add(SparkJavaSnippetJob.PARAM_SNIPPET_CLASS);
        inputParams.add(snippetClass);

        inputParams.add(SparkJavaSnippetJob.PARAM_JAR_FILES_TO_ADD);
        inputParams.add(JsonUtils.toJsonArray(jarCacheInfo.getJarsUploaded().values().toArray()));

        inputParams.add(SparkJavaSnippetJob.PARAM_FLOW_VAR_VALUES);
        inputParams.add(JobConfig.encodeToBase64(flowVariableValues));

        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParams.toArray(new String[0]),
            ParameterConstants.PARAM_OUTPUT, new String[]{SparkJavaSnippetJob.PARAM_OUTPUT_TABLE_KEY, resultRDD}});
    }

    private String getLocalSnippetJarFileName() {
        return snippetClass + ".jar";
    }

    /**
     * Executes a previously compiled Java snippet as a job within Spark.
     *
     * @param exec
     * @return a job result
     * @throws GenericKnimeSparkException
     * @throws CanceledExecutionException
     */
    public JobResult execute(final ExecutionMonitor exec)
        throws GenericKnimeSparkException, CanceledExecutionException {

        jarCacheInfo.refreshCacheInfo(context, getLocalSnippetJarFileName());

        if (!jarCacheInfo.isSnippetJarUploaded()) {
            jarCacheInfo.addUploadedJar(jarCacheInfo.getLocalSnippetJarFileName(),
                uploadJarFile(createSnippetJarFile(), true));
        }

        String jsonConfig = params2Json();

        exec.checkCanceled();
        return JobControler.startJobAndWaitForResult(context, SparkJavaSnippetJob.class.getCanonicalName(), jsonConfig,
            exec);
    }

    private File createSnippetJarFile() throws GenericKnimeSparkException {
        final File snippetDir = new File(KNIMEConstants.getKNIMEHomeDir(), "sparkSnippetJars");
        if (!snippetDir.exists()) {
            snippetDir.mkdirs();
        }

        final File jarFile;
        try {
            jarFile = FileUtil.createTempFile(snippetClass, ".jar", snippetDir, true);
            JarPacker.createJar(jarFile, "", bytecode);
        } catch (IOException e) {
            throw new GenericKnimeSparkException("Failed to create snippet jar file: " + e.getMessage(), e);
        }

        return jarFile;
    }

    private String uploadJarFile(final File jarFile, final boolean deleteAfterUpload)
        throws GenericKnimeSparkException {
        String jarFilenameOnJobServer =
            DataUploader.uploadDataFile(context, jarFile.getAbsolutePath(), jarFile.getName());

        if (deleteAfterUpload) {
            jarFile.delete();
        }
        return jarFilenameOnJobServer;
    }

    /**
     * @return the jarCacheInfo
     */
    public SparkJavaSnippetJarCacheInfo getJarCacheInfo() {
        return jarCacheInfo;
    }
}
