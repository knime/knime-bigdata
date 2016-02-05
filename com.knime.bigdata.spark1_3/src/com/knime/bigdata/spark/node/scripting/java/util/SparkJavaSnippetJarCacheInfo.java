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
 *   Created on Feb 4, 2016 by bjoern
 */
package com.knime.bigdata.spark.node.scripting.java.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.knime.bigdata.spark.jobserver.client.DataUploader;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkJavaSnippetJarCacheInfo {

    private final Map<String, String> jarsUploaded;

    private String localSnippetJarFileName;

    /**
     * @param jarsUploaded
     * @param localSnippetJarFileName
     */
    public SparkJavaSnippetJarCacheInfo(final Map<String, String> jarsUploaded, final String localSnippetJarFileName) {
        this.jarsUploaded = jarsUploaded;
        this.localSnippetJarFileName = localSnippetJarFileName;
    }

    /**
     * @return a list of absolute paths (in jobserver filesystem) of jar files that have been previously uploaded to the
     *         jobserver. This includes the jar file with the snippet class, as well as some jar files that have been
     *         added via the snippet dialog.
     */
    public Map<String, String> getJarsUploaded() {
        return jarsUploaded;
    }


    public boolean isSnippetJarUploaded() {
        return jarsUploaded.containsKey(localSnippetJarFileName);
    }

    public String getUploadedSnippetJarFilename() {
        return jarsUploaded.get(localSnippetJarFileName);
    }

    public boolean isJarUploaded(final String localJarFile) {
        return jarsUploaded.containsKey(localJarFile);
    }

    public String getUploadedJarFileName(final String localJarFile) {
        return jarsUploaded.get(localJarFile);
    }

    public void addUploadedJar(final String localJarFileName, final String uploadedJarFileName) {
        jarsUploaded.put(localJarFileName, uploadedJarFileName);
    }

    /**
     * @return the name of the snippet class
     */
    public String getLocalSnippetJarFileName() {
        return localSnippetJarFileName;
    }

    /**
     * Refreshes the map of uploaded jars by making sure that every entry in the map is still valid, i.e. the files are
     * present on the server.
     *
     * @param context
     * @param newSnippetJarName
     * @throws GenericKnimeSparkException
     */
    public void refreshCacheInfo(final KNIMESparkContext context, final String newSnippetJarName) throws GenericKnimeSparkException {

        if (!newSnippetJarName.equals(localSnippetJarFileName)) {
            jarsUploaded.remove(localSnippetJarFileName);
            localSnippetJarFileName = newSnippetJarName;
        }

        if (jarsUploaded.isEmpty()) {
            return;
        }

        Set<String> dataFilesOnServer = new HashSet<String>(Arrays.asList(DataUploader.listFiles(context)));

        Iterator<Entry<String, String>> iter = jarsUploaded.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, String> next = iter.next();

            if (!dataFilesOnServer.contains(next.getValue())) {
                iter.remove();
            }
        }
    }
}
