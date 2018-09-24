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
 *   Created on May 4, 2018 by oole
 */
package org.knime.bigdata.spark.core.util.jarupload;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.EmptyJobInput;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

/**
 * This is a helper class, which simplifies checking for the presence of a jar.
 *
 * @author Ole Ostergaard, KNIME GmbH, Konstanz, Germany
 */
public class EnsureJarPresence {

    /** Job id for the check jar job **/
    public static String CHECK_JAR_JOB_ID = EnsureJarPresence.class.getCanonicalName() + ".check";

    /** Job id for the upload jar job **/
    public static String UPLOAD_JAR_JOB_ID = EnsureJarPresence.class.getCanonicalName() + ".upload";


    /**
     * Checks whether the classes, given the classnames are available on the Spark classpath, and uploads the
     * corresponding jars, should they be missing.
     *
     * @param sparkContextID Identifies the Spark context in which to ensure jar presence.
     * @param exec The KNIME execution monitor.
     * @param jarMap Maps class names (to check for), to jar files that contain the classes.
     * @throws KNIMESparkException when something goes wrong while ensuring jar presence.
     * @throws CanceledExecutionException when the execution was canceled by the user.
     */
    public static void checkSparkJars(final SparkContextID sparkContextID, final ExecutionMonitor exec,
        final Map<String, File> jarMap) throws KNIMESparkException, CanceledExecutionException {

        final CheckJarPresenceJobInput checkJarJobInput =
            new CheckJarPresenceJobInput(new ArrayList<>(jarMap.keySet()));

        exec.setMessage("Checking presence of necessary JAR files.");
        // check whether the classes for the jars are present on spark side.
        final CheckJarPresenceJobOutput checkJarJobOutput = SparkContextUtil.
                <CheckJarPresenceJobInput,CheckJarPresenceJobOutput>getJobRunFactory(sparkContextID, EnsureJarPresence.CHECK_JAR_JOB_ID)
                .createRun(checkJarJobInput).run(sparkContextID, exec);

        final ArrayList<File> filesToUpload = new ArrayList<>();
        for (String classOfMissingJar : checkJarJobOutput.getMissingJarClassList()) {
            filesToUpload.add(jarMap.get(classOfMissingJar));
        }

        if (!filesToUpload.isEmpty()) {
            exec.setMessage("Uploading missing JAR files to Spark.");
            // Upload the jars that are missing according to the classnames returned by the check jar job.
            SparkContextUtil.getJobWithFilesRunFactory(sparkContextID, UPLOAD_JAR_JOB_ID)
                .createRun(EmptyJobInput.getInstance(), filesToUpload).run(sparkContextID, exec);
        }
    }
}
