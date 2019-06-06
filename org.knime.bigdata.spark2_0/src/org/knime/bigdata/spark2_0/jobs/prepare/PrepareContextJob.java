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
package org.knime.bigdata.spark2_0.jobs.prepare;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.DataType;
import org.knime.bigdata.spark.core.context.util.PrepareContextJobInput;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.jar.JobJarDescriptor;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark2_0.api.NamedObjects;
import org.knime.bigdata.spark2_0.api.SimpleSparkJob;
import org.knime.bigdata.spark2_0.api.TypeConverters;
import org.knime.bigdata.spark2_0.base.Spark_2_0_CustomUDFProvider;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class PrepareContextJob implements SimpleSparkJob<PrepareContextJobInput> {

    private final static Logger LOGGER = Logger.getLogger(PrepareContextJob.class.getName());

    private static final long serialVersionUID = 5767134504557370285L;

    /**
     * SparkConf key that activates the KNIME workaround for SPARK-18627 (Cannot connect to Hive metastore in client
     * mode with proxy user).
     */
    private final static String ACTIVATE_METASTORE_TOKEN_PATCH = "spark.knime.metastoreTokenPatch";

    /**
     * {@inheritDoc}
     */
    @Override
    public void runJob(final SparkContext sparkContext, final PrepareContextJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        try {
            JobJarDescriptor jobJarInfo =
                JobJarDescriptor.load(this.getClass().getClassLoader().getResourceAsStream(JobJarDescriptor.FILE_NAME));

            if (!sparkContext.version().startsWith(input.getSparkVersion())) {
                throw new KNIMESparkException(String.format(
                    "Spark version mismatch: KNIME Extension for Apache Spark is set to %s, but the cluster runs %s. Please correct the setting under Preferences > KNIME > Spark. Then destroy and reopen the Spark context.",
                    input.getSparkVersion(), sparkContext.version()));
            }

            if (!input.getKNIMEPluginVersion().equals(jobJarInfo.getPluginVersion())) {
                throw new KNIMESparkException(String.format(
                    "Spark context was created by version %s of the KNIME Extension for Apache Spark, but you are running %s. Please destroy and reopen this Spark context or use a different one.",
                    jobJarInfo.getPluginVersion(), input.getKNIMEPluginVersion()));
            }

            // FIXME Deactivated hash check, as this was causing trouble with win+lin on the same context.
            //            if (!input.getJobJarHash().equals(jobJarInfo.getHash())) {
            //                throw new KNIMESparkException(
            //                    "Spark context was created by a KNIME Extension for Apache Spark that has incompatible community extensions. Please destroy and reopen this Spark context or use a different one.");
            //            }
        } catch (IOException e) {
            throw new KNIMESparkException("Spark context was probably not created with KNIME Extension for Apache Spark (or an old version of it).  Please destroy and reopen this Spark context or use a different one.",
                e);
        }

        // Hive Metastore token patch due to SPARK-18627
        final String master = sparkContext.getConf().get("spark.master");
        final String deployMode = sparkContext.getConf().get("spark.submit.deployMode");
        if (sparkContext.getConf().getBoolean(ACTIVATE_METASTORE_TOKEN_PATCH, true)
                && master.equals("yarn")
                && deployMode.equals("client")) {
            try {
                monkeyPatchMetastoreToken(sparkContext);
            } catch (Exception e) {
                LOGGER.error(String.format("Failed to activate Hive Metastore token patch: %s (%s)", e.getMessage(),
                    e.getClass().getName()), e);
            }
        }

        TypeConverters.ensureConvertersInitialized(input.<DataType>getTypeConverters());
        Spark_2_0_CustomUDFProvider.registerCustomUDFs(sparkContext);
    }

    private void monkeyPatchMetastoreToken(final SparkContext sparkContext)
            throws ReflectiveOperationException, SecurityException, IOException {

            final Credentials credentials = getCredentialsCache(sparkContext);
            final Text properTokenId = new Text("HIVE_DELEGATION_TOKEN");
            final Text wrongTokenId = new Text("hive.server2.delegation.token");

            final Token<?> metastoreToken = tryToFindMetastoreToken(credentials, properTokenId, wrongTokenId);

            if (metastoreToken != null) {
                LOGGER.info("Found delegation token for Hive Metastore.");

                if (credentials.getToken(properTokenId) == null) {
                    LOGGER.info("Registering metastore token in client credentials (workaround for SPARK-18627)");
                    credentials.addToken(properTokenId, metastoreToken);
                } else {
                    LOGGER.info("Metastore token already in client credentials under proper ID (doing nothing)");
                }

                final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                boolean foundInUGI = false;
                for (Token<?> token : ugi.getTokens()) {
                    if (token.getKind().equals(properTokenId)) {
                        foundInUGI = true;
                        break;
                    }
                }

                if (!foundInUGI) {
                    LOGGER.info("Registering metastore token in UGI (workaround for SPARK-18627)");
                    ugi.addToken(new Text("HIVE_DELEGATION_TOKEN"), metastoreToken);
                } else {
                    LOGGER.info("Metastore token already in UGI (doing nothing)");
                }
            } else {
                LOGGER.info("No delegation token for Hive Metastore found.");
            }
        }

        private Token<?> tryToFindMetastoreToken(final Credentials credentials, final Text properTokenId,
            final Text wrongTokenId) {

            Token<?> hiveMetastoreToken = credentials.getToken(wrongTokenId);

            hiveMetastoreToken = (hiveMetastoreToken == null) ? credentials.getToken(properTokenId) : hiveMetastoreToken;

            if (hiveMetastoreToken == null) {
                for (Token<?> token : credentials.getAllTokens()) {
                    if (token.getKind() != null && token.getKind().equals(properTokenId)) {
                        hiveMetastoreToken = token;
                        break;
                    }
                }
            }
            return hiveMetastoreToken;
        }

        private Credentials getCredentialsCache(final SparkContext sparkContext)
            throws IllegalArgumentException, ReflectiveOperationException {

            final Object schedulerBackend = getScalaFieldValue(SparkContext.class, sparkContext, "_schedulerBackend");
            final Object client = getScalaFieldValue(schedulerBackend.getClass(), schedulerBackend, "client");
            return  (Credentials) getScalaFieldValue(client.getClass(), client, "credentials");
        }

        private Object getScalaFieldValue(final Class<?> clazz, final Object scalaObj, final String fieldName) throws IllegalArgumentException, ReflectiveOperationException {
            // first we try to find an actual field
            ArrayList<Field> fields = new ArrayList<Field>();
            fields.addAll(Arrays.asList(clazz.getFields()));
            fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
            for(Field field : fields) {
                LOGGER.debug("Field name: " + field.getName() + " // Looking for: " + fieldName);
                if (field.getName().equals(fieldName) || field.getName().endsWith("$$" + fieldName)) {
                    field.setAccessible(true);
                    return field.get(scalaObj);
                }
            }

            // okay, no matching fields found. Trying getter-style methods
            ArrayList<Method> methods = new ArrayList<Method>();
            methods.addAll(Arrays.asList(clazz.getMethods()));
            methods.addAll(Arrays.asList(clazz.getDeclaredMethods()));
            for (Method method : methods) {
                if (method.getName().equals(fieldName) || method.getName().endsWith("$$" + fieldName)) {
                    method.setAccessible(true);
                    return method.invoke(scalaObj);
                }
            }

            throw new ReflectiveOperationException("No field or method found with name " + fieldName);
        }

}
