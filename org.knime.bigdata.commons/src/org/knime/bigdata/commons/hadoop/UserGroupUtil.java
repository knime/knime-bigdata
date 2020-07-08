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
 *   Created on 24.06.2016 by koetter
 */
package org.knime.bigdata.commons.hadoop;

import java.security.AccessController;
import java.util.Optional;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.knime.bigdata.commons.config.CommonConfigContainer;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.NodeLogger;
import org.knime.kerberos.api.KerberosProvider;

/**
 * Utility class to perform operations with Hadoop {@link UserGroupInformation} objects, that capture perform Kerberos
 * logins, Hadoop identities and Hadoop impersonation (proxy-user).
 *
 * <p>
 * NOTE: In KNIME 3.7 and earlier, the methods of this class used the Hadoop library to perform Kerberos logins. In KNIME 3.8
 * and higher this is no longer the case, as the Kerberos login is delegated to the KNIME Kerberos authentication
 * framework.
 * </p>
 *
 * @author Tobias Koetter, KNIME.com
 * @author Bjoern Lohrmann, KNIME GmbH
 * @since 3.6
 */
public class UserGroupUtil {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(UserGroupUtil.class);

    /**
     * Sets up the global Hadoop configuration according to preferences and sets a basic UGI. This method must only be
     * called during KNIME startup, when KNIME perferences are being changed, or when the global Hadoop configuration
     * needs to be "upgraded" from SIMPLE auth to KERBEROS auth. You should never have to call this method directly when
     * writing nodes.
     *
     * @param initSecureConfiguration Whether to init a Hadoop configuration that requires Kerberos, or not.
     */
    public static synchronized void initHadoopConfigurationAndUGI(final boolean initSecureConfiguration) {
        UserGroupInformation.reset();

        if (initSecureConfiguration) {
            UserGroupInformation.setConfiguration(ConfigurationFactory.createBaseConfigurationWithKerberosAuth());
        } else {
            UserGroupInformation.setConfiguration(ConfigurationFactory.createBaseConfigurationWithSimpleAuth());
        }
        UserGroupInformation
            .setLoginUser(UserGroupInformation.createRemoteUser(getOSUserName()));
    }

    private static String getOSUserName() {
        String osUserName = System.getProperty("user.name");

        // in the build system this System.getProperty("user.name") strangely returns an empty string.
        if (osUserName == null || osUserName.isEmpty()) {
            osUserName = "knime";
        }

        return osUserName;
    }

    private static void ensureSecureHadoopConfiguration() {
        if (!UserGroupInformation.isSecurityEnabled()) {
            initHadoopConfigurationAndUGI(true);
        }
    }

    /**
     * Callback interface for operations that require a Hadoop {@link UserGroupInformation} object.
     *
     * @author Bjoern Lohrmann, KNIME GmbH
     * @param <T> The result type of the operation.
     */
    @FunctionalInterface
    public static interface UserGroupInformationCallback<T> {
        /**
         * Performs an operation that requires a Hadoop {@link UserGroupInformation}.
         *
         * @param ugi a Hadoop {@link UserGroupInformation} that captures the Hadoop user to run the operation with.
         * @return the result value of the operation.
         * @throws Exception When something went wrong while performing the operation.
         */
        public T runWithUGI(final UserGroupInformation ugi) throws Exception;
    }

    /**
     * This method runs the given callback with a {@link UserGroupInformation} that wraps the given user with SIMPLE
     * authentication (no Kerberos!). This method does not perform any impersonation on KNIME Server.
     *
     * @param username The user to run with given callback with.
     * @param callback The callback to execute.
     * @return the return value of the given callback.
     * @throws Exception when the given callback threw an exception.
     */
    public static <T> T runWithRemoteUserUGI(final String username,
        final UserGroupInformationCallback<T> callback) throws Exception {
        return callback.runWithUGI(UserGroupInformation.createRemoteUser(username));
    }

    /**
     * This method runs the given callback with a {@link UserGroupInformation} that wraps a Kerberos TGT and possibly a
     * Hadoop proxy-user on top. Specifically, if we are running on KNIME server and user impersonation is enabled, then
     * this will impersonate the current workflow user, by means of Hadoop impersonation ("proxy user").
     *
     * <p>
     * NOTE: This method does not set any Hadoop {@link Configuration}. It is up to the caller to ensure that a correct
     * {@link Configuration} is being used.
     * </p>
     *
     * <p>
     * This method uses the KNIME Kerberos authentication framework to handle the Kerberos login.
     * </p>
     *
     * @param callback The callback to execute.
     * @return the return value of the given callback.
     * @throws CanceledExecutionException If the callback execution has been cancelled using the given by interrupting
     *             the current thread.
     * @throws LoginException, when Kerberos authentication is not done with keytab but the user is not already logged
     *             in.
     * @throws Exception when the given callback threw an exception.
     * @see CommonConfigContainer#getUserToImpersonate()
     */
    public static <T> T runWithProxyUserUGIIfNecessary(final UserGroupInformationCallback<T> callback)
        throws Exception {

        return KerberosProvider.doWithKerberosAuthBlocking(() -> {
            UserGroupInformation ugi =
                UserGroupInformation.getUGIFromSubject(Subject.getSubject(AccessController.getContext()));

            final Optional<String> userToImpersonate = CommonConfigContainer.getInstance().getUserToImpersonate();
            if (userToImpersonate.isPresent()) {
                if (!ugi.getUserName().equals(userToImpersonate.get())
                    && !ugi.getShortUserName().equals(userToImpersonate.get())) {
                    // the Kerberos user differs from the workflow user so we have to impersonate it
                    ugi = UserGroupInformation.createProxyUser(userToImpersonate.get(), ugi);
                    LOGGER.debug("Impersonating workflow user " + userToImpersonate.get());
                } else {
                    LOGGER.debug("Not impersonating workflow user, as it is the same as the Kerberos TGT user.");
                }
            }

            ensureSecureHadoopConfiguration();
            return callback.runWithUGI(ugi);
        }, null);
    }

    /**
     * This method runs the given callback with a {@link UserGroupInformation} that wraps Kerberos TGT credentials.
     *
     * <p>
     * This method is only useful in rare cases, because it does NOT perform Hadoop impersonation ("proxy user"). Hence,
     * on KNIME Server this method will NOT impersonate the workflow user. On KNIME Server it executes the callback with
     * Kerberos principal of KNIME Server. To seamlessly impersonate users on KNIME Server, use
     * {@link #runWithProxyUserUGIIfNecessary(UserGroupInformationCallback)}.
     * </p>
     *
     * <p>
     * This method uses the KNIME Kerberos authentication framework to handle the Kerberos login.
     * </p>
     *
     * @param callback The callback to execute.
     * @return the return value of the given callback.
     * @throws CanceledExecutionException If the callback execution has been cancelled using the given by interrupting
     *             the current thread.
     * @throws LoginException, when Kerberos authentication is not done with keytab but the user is not already logged
     *             in.
     * @throws Exception when the given callback threw an exception.
     * @see #runWithProxyUserUGIIfNecessary(UserGroupInformationCallback)
     */
    public static <T> T runWithKerberosUGI(final UserGroupInformationCallback<T> callback) throws Exception {

        return KerberosProvider.doWithKerberosAuthBlocking(() -> {
            final UserGroupInformation ugi =
                UserGroupInformation.getUGIFromSubject(Subject.getSubject(AccessController.getContext()));

            if (!ugi.hasKerberosCredentials()) {
                throw new IllegalStateException("Hadoop UGI has no Kerberos TGT for " + ugi.toString());
            }
            LOGGER.debug("Kerberos TGT user found: " + ugi.getUserName());
            ensureSecureHadoopConfiguration();
            return callback.runWithUGI(ugi);
        }, null);
    }
}
