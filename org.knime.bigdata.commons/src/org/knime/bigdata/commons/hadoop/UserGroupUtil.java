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

import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.knime.bigdata.commons.config.CommonConfigContainer;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.util.KNIMERuntimeContext;

/**
 * Utility class to obtain Hadoop {@link UserGroupInformation} objects, through which we perform Kerberos logins, Hadoop
 * identities and Hadoop impersonation (proxy-user).
 *
 * @author Tobias Koetter, KNIME.com
 * @since 3.6
 */
public class UserGroupUtil {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(UserGroupUtil.class);

    /**
     * This method returns the {@link UserGroupInformation} of the OS user or the given userName with authentication
     * method simple. Use this method for unsecured Hadoop clusters.
     * To execute an operation use the {@link UserGroupInformation#doAs(java.security.PrivilegedAction)} method.
     *
     * @param conf {@link Configuration} to use
     * @param userName the name of the user that should be used or <code>null</code> if the
     * OS user should be used
     * @return the {@link UserGroupInformation} for the OS user or given userName with authentication method simple
     * @throws Exception
     */
    public static synchronized UserGroupInformation getUser(final Configuration conf, final String userName)
            throws Exception {

        LOGGER.debug("Retrieving user for simple authentication");
        UserGroupInformation.setConfiguration(conf);
        //The OS user should be a user with simple authentication
        final UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        //we do not care if the user is a kerberos user or not since we always create a proxy user that uses
        //simple authentication. So we shouldn't check otherwise the simple method will fail if a Kerberos ticket
        //is available but simple is selected in the node dialog
//        if (isKerberosUser(currentUser)) {
//            throw new Exception("Current user " + currentUser.getUserName()
//            + " is a Kerberos user with authentication method: " + getAuthMethodName(currentUser));
//        }
        LOGGER.debug("Current user: " + currentUser.toString());
        //use the OS user if the given userName is null or empty
        final String loginUser = userName != null && !userName.trim().isEmpty() ? userName : currentUser.getUserName();
        LOGGER.debug("Login user: " + loginUser);

        //this is the user that is returned to executed the operation with
        final UserGroupInformation user;
        if (currentUser.hasKerberosCredentials()) {
            //always create a new remote user if the currentUser is a Kerberos user even if their names are equal!!!
            LOGGER.debug("Creating remote user " + loginUser + " using Kerberos TGT user " + currentUser.toString());
            user = UserGroupInformation.createRemoteUser(loginUser);
        } else if (!currentUser.getUserName().equals(loginUser) && !currentUser.getShortUserName().equals(loginUser)) {
            //the real user differs from the login user so we have to impersonate it
            LOGGER.debug("Creating remote user " + loginUser + " using current user " + currentUser.getUserName());
            user = UserGroupInformation.createRemoteUser(loginUser);
        } else {
            LOGGER.debug("Using current user: " + currentUser.getUserName());
            //the real user is the same as the login user so we can simply use the real user
            user = currentUser;
        }
        LOGGER.debug("Returning simple authentication user: " + user.toString());
        return user;
    }

    /**
     * This method returns the {@link UserGroupInformation} of a user with a Kerberos TGT that could be used to
     * execute operations with. To execute an operation use the
     * {@link UserGroupInformation#doAs(java.security.PrivilegedAction)} method.
     * The method also checks if the workflow is executed on the server. If executed on the server the method always
     * returns the workflow user for security reasons since the server user itself is used to for impersonation.
     * Use this method for secured Hadoop cluster.
     * To execute an operation use the {@link UserGroupInformation#doAs(java.security.PrivilegedAction)} method.
     *
     * @param conf {@link Configuration} to use
     * @return the {@link UserGroupInformation} for the appropriate user. If the workflow is executed locally this
     * is the Kerberos user itself. If executed on the server the {@link UserGroupInformation} belongs
     * to the user in which name the workflow should be executed
     * @throws Exception if the Kerberos user can not be obtained
     */
    public static synchronized UserGroupInformation getKerberosUser(final Configuration conf)
            throws Exception {
        LOGGER.debug("Retrieving user for Kerberos authentication");
        //Get the user with the Kerberos TGT
        final UserGroupInformation kerberosTGTUser = getKerberosTGTUser(conf);

        final UserGroupInformation user;
        if (KNIMERuntimeContext.INSTANCE.runningInServerContext()) {
            //Always use the workflow user on the server in Kerberos mode because of security reasons!!!
            Optional<String> wfUser = NodeContext.getWorkflowUser();
            if (wfUser.isPresent() && !kerberosTGTUser.getUserName().equals(wfUser.get())
                    && !kerberosTGTUser.getShortUserName().equals(wfUser.get())) {
                LOGGER.debug("Creating proxy user for workflow user " + wfUser
                    + " using Kerberos TGT user " + kerberosTGTUser.getUserName() + " on server");
                //the Kerberos user differs from the workflow user so we have to impersonate it
                user = UserGroupInformation.createProxyUser(wfUser.get(), kerberosTGTUser);
            } else {
                LOGGER.debug("Using Kerberos user: " + kerberosTGTUser.getUserName() + " as login user on the server.");
                user = kerberosTGTUser;
            }
        } else {
            //use the Kerberos user as login user on the client
            LOGGER.debug("Using Kerberos user: " + kerberosTGTUser.getUserName() + " as login user on the client.");
            //the real user is the same as the login user so we can simply use the real user
            user = kerberosTGTUser;
        }
        LOGGER.debug("Returning Kerberos user: " + user.toString());
        return user;
    }

    /**
     * This method always returns a {@link UserGroupInformation} with the principal and credentials of a Kerberos TGT.
     * If the KNIME preferences are configured with a keytab file, a keytab login will be performed to obtain the TGT.
     * Otherwise it will try to obtain the TGT from a ticket cache.
     *
     * <p>
     * This method is only useful in rare cases, because it does NOT return a a proxy user UGI. Hence, on KNIME Server
     * this method will NOT impersonate the workflow user. On KNIME Server it returns the UGI with the TGT of the KNIME
     * Server principal. To seamlessly impersonate users on KNIME Server, Use {@link #getKerberosUser(Configuration)}.
     * </p>
     *
     * @param conf Hadoop {@link Configuration} to use. It must be set to the KERBEROS authentication method.
     * @return a UGI with the principal and credentials of a Kerberos TGT.
     * @throws Exception If Kerberos authentication was not enabled in the given configuration, or if the keytab login
     *             failed, or if no TGT could be obtained from a ticket cache.
     */
    public static synchronized UserGroupInformation getKerberosTGTUser(final Configuration conf) throws Exception {
        UserGroupInformation.setConfiguration(conf);
        if (!UserGroupInformation.isSecurityEnabled()) {
            throw new Exception("Kerberos authentication not enabled in configuration.");
        }
        final UserGroupInformation user;
        if (CommonConfigContainer.getInstance().hasKerberosKeytabConfig()) {
            final String keytabFile = CommonConfigContainer.getInstance().getKerberosKeytabConfig();
            final String keytabUser = CommonConfigContainer.getInstance().getKerberosUserConfig();
            LOGGER.debug(String.format("Determining Kerberos TGT user based on keytab settings (keytab file: %s / keytab user: %s).", keytabFile, keytabUser));
            UserGroupInformation.loginUserFromKeytab(keytabUser, keytabFile);
            user = UserGroupInformation.getLoginUser();
        } else {
            LOGGER.debug("Determining Kerberos TGT user based on ticket cache.");
            //we do not have any keytab information so we use ticket cache user instead.
            //We can not use the getCurrentUser() method since it will return the OS user if the authentication method
            //was simple before!!!
            user = UserGroupInformation.getUGIFromTicketCache(null, null);
        }
        if (!user.hasKerberosCredentials()) {
            throw new IllegalStateException("Retrieved Kerberos TGT user has no Kerberos information available."
                    + user.toString());
        }
        LOGGER.debug("Kerberos TGT user found: " + user.getUserName());
        return user;
    }
}
