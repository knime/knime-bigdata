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
 *   Created on 24.06.2016 by koetter
 */
package com.knime.bigdata.commons.security.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.WorkflowContext;
import org.knime.core.node.workflow.WorkflowManager;

import com.knime.bigdata.commons.config.CommonConfigContainer;
import com.knime.licenses.License;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class UserGroupUtil {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(UserGroupUtil.class);

    /**
     * @return the user id of the user the workflow should be executed with
     */
    private static String getWFUser() {
        LOGGER.debug("Retrieving workflow user");
        //return only a user if the workflow runs on the server
        final NodeContext context = NodeContext.getContext();
        if (context != null) {
            final WorkflowManager workflowManager = context.getWorkflowManager();
            if (workflowManager != null) {
                final WorkflowContext workflowContext = workflowManager.getContext();
                if (workflowContext != null) {
                    final String userid = workflowContext.getUserid();
                    if (userid != null && !userid.isEmpty()) {
                        LOGGER.debug("Workflow user found: " + userid);
                        return userid;
                    } else {LOGGER.warn("Workflow user id not available");}
                } else {LOGGER.warn("Workflow context not available");}
            } else {LOGGER.warn("Workflow manager not available");}
        } else {LOGGER.warn("Node context not available");}
        return null;
    }


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
            LOGGER.debug("Creating remote user " + loginUser + " using Kerberos user " + currentUser.toString());
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
        LOGGER.debug("Retrieving Kerberos user");
        UserGroupInformation.setConfiguration(conf);
        //Get the user with the Kerberos TGT
        final UserGroupInformation kerberosUser = getKerberosUser();
        final UserGroupInformation user;
        if (License.runningInServerContext()) {
            //Always use the workflow user on the server in Kerberos mode because of security reasons!!!
            final String wfUser = getWFUser();
            if (wfUser != null && !kerberosUser.getUserName().equals(wfUser)
                    && !kerberosUser.getShortUserName().equals(wfUser)) {
                LOGGER.debug("Creating proxy user for workflow user " + wfUser
                    + " using Kerberos user " + kerberosUser.getUserName() + " on server");
                //the Kerberos user differs from the workflow user so we have to impersonate it
                user = UserGroupInformation.createProxyUser(wfUser, kerberosUser);
            } else {
                LOGGER.debug("Using Kerberos user: " + kerberosUser.getUserName() + " as login user on the server.");
                user = kerberosUser;
            }
        } else {
            //use the Kerberos user as login user on the client
            LOGGER.debug("Using Kerberos user: " + kerberosUser.getUserName() + " as login user on the client.");
            //the real user is the same as the login user so we can simply use the real user
            user = kerberosUser;
        }
        LOGGER.debug("Returning Kerberos user: " + user.toString());
        return user;
    }

    /**
     * This method returns the Kerberos user which can be used directly or used for proxy user creation.
     * If the user has specified a keytab file in the KNIME preferences the method returns the Kerberos user from
     * the keytab file otherwise it looks in the ticket cache for a user.
     * @return the Kerberos user
     * @throws Exception if no Kerberos user could be obtained
     */
    private static UserGroupInformation getKerberosUser() throws Exception {
        if (!UserGroupInformation.isSecurityEnabled()) {
            throw new Exception("Kerberos authentication not enabled in configuration.");
        }
        final UserGroupInformation user;
        if (CommonConfigContainer.getInstance().hasKerberosKeytabConfig()) {
            final String keytabFile = CommonConfigContainer.getInstance().getKerberosKeytabConfig();
            final String keytabUser = CommonConfigContainer.getInstance().getKerberosUserConfig();
            LOGGER.debug("Using keytab file " + keytabFile + " to get keytab user " + keytabUser);
            UserGroupInformation.loginUserFromKeytab(keytabUser, keytabFile);
            user = UserGroupInformation.getLoginUser();
        } else {
            LOGGER.debug("Retrieving Kerberos user from ticket cache.");
            //we do not have any keytab information so we use ticket cache user instead.
            //We can not use the getCurrentUser() method since it will return the OS user if the authentication method
            //was simple before!!!
            user = UserGroupInformation.getUGIFromTicketCache(null, null);
        }
        if (!user.hasKerberosCredentials()) {
            throw new IllegalStateException("Retrieved Kerberos user has no Kerberos information available."
                    + user.toString());
        }
        LOGGER.debug("Kerberos user found: " + user.getUserName());
        return user;
    }
}
