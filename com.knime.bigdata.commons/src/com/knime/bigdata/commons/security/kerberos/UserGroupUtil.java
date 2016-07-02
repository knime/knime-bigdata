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
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
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
     * @return the user id of the user the workflow should be executed with if the workflow is executed on the server
     * otherwise the method returns <code>null</code>
     */
    public static String getWFUser() {
        if (License.runningInServerContext()) {
            LOGGER.debug("Workflow is running on server. Retrieving workflow user.");
            //return only a user if the workflow runs on the server
            final NodeContext context = NodeContext.getContext();
            if (context != null) {
                final WorkflowManager workflowManager = context.getWorkflowManager();
                if (workflowManager != null) {
                    LOGGER.debug("Workflow manager available");
                    final WorkflowContext workflowContext = workflowManager.getContext();
                    if (workflowContext != null) {
                        LOGGER.debug("Workflow context available");
                        final String userid = workflowContext.getUserid();
                        if (userid != null && !userid.isEmpty()) {
                            LOGGER.debug("Workflow user id: " + userid);
                            return userid;
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * This method returns the {@link UserGroupInformation} object that should be used to execute the operation with.
     * To execute the operation use the {@link UserGroupInformation#doAs(java.security.PrivilegedAction)} method.
     * The method also checks if the workflow is executed on the server and ensures that the workflow user is returned
     * and that the server super user has a valid Kerberos ticket that is used for impersonation.
     *
     * @param conf {@link Configuration} to use
     * @param userName the name of the user that should be used in client mode or <code>null</code> if the
     * os user should be user
     * @return the {@link UserGroupInformation} for the appropriate user. If the workflow is executed locally this
     * is the same as the given userName parameter. If executed on the server the {@link UserGroupInformation} belongs
     * to the user in which name the workflow should be executed
     * @throws Exception
     */
    public static synchronized UserGroupInformation getUser(final Configuration conf, final String userName)
            throws Exception {
        //this user is the user with the Kerberos ticket. On the server this is the super user that is used
        //to impersonate other users. On the client this is the user with the Kerberos TGT ticket.
        final UserGroupInformation kerberosUser;
        UserGroupInformation.setConfiguration(conf);
        //On the client the wfUser is null. On the server the user that has itself authenticated against
        //the server and in whose context the job must run.
        final String wfUser = getWFUser();
        //the user in which name the operation should be executed
        final String loginUser;
        if (wfUser != null && !wfUser.isEmpty()) {
            //we should do this on the server to get the kerberos tgt via the keytab
            //when the workflow is executed on the server always use the user id from the workflow context because of
            //security reasons!
            LOGGER.debug("Workflow user: " + wfUser + " found execute in server mode");
            if (CommonConfigContainer.getInstance().hasKerberosKeytabConfig()) {
                final String keytabFile = CommonConfigContainer.getInstance().getKerberosKeytabConfig();
                final String keytabUser = CommonConfigContainer.getInstance().getKerberosUserConfig();
                LOGGER.debug("Using keytab file " + keytabFile + " to create Kerberos user: " + keytabUser);
                kerberosUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(keytabUser, keytabFile);
            } else {
                throw new Exception("No keytab information available in preferences.");
            }
            //always use the wf user because of security reasons  never use the given user name!!!
            loginUser = wfUser;
            if (userName != null && !userName.equals(wfUser)) {
                LOGGER.warn("Workflow user: " + wfUser + " different from login user: " + userName);
            }
        } else {
            LOGGER.debug("Workflow runs on client getting current Kerberos/OS user.");
            kerberosUser = UserGroupInformation.getCurrentUser();
            //UserGroupInformation.getBestUGI(null, null); is equivalent to the getCurrentUser method!!!
            LOGGER.debug("Kerberos user: " + kerberosUser.getUserName());
            //use the Kerberos/OS user if the given userName is null or empty
            loginUser = userName != null && !userName.trim().isEmpty() ? userName : kerberosUser.getUserName();
        }
        //this is the user that is returned who is used to executed the opertation
        final UserGroupInformation user;
        if (!kerberosUser.getUserName().equals(loginUser) && !kerberosUser.getShortUserName().equals(loginUser)) {
            LOGGER.debug("Kerberos user: " + kerberosUser.getUserName() + " different from login user: " + loginUser);
            //the real user differs from the login user so we have to impersonate it
            if (AuthenticationMethod.SIMPLE.equals(kerberosUser.getAuthenticationMethod())) {
                LOGGER.debug("Creating remote user " + loginUser + " using real user "
                        + kerberosUser.getShortUserName() + " for simple connections");
                user = UserGroupInformation.createRemoteUser(loginUser);
            } else {
                LOGGER.debug("Creating proxy user " + loginUser + " using real user "
                        + kerberosUser.getShortUserName() + " for secured connections");
                user = UserGroupInformation.createProxyUser(loginUser, kerberosUser);
            }
        } else {
            LOGGER.debug("Using Kerberos user: " + kerberosUser.getUserName());
            //the real user is the same as the login user so we can simply use the real user
            user = kerberosUser;
        }
        LOGGER.debug("Using Keberos user: " + user.toString());
        return user;
    }
}
