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
import org.knime.kerberos.config.KerberosConfigContainer;

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
        //flag that defines if this code is executed in the KNIME client or on the KNIME server
        final UserGroupInformation realUser;
        UserGroupInformation.setConfiguration(conf);
        final String wfUser = getWFUser();
        final String loginUser;
        if (wfUser != null && !wfUser.isEmpty()) {
            //we should do this on the server to get the kerberos tgt via the keytab
            //when the workflow is executed on the server always use the user id from the workflow context!
            if (KerberosConfigContainer.getInstance().hasKeytabFile()) {
                String keytabUser = KerberosConfigContainer.getInstance().getKeytabUser();
                LOGGER.debug("Using keytab file to create user: " + keytabUser);
                realUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(keytabUser,
                    KerberosConfigContainer.getInstance().getKeytabFile().getAbsolutePath());
            } else {
                throw new Exception("No keytab information available in preferences.");
            }
            loginUser = wfUser;
        } else {
            LOGGER.debug("Workflow runs on client getting current user.");
            realUser = UserGroupInformation.getCurrentUser();
            //this is equivalent to the getCurrentUser method!!!
//          realUser = UserGroupInformation.getBestUGI(null, null);
            LOGGER.debug("Current user: " + realUser.getUserName());
            //use the OS user if the given userName is null or empty
            loginUser = userName != null && userName.trim().isEmpty() ? userName : realUser.getUserName();
        }
        final UserGroupInformation user;
        if (!realUser.getUserName().equals(loginUser) && !realUser.getShortUserName().equals(loginUser)) {
            LOGGER.debug("Real user: " + realUser.getUserName() + " different from login user: " + loginUser);
            //the real user differs from the login user so we have to impersonate it
            if (AuthenticationMethod.SIMPLE.equals(realUser.getAuthenticationMethod())) {
                LOGGER.debug("Creating remote user " + loginUser + " using real user "
            + realUser.getShortUserName() + " for simple connections");
                user = UserGroupInformation.createRemoteUser(loginUser);
            } else {
                LOGGER.debug("Creating proxy user " + loginUser + " using real user "
            + realUser.getShortUserName() + " for secured connections");
                user = UserGroupInformation.createProxyUser(loginUser, realUser);
            }
        } else {
            LOGGER.debug("Using real user: " + realUser.getUserName());
            //the real user is the same as the login user so we can simply use the real user
            user = realUser;
        }
        LOGGER.debug("Using Keberos user: " + user.toString());
        return user;
    }
}
