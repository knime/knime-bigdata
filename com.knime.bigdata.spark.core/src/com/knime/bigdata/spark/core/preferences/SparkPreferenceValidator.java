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
 *   Created on Jun 27, 2016 by Sascha Wolke
 */
package com.knime.bigdata.spark.core.preferences;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class SparkPreferenceValidator {

    /**
     * Validates given settings and returns null or error messages.
     */
    public static String validate(final String jobServerUrl,
            final boolean withAuthentication, final String username, final String password,
            final int jobTimeout, final int jobCheckFrequency,
            final String sparkVersion, final String contextName, final boolean deleteSparkObjectsOnDispose,
            final boolean overrideSettings, final String customSettings) {

        ArrayList<String> errors = new ArrayList<String>();

        // Job server URL
        if (jobServerUrl == null || jobServerUrl.isEmpty()) {
            errors.add("Job server url can't be empty.");

        } else {
            try {
                URI uri = new URI(jobServerUrl);

                if (uri.getScheme() == null || uri.getScheme().isEmpty()) {
                    errors.add("Protocol in job server URL required (http or https)");
                } else if (!(uri.getScheme().equalsIgnoreCase("http") || uri.getScheme().equalsIgnoreCase("https"))) {
                    errors.add("Only http and https are supported.");
                } else if (uri.getHost() == null || uri.getHost().isEmpty()) {
                    errors.add("Hostname in job server URL required.");
                } else if (uri.getPort() < 0) {
                    errors.add("Port in job server URL required.");
                }

            } catch(URISyntaxException e) {
                errors.add("Invalid job server url: " + e.getMessage());
            }
        }

        // Username
        if (withAuthentication && (username == null || username.isEmpty())) {
            errors.add("Username required with authentication enabled.");
        } else if (withAuthentication && username.startsWith(" ")) {
            errors.add("Unsupported leading space in username found." );
        } else if (withAuthentication && username.endsWith(" ")) {
            errors.add("Unsupported trailing space in username found.");
        }

        // Context name
        if (contextName == null || contextName.isEmpty()) {
            errors.add("Context name required.");
        } else if (!contextName.matches("^[A-Za-z].*")) {
            errors.add("Context name must start with letters.");
        } else if (!contextName.matches("^[A-Za-z0-9\\_\\-\\.]+$")) {
            errors.add("Invalid characters in context name found. Supported characters are: A-Z, 0-9, ., - and _.");
        }

        // Custom spark settings
        if (overrideSettings) {
            String lines[] = customSettings.split("\n");
            for (int i = 0; i < lines.length; i++) {
                if (!lines[i].isEmpty() && !lines[i].startsWith("#") && !lines[i].startsWith("//")) {
                    String kv[] = lines[i].split(": ", 2);

                    if (kv.length != 2 || kv[0].isEmpty() || kv[1].isEmpty()) {
                        errors.add("Failed to parse custom spark config line " + (i + 1) + ".");
                    }
                }
            }
        }


        if (errors.size() > 0) {
            StringBuilder sb = new StringBuilder();
            for (String error : errors) {
                sb.append(error).append(' ');
            }
            sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        } else {
            return null;
        }
    }
}
