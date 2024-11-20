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
 *   Created on Jun 27, 2016 by Sascha Wolke
 */
package org.knime.bigdata.spark.core.preferences;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class that validates the Spark preferences.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SparkPreferenceValidator {

    public static void validateUsernameAndPassword(final String username,
        final String password, final List<String> errors) {

        if (username == null || username.isEmpty()) {
            errors.add("Username required with authentication enabled.");
        } else if (username.startsWith(" ")) {
            errors.add("Unsupported leading space in username found.");
        } else if (username.endsWith(" ")) {
            errors.add("Unsupported trailing space in username found.");
        }
    }

    /**
     * Validates the given URL for a REST endpoint (e.g. Spark Jobserver, Apache Livy, etc).
     *
     * @param endpointUrl The user-specified URL as a string.
     * @param errors A list to add validation error messages to.
     * @param displayName A display name for what endpoint URL is being validated (will become part of error messages),
     *            e.g. "jobserver".
     */
    public static void validateRESTEndpointURL(final String endpointUrl, final List<String> errors, final String displayName) {
        if (endpointUrl == null || endpointUrl.isEmpty()) {
            errors.add(String.format("The %s URL must not be empty.", displayName));
        } else {
            try {
                URL uri = new URL(endpointUrl);

                if (uri.getProtocol() == null || uri.getProtocol().isEmpty()) {
                    errors.add(String.format("Protocol in %s URL required (http or https)", displayName));
                } else if (!(uri.getProtocol().equalsIgnoreCase("http") || uri.getProtocol().equalsIgnoreCase("https"))) {
                    errors.add(String.format("Only http:// and https:// are supported in the %s URL", displayName));
                } else if (uri.getHost() == null || uri.getHost().isEmpty()) {
                    errors.add(String.format("Hostname in %s URL required.", displayName));
                } else if (uri.getPort() < 0) {
                    errors.add(String.format("Port in %s URL required.", displayName));
                }

            } catch (MalformedURLException e) {
                errors.add(String.format("Invalid %s URL: %s", displayName, e.getMessage()));
            }
        }
    }

    public static void validateCredential(final String credentialName, final List<String> errors) {
        if (credentialName == null || credentialName.isEmpty()) {
            errors.add("Credentials name required.");
        }
    }

    public static void validateReceiveTimeout(final Duration receiveTimeout, final List<String> errors) {
        // Receive timeout
        if (receiveTimeout.toMillis() < 0) {
            errors.add("Receive timeout must be positive.");
        }
    }

    public static void validateCustomSparkSettings(final boolean useCustomSparkSettings,
        final String customSparkSettings, final List<String> errors) {
        if (useCustomSparkSettings) {
            try {
                parseSettingsString(customSparkSettings);
            } catch (IllegalArgumentException e) {
                errors.add("Invalid custom Spark setting: " + e.getMessage());
            }
        }
    }

    public static void validateSparkContextName(final String contextName, final List<String> errors) {
        // Context name
        if (contextName == null || contextName.isEmpty()) {
            errors.add("Context name required.");
        } else if (!contextName.matches("^[A-Za-z].*")) {
            errors.add("Context name must start with letters.");
        } else if (!contextName.matches("^[A-Za-z0-9\\_\\-\\.]+$")) {
            errors.add("Invalid characters in context name found. Supported characters are: a-z, A-Z, 0-9, ., - and _.");
        }
    }

    public static String mergeErrors(final List<String> errors) {
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

    /**
     * Parses the contents of a custom settings string into a map with String keys and values. A custom settings string
     * uses the colon (':') as delimiter between key and value, and a newline ('\n') as a delimiter between key/value
     * pairs.
     *
     * @param settingsString A String of the form key1:value1\nkey2:value2\n...
     * @return a map with the parsed key value pairs.
     * @throws IllegalArgumentException if the settings string did not conform to the required format.
     */
    public static Map<String, String> parseSettingsString(final String settingsString) {
        final Map<String, String> toReturn = new HashMap<String, String>();

        String[] lines = settingsString.split("\n");
        for (int lineNumber = 1; lineNumber <= lines.length; lineNumber++) {
            final String line = lines[lineNumber - 1].trim();

            if (line.isEmpty() || line.startsWith("#") || line.startsWith("//")) {
                continue;
            }

            final String[] splits = line.split(":", 2);
            if (splits.length == 1) {
                throw new IllegalArgumentException(
                    String.format("Missing colon (':') to delimit key and value in line %d.", lineNumber));
            }

            final String key = splits[0].trim();
            final String value = splits[1].trim();

            if (key.isEmpty()) {
                throw new IllegalArgumentException(String.format("Missing key in line %d.", lineNumber));
            }

            if (value.isEmpty()) {
                throw new IllegalArgumentException(String.format("Missing value in line %d.", lineNumber));
            }

            toReturn.put(key, value);
        }

        return toReturn;
    }
}
