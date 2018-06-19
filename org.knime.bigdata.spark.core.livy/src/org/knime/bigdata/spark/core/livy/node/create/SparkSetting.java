package org.knime.bigdata.spark.core.livy.node.create;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.core.runtime.Platform;
import org.knime.bigdata.spark.core.livy.node.create.ui.DefaultKeyDescriptor;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.osgi.framework.Bundle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Models a Spark setting. This class contains Jackson annotation so that instances of it can be deserialized from JSON.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class SparkSetting extends DefaultKeyDescriptor {

    private static final Map<SparkVersion, List<SparkSetting>> CACHED_DESCRIPTORS = new HashMap<>();

    @JsonCreator
    private SparkSetting(@JsonProperty("setting") final String key, @JsonProperty("default") final String defaultValue,
        @JsonProperty("description") final String description) {
        super(key, defaultValue, false, description);
    }

    /**
     * Loads the Spark settings supported by the given Spark version from a JSON file in the respective jobs plugin.
     * Repeated invocations of this method with the same Spark version will return the same list object.
     * 
     * @param sparkVersion The Spark version to load the supported settings for.
     * @return a list of Spark settings.
     * @throws IOException If loading or deserializing the supported settings failed.
     */
    public static synchronized List<SparkSetting> getSupportedSettings(SparkVersion sparkVersion) throws IOException {

        List<SparkSetting> toReturn = CACHED_DESCRIPTORS.get(sparkVersion);
        if (toReturn == null) {
            // FIXME this is a bit of a hack and needs to be cleaned up. This should be done via
            // an extension point or OSGI service.
            final Bundle jobsBundle = Platform.getBundle(
                String.format("org.knime.bigdata.spark%d_%d", sparkVersion.getMajor(), sparkVersion.getMinor()));
            final URL settingsFile = jobsBundle.getEntry("/resources/spark-settings.json");
            try (InputStream in = new BufferedInputStream(settingsFile.openStream())) {
                ObjectMapper objectMapper = new ObjectMapper();
                final SparkSetting[] settings = objectMapper.readValue(in, SparkSetting[].class);
                toReturn = Collections.unmodifiableList(Arrays.asList(settings));
            }
            CACHED_DESCRIPTORS.put(sparkVersion, toReturn);
        }

        return toReturn;
    }
}
