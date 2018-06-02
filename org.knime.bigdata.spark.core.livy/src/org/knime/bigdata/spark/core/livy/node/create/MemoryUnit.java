package org.knime.bigdata.spark.core.livy.node.create;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Enum for commonly used memory units in Spark. This models base-2 memory units, e.g. MiB (mebibytes). Methods to
 * perform simple string parsing are also provided.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public enum MemoryUnit {

        /**
         * Mebibytes unit.
         */
        MB("m", 1),

        /**
         * Gibibytes unit.
         */
        GB("g", 1024),

        /**
         * Tebibytes unit.
         */
        TB("t", 1024 * 1024);

    private final String m_sparkSettingsUnit;

    private final int m_mebibytes;

    MemoryUnit(final String sparkSettingsUnit, final int mebibytes) {
        m_sparkSettingsUnit = sparkSettingsUnit;
        m_mebibytes = mebibytes;
    }

    /**
     * @return the unit string to use in the Spark settings.
     */
    public String getSparkSettingsUnit() {
        return m_sparkSettingsUnit;
    }

    /**
     * Converts a value in the current unit into a value in the given target unit. Example: MB.convertTo(2048, GB)
     * yields "2".
     * 
     * @param currentUnitValue A value in the unit represented by the current instance.
     * @param targetUnit The unit to convert to.
     * @return The converted value in the target unit.
     */
    public double convertTo(double currentUnitValue, MemoryUnit targetUnit) {
        if (this == targetUnit) {
            return currentUnitValue;
        } else {
            return (currentUnitValue * m_mebibytes) / targetUnit.m_mebibytes;
        }
    }
    
    private final static Map<MemoryUnit, Set<String>> UNIT_STRINGS = new HashMap<>();
    static {
        UNIT_STRINGS.put(MB, new HashSet<>(Arrays.asList("m", "mb", "M", "MB")));
        UNIT_STRINGS.put(GB, new HashSet<>(Arrays.asList("g", "gb", "G", "GB")));
        UNIT_STRINGS.put(TB, new HashSet<>(Arrays.asList("t", "tb", "T", "TB")));
    }
    
    /**
     * Resolves the given unit string into a {@link MemoryUnit} enum value.
     * 
     * @param unitString The string representation of a unit.
     * @return the {@link MemoryUnit} enum value represented by the given string.
     * @throws IllegalArgumentException when the unit string cannot be resolved to a {@link MemoryUnit} enum value.
     */
    public static MemoryUnit parse(final String unitString) {
        if (UNIT_STRINGS.get(MB).contains(unitString)) {
            return MB;
        } else if (UNIT_STRINGS.get(GB).contains(unitString)) {
            return GB;
        } else if (UNIT_STRINGS.get(TB).contains(unitString)) {
            return TB;
        } else {
            throw new IllegalArgumentException("Unsupported memory unit: " + unitString);
        }
    }
    
    /**
     * Parsed a string of the form "2g" and into a number representing the value in the given target unit. Example:
     * parseMemorySetting("2g", MB) yields 2048.
     * 
     * @param memorySetting A memory setting string of the form "2g".
     * @param targetUnit The unit to convert to.
     * @return the value in the given target unit.
     */
    public static double parseMemorySetting(String memorySetting, MemoryUnit targetUnit) {
        // defaults, which are used if parsing fails
        final Pattern pattern = Pattern.compile("([0-9]+)\\s*([a-zA-Z]+)");

        final Matcher matcher = pattern.matcher(memorySetting.replaceAll("\\s+", ""));
        if (matcher.matches() && matcher.groupCount() == 2) {
            final int parsedNumber = Integer.parseInt(matcher.group(1));
            final MemoryUnit parsedUnit = MemoryUnit.parse(matcher.group(2));
            return parsedUnit.convertTo(parsedNumber, targetUnit);
        } else {
            throw new IllegalArgumentException("Unsupported memory setting format: " + memorySetting);
        }
    }
}
