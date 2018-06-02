package org.knime.bigdata.spark.core.livy.node.create;

import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

import org.knime.bigdata.spark.core.util.TextTemplateUtil;

/**
 * Utility class to generate an estimation of the YARN cluster resources
 * that will be requested by a given configuration of the "Create Spark Context (Livy)"
 * node. This class assumed that the YARN-cluster deploy mode is used.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 *
 */
public class SparkResourceEstimator {
    

    private final String EXECUTOR_MEMORY_OVERHEAD_KEY = "spark.yarn.executor.memoryOverhead";

    private final int EXECUTOR_MEMORY_OVERHEAD_MIN = 384;

    private final String DRIVER_MEMORY_OVERHEAD_KEY = "spark.yarn.driver.memoryOverhead";

    private final int DRIVER_MEMORY_OVERHEAD_MIN = 384; 

    private final int YARN_MEMORY_INCREMENT = 512;
    
    private final LivySparkContextCreatorNodeSettings m_settings;

    /**
     * Constructor.
     * 
     * @param settings The underlying node settings.
     */
    public SparkResourceEstimator(final LivySparkContextCreatorNodeSettings settings) {
        m_settings = settings;
    }

    /**
     * 
     * @return a HTML summary of the YARN cluster resources that will be requested. 
     */
    public String createResourceSummary() {
        switch(m_settings.getExecutorAllocation()) {
            case DEFAULT:
                return createDefaultAllocationSummary();
            case FIXED:
                return createFixedAllocationSummary();
            case DYNAMIC:
                return createDynamicAllocationSummary();
            default:
                throw new IllegalArgumentException("Unknown allocation type: " + m_settings.getExecutorAllocation());
        }
    }

    private String createDynamicAllocationSummary() {
        final int driverMemoryMb = estimateDriverMemoryMb();
        final int driverCores = getDriverCores();
        
        final int executorMemoryMb = estimateExecutorMemoryMb();
        final int executorCores = m_settings.getExecutorResources().getCores();

        final int minExecutors = m_settings.getDynamicExecutorsMin();
        final int maxExecutors = m_settings.getDynamicExecutorsMax();
        
        final int totalCoresMin = driverCores + minExecutors * executorCores;
        final int totalCoresMax = driverCores + maxExecutors * executorCores;
        final int totalMemoryMinMb = driverMemoryMb + minExecutors * executorMemoryMb;
        final int totalMemoryMaxMb = driverMemoryMb + maxExecutors * executorMemoryMb;
        
        final MemoryUnit containerDisplayMemoryUnit = chooseConvenientMemoryUnit(Math.max(driverMemoryMb, executorMemoryMb));
        final MemoryUnit totalDisplayMemoryUnit = chooseConvenientMemoryUnit(Math.max(totalMemoryMinMb, totalMemoryMaxMb));
        
        final Map<String, String> reps = new HashMap<>();
        reps.put("driver.memory", renderMemoryForDisplay(driverMemoryMb, containerDisplayMemoryUnit));
        reps.put("driver.cores", Integer.toString(driverCores));
        reps.put("executors.min", Integer.toString(minExecutors));
        reps.put("executors.max", Integer.toString(maxExecutors));
        reps.put("executor.memory",renderMemoryForDisplay(executorMemoryMb, containerDisplayMemoryUnit));
        reps.put("executor.cores", Integer.toString(executorCores));
        reps.put("total.memory.min", renderMemoryForDisplay(totalMemoryMinMb, totalDisplayMemoryUnit));
        reps.put("total.memory.max", renderMemoryForDisplay(totalMemoryMaxMb, totalDisplayMemoryUnit));
        reps.put("total.cores.min", Integer.toString(totalCoresMin));
        reps.put("total.cores.max", Integer.toString(totalCoresMax));
        reps.put("memory.unit.containers", containerDisplayMemoryUnit.toString());
        reps.put("memory.unit.total", totalDisplayMemoryUnit.toString());
        
        try (InputStream r = getClass().getResourceAsStream("resource_summary_dynamic_allocation.template")) {
            return TextTemplateUtil.fillOutTemplate(r, reps);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read context description template");
        }
    }

    private String createFixedAllocationSummary() {
        final int driverMemoryMb = estimateDriverMemoryMb();
        final int driverCores = getDriverCores();
        
        final int executorMemoryMb = estimateExecutorMemoryMb();
        final int executorCores = m_settings.getExecutorResources().getCores();
        
        final int numExecutors = m_settings.getFixedExecutors();
        
        final int totalCores = driverCores + numExecutors * executorCores;
        final int totalMemoryMb = driverMemoryMb + numExecutors * executorMemoryMb;
        
        final MemoryUnit containerDisplayMemoryUnit = chooseConvenientMemoryUnit(Math.max(driverMemoryMb, executorMemoryMb));
        final MemoryUnit totalDisplayMemoryUnit = chooseConvenientMemoryUnit(totalMemoryMb);
        
        final Map<String, String> reps = new HashMap<>();
        reps.put("driver.memory", renderMemoryForDisplay(driverMemoryMb, containerDisplayMemoryUnit));
        reps.put("driver.cores", Integer.toString(driverCores));
        reps.put("executors.fixed", Integer.toString(numExecutors));
        reps.put("executor.memory",renderMemoryForDisplay(executorMemoryMb, containerDisplayMemoryUnit));
        reps.put("executor.cores", Integer.toString(executorCores));
        reps.put("total.memory", renderMemoryForDisplay(totalMemoryMb, totalDisplayMemoryUnit));
        reps.put("total.cores", Integer.toString(totalCores));
        reps.put("memory.unit.containers", containerDisplayMemoryUnit.toString());
        reps.put("memory.unit.total", totalDisplayMemoryUnit.toString());
        
        try (InputStream r = getClass().getResourceAsStream("resource_summary_fixed_allocation.template")) {
            return TextTemplateUtil.fillOutTemplate(r, reps);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read context description template");
        }
    }
    
    private final static NumberFormat MEMORY_NUMBER_FORMAT = new DecimalFormat("##.##");

    private static String renderMemoryForDisplay(int memoryMb, MemoryUnit displayUnit) {
        return MEMORY_NUMBER_FORMAT.format(MemoryUnit.MB.convertTo(memoryMb, displayUnit));
    }

    private static MemoryUnit chooseConvenientMemoryUnit(int valueMb) {
        if (valueMb > MemoryUnit.TB.convertTo(1, MemoryUnit.MB)) {
            return MemoryUnit.TB;
        } else if (valueMb >= 10000) {
            return MemoryUnit.GB;
        } else {
            return MemoryUnit.MB;
        }
    }

    private int estimateExecutorMemoryMb() {
        final ContainerResourceSettings executorResources = m_settings.getExecutorResources();
        final int executorMemoryMb = (int) executorResources.getMemoryUnit().convertTo(executorResources.getMemory(), MemoryUnit.MB);
        return estimateYarnMemory(executorMemoryMb, getExecutorMemoryOverhead(), EXECUTOR_MEMORY_OVERHEAD_MIN);
    }
    
    private double getExecutorMemoryOverhead() {
        if (m_settings.useCustomSparkSettings()
            && m_settings.getCustomSparkSettingsModel().isAssignedKey(EXECUTOR_MEMORY_OVERHEAD_KEY)) {

            try {
                return Double
                    .parseDouble(m_settings.getCustomSparkSettingsModel().getValue(EXECUTOR_MEMORY_OVERHEAD_KEY));
            } catch (Exception e) {
            }
        }

        // FIXME: make defaults configurable
        return 0.1;
    }

    private int getDriverCores() {
        final ContainerResourceSettings driverResources = m_settings.getDriverResources();
        
        // FIXME: make defaults configurable
        int driverCores = 1;
        if (driverResources.overrideDefault()) {
            driverCores = driverResources.getCores();
        }
        
        return driverCores;
    } 

    private int estimateDriverMemoryMb() {
        final ContainerResourceSettings driverResources = m_settings.getDriverResources();

        // FIXME: make defaults configurable
        int driverMemoryMb = 1024;
        if (driverResources.overrideDefault()) {
            driverMemoryMb = (int)driverResources.getMemoryUnit().convertTo(driverResources.getMemory(), MemoryUnit.MB);
        }

        return estimateYarnMemory(driverMemoryMb, getDriverMemoryOverhead(), DRIVER_MEMORY_OVERHEAD_MIN);
    }

    
    private double getDriverMemoryOverhead() {
        if (m_settings.useCustomSparkSettings()
            && m_settings.getCustomSparkSettingsModel().isAssignedKey(DRIVER_MEMORY_OVERHEAD_KEY)) {

            try {
                return Double
                    .parseDouble(m_settings.getCustomSparkSettingsModel().getValue(DRIVER_MEMORY_OVERHEAD_KEY));
            } catch (Exception e) {
            }
        }
        // FIXME: make defaults configurable
        return 0.1;
    }

    
    private int estimateYarnMemory(int baseMemoryMb, double overheadFraction, int overheadMinMb) {
        final int overhead = (int)Math.max(baseMemoryMb * overheadFraction, overheadMinMb);
        
        final int preTotalMb = baseMemoryMb + overhead;
        
        // round up to the next multiple of YARN_MEMORY_INCREMENT if necessary
        if (preTotalMb % YARN_MEMORY_INCREMENT == 0) {
            return preTotalMb;
        } else {
            
            return ((preTotalMb / YARN_MEMORY_INCREMENT) + 1) * YARN_MEMORY_INCREMENT;
        }
    }

    private String createDefaultAllocationSummary() {
        final int driverMemoryMb = estimateDriverMemoryMb();
        final int driverCores = getDriverCores();
        
        final int executorMemoryMb = estimateExecutorMemoryMb();
        final int executorCores = m_settings.getExecutorResources().getCores();
        
        final MemoryUnit containerDisplayMemoryUnit = chooseConvenientMemoryUnit(Math.max(driverMemoryMb, executorMemoryMb));
        
        final Map<String, String> reps = new HashMap<>();
        reps.put("driver.memory", renderMemoryForDisplay(driverMemoryMb, containerDisplayMemoryUnit));
        reps.put("driver.cores", Integer.toString(driverCores));
        reps.put("executor.memory",renderMemoryForDisplay(executorMemoryMb, containerDisplayMemoryUnit));
        reps.put("executor.cores", Integer.toString(executorCores));
        reps.put("memory.unit.containers", containerDisplayMemoryUnit.toString());
        
        try (InputStream r = getClass().getResourceAsStream("resource_summary_default_allocation.template")) {
            return TextTemplateUtil.fillOutTemplate(r, reps);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read context description template");
        }
    }
}
