---
name: add-new-spark-version
description: Automates the process of adding a new Spark version to the KNIME Apache Spark Integration. Handles creating a new Spark version and plugin, updating dependencies, renaming files/directories, and registering the new plugin. Requires ticket ID and version number.
---

# Add a new Spark Version

This agent automates the process of adding a new Spark version to the KNIME Apache Spark Integration.

## Prerequisites

**IMPORTANT:** Before running this agent:
1. The target platform changes in `knime-bigdata-externals` must already be completed. This includes adding the new Spark artifacts to `knime-bigdata-externals/com.knime.tpbuilder/tp-config.yml`.
2. You must provide the new Spark version number in the format `X.Y.Z` (e.g., `4.0.2`).
3. The agent requires access to Maven Central to validate artifact versions and resolve transitive dependencies.

## Required User Input

The agent **MUST** ask the user for the following information before proceeding:

1. **Ticket ID and Summary**: A ticket reference with summary (e.g., `BD-12345: Add support for Spark 4.0`)
   - Format validation: Must match pattern `[A-Z]+-\d+:\s+.+`
   - If not provided, error out immediately
   - Extract the ticket ID (e.g., `BD-12345`) and full summary for use in commit messages

2. **New Spark Version**: The full version string (e.g., `4.0.2`)
   - Format validation: Must match pattern `\d+\.\d+\.\d+`
   - If not provided, error out immediately

## Version Naming Convention

- **Full version**: `X.Y.Z` (e.g., `4.0.2`)
- **Plugin suffix**: `X_Y` (e.g., `4_0`) - derived by combining the major and minor version numbers with an underscore
- **Plugin naming pattern**: `org.knime.bigdata.spark<suffix>`

## Current Repository State

- **Latest version in repo**: `3.5.8` (plugin suffix: `3_5`)
- **Projects per version**: 1 project are created for each version:
  1. `org.knime.bigdata.spark<suffix>` - Main plugin

## Step-by-Step Process

### Step 1: Identify Latest Version

**Action**: Automatically detect the latest Spark version in the repository by finding the highest version number in existing plugin directories.

- Look for directories matching `org.knime.bigdata.spark*`
- Compare version numbers (e.g., `4_0` > `3_5`)
- Current latest (as an example): `3_5` (version `3.5.8`)

### Step 2: Copy Projects (SEPARATE COMMIT)

**CRITICAL**: This copying step must be done as a **separate Git commit** to make it easier to trace changes.

**Action**: Copy the project from the latest version:

```bash
# Assuming latest is 3_5 and new version is 4_0
cp -r org.knime.bigdata.spark3_5 org.knime.bigdata.spark4_0
```

**Git Commit**:
```bash
git add org.knime.bigdata.spark4_0
git commit -m "<ticket-id>: <short summary>

Copy projects without modifications for easier change tracking.

<ticket-id> (<full ticket summary>)"
```

**Example**:
```bash
git commit -m "BD-12345: copy Spark 3.5 to 4.0

Copy projects without modifications for easier change tracking.

BD-12345 (Add support for Spark 4.0)"
```

**Wait for user confirmation before proceeding to modifications.**

### Step 3: Replace Version Strings in Files

**Action**: Replace all occurrences of the old version strings with the new ones in all three copied projects.

For each project directory:

```bash
# Replace version suffix (e.g., 3_5 → 4_0)
find org.knime.bigdata.spark4_0 -type f -exec sed -i 's/3_5/4_0/g' {} \;

# Replace full version string (e.g., 3.5.8 → 4.0.2)
find org.knime.bigdata.spark4_0 -type f -exec sed -i 's/3\.5\.8/4\.0\.2/g' {} \;
```

**Note**: Use proper escaping for version strings with dots in sed commands.

### Step 4: Rename Files and Directories

**Action**: Rename files and directories that contain the version string.

```bash
# For each project directory
find org.knime.bigdata.spark4_0 -depth -execdir rename 's/3_5/4_0/' '{}' \+
```

**Note**: The `-depth` flag ensures that directories are renamed bottom-up.

### Step 5: Update MANIFEST.MF Files

**Action**: Update the `org.apache.spark` version bounds (especially upper bound) in the `META-INF/MANIFEST.MF` files of:

- `org.knime.bigdata.spark4_0/META-INF/MANIFEST.MF`

**Example**:
```
Bundle-Name: KNIME Extension for Apache Spark jobs for Spark 3.5
...
Require-Bundle: ...,
 org.apache.spark_2.12;bundle-version="[3.5.8,3.6.0)",
 ...
```

Update to:

```
Bundle-Name: KNIME Extension for Apache Spark jobs for Spark 4.0
...
Require-Bundle: ...,
 org.apache.spark_2.12;bundle-version="[4.0.2,4.1.0)",
 ...
```

**Note**: Adjust the upper bound to the next minor version.

### Step 6: Update Root POM

**Action**: Add the new module to the root `pom.xml`.

**File**: `knime-bigdata/pom.xml`

**Add**:
```xml
<modules>
    ...
    <module>org.knime.bigdata.spark4_0</module>
    ...
</modules>
```

**Note**: Insert in version order to maintain consistency.

### Step 7: Update Feature XML

**Action**: Add the new plugins to the feature definition.

**File**: `org.knime.features.bigdata.spark/feature.xml`

**Add**:
```xml
   <plugin
         id="org.knime.bigdata.spark4_0"
         download-size="0"
         install-size="0"
         version="0.0.0"/>
```

**Note**: These should be added after the existing version plugins.

### Step 8: Commit Plugin changes

**Action**: Commit the changes related to version string replacements and file renaming as a separate commit.

```bash
git add pom.xml
git add org.knime.features.bigdata.spark/feature.xml
git add org.knime.bigdata.spark4_0
git commit -m "<ticket-id>: fix version strings and rename files for new Spark X.Y version

Add the new Spark X.Y plugin:

* Update version strings in all files to reflect the new Spark version
* Rename files and directories to match the new version suffix
* Add plugin to root pom.xml and feature.xml

<ticket-id> (<full ticket summary>)"
```

**Example**:
```bash
git add pom.xml
git add org.knime.features.bigdata.spark/feature.xml
git add org.knime.bigdata.spark4_0
git commit -m "BD-12345: fix version strings and rename files for new Spark 4.0 version

Add the new Spark 4.0 plugin:

* Update version strings in all files to reflect the new Spark version
* Rename files and directories to match the new version suffix
* Add plugin to root pom.xml and feature.xml

BD-12345 (Add support for Spark 4.0)"
```

**Wait for user confirmation before proceeding to modifications.**

### Step 9: Add Spark Version enum entry and add support to Livy/Databricks plugins (SEPARATE COMMIT)

**Action**: Add the new Spark version to the `SparkVersion` enum and update any relevant code in the Livy and Databricks plugins to support the new version:

- `org.knime.bigdata.spark.core/src/org/knime/bigdata/spark/core/version/SparkVersion.java`
- `org.knime.bigdata.spark.core.livy/src/org/knime/bigdata/spark/core/livy/LivyPlugin.java`
- `org.knime.bigdata.databricks/src/org/knime/bigdata/databricks/DatabricksPlugin.java`


**Git Commit**:
```bash
git add org.knime.bigdata.spark.core/src/org/knime/bigdata/spark/core/version/SparkVersion.java
git add org.knime.bigdata.spark.core.livy/src/org/knime/bigdata/spark/core/livy/LivyPlugin.java
git add org.knime.bigdata.databricks/src/org/knime/bigdata/databricks/DatabricksPlugin.java
git commit -m "<ticket-id>: add Spark X.Y version to enum and support in Livy/Databricks plugins

Add the new Spark X.Y version:

* Add Spark X.Y to the SparkVersion enum
* Add Spark X.Y support to Livy plugin
* Add Spark X.Y support to Databricks plugin

<ticket-id> (<full ticket summary>)"
```

**Example**:
```bash
git commit -m "BD-12345: add Spark 4.0 version to enum and support in Livy/Databricks plugins

Add the new Spark 4.0 version:

* Add Spark 4.0 to the SparkVersion enum
* Add Spark 4.0 support to Livy plugin
* Add Spark 4.0 support to Databricks plugin

BD-12345 (Add support for Spark 4.0)"
```

## Error Handling

The agent should handle the following error cases:

1. **No ticket ID provided**: Error out immediately with message requesting the ticket ID and summary.
2. **Invalid ticket ID format**: Validate that the ticket matches `[A-Z]+-\d+:\s+.+` pattern.
3. **No version provided**: Error out immediately with message requesting the version.
4. **Invalid version format**: Validate that the version matches `\d+\.\d+\.\d+\.\d+` pattern.
5. **Latest version detection fails**: Ask user to manually specify the latest version.
6. **File copy failures**: Report which files failed to copy and why.
7. **sed/rename command failures**: Report and suggest manual intervention.

## Validation Steps

After all changes are complete, the agent should verify:

1. The new project directories exist
2. Version strings have been updated in key files:
   - MANIFEST.MF files
   - Java source files
3. Root pom.xml includes the new modules
4. feature.xml includes the new plugins

## Example Usage

User: `/add-new-spark-version with ticket "BD-12345: add Spark 4.0" to add Spark version 4.0.2`

Agent would:
1. Extract ticket ID `BD-12345` and summary `add Spark 4.0`
2. Parse version `4.0.2` → suffix `4_0`
3. Detect latest version `3_5` (3.5)
4. Copy the project and commit with ticket reference
5. Replace all version strings
6. Rename files/directories
7. Update pom.xml, feature.xml, MANIFEST.MF files
8. Commit plugin changes with ticket reference
9. Add Spark version to enum and support in Livy/Databricks plugins, commit changes with ticket reference

## Notes for the Agent

- **Always use the latest version** in the repository as the source for copying
- **Version comparison**: Parse the numeric suffix (e.g., `35` vs `40`) and compare numerically
- **Commit discipline**: The copying step MUST be a separate commit
- **sed escaping**: Remember to escape dots in version strings: `3\.5\.8`
- **File permissions**: Preserve file permissions when copying
- **Incremental approach**: After major steps, ask if the user wants to review before continuing

## Additional Context

This process is based on human-readable instructions created several years ago and has been successfully used to add multiple Spark versions to the KNIME codebase. The key insight is that each Spark version is isolated in its own set of plugins, allowing multiple versions to coexist in the same installation.
