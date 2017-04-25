package com.knime.tpbuilder.osgi

import org.apache.maven.shared.osgi.DefaultMaven2OsgiConverter
import org.osgi.framework.Version
import aQute.lib.osgi.Analyzer
import java.util.jar.JarFile
import org.apache.maven.artifact.versioning.VersionRange
import java.io.File
import org.apache.maven.artifact.repository.layout.DefaultRepositoryLayout
import org.apache.maven.artifact.handler.DefaultArtifactHandler
import java.nio.file.Paths
import java.util.jar.Attributes.Name
import com.knime.tpbuilder.TPConfigReader.TPConfig
import org.apache.maven.artifact.{DefaultArtifact, Artifact => MavenArtifact}
import com.knime.tpbuilder.Artifact
import com.knime.tpbuilder.BundleInfo

object OsgiUtil {

  //  val localRepoDir = Paths.get(System.getProperty("user.home"), ".m2", "repository").toFile()
  val localRepoDir = Paths.get("/home/bjoern", ".m2", "repository").toFile()

  val mavenRepoLayout = new DefaultRepositoryLayout()

  val maven2Osgi = new DefaultMaven2OsgiConverter()

  def withBundleAndVersion(art: Artifact, srcFile: Option[File], config: TPConfig): Artifact = {

    val mavenArtifact = artToMavenArt(art)

    val isPrebundled = isAlreadyBundled(mavenArtifact)

    val osgiName = findEffectiveBundleSymbolicName(art, mavenArtifact, config)
    val osgiVersion = findEffectiveBundleVersion(art, mavenArtifact, isPrebundled, config)

    val bundleInfo = BundleInfo(
      osgiName,
      Version.parseVersion(osgiVersion),
      isPrebundled)

    withBundle(art, bundleInfo, Some(mavenArtifact.getFile), srcFile)
  }
  
  def artToMavenArt(art: Artifact, requireFile: Boolean = true): MavenArtifact = {
    val mavenArtifact = new DefaultArtifact(art.group,
      art.artifact,
      VersionRange.createFromVersion(art.version),
      null,
      "jar",
      art.classifier match {
        case Some(classifier) => classifier
        case _ => null
      },
      new DefaultArtifactHandler("jar"))
    
    
    if (requireFile) {
      val artifactJar = new File(localRepoDir, mavenRepoLayout.pathOf(mavenArtifact))

      require(artifactJar.canRead(),
        "Artifact jar %s is not readable. Does it exist?".format(artifactJar.getAbsolutePath))

      mavenArtifact.setFile(artifactJar)
    }
    
    mavenArtifact
  }

  private def findEffectiveBundleSymbolicName(art: Artifact, mavenArtifact: MavenArtifact, config: TPConfig): String = {
    TPConfig.getBundleInstructions(config)(art) match {
      case Some(instr) =>
        instr.getOrElse(Analyzer.BUNDLE_SYMBOLICNAME, maven2Osgi.getBundleSymbolicName(mavenArtifact))
      case None =>
        maven2Osgi.getBundleSymbolicName(mavenArtifact)
    }
  }

  private def findEffectiveBundleVersion(art: Artifact, mavenArtifact: MavenArtifact, isPrebundled: Boolean, config: TPConfig): String = {
    val baseVersion = maven2Osgi.getVersion(art.version).replaceAll("[^A-Za-z0-9.]", "")

    // a version string that is suffixed with the version of the target platform itself
    val baseVersionWithSuffix = addKNIMEVersionSuffix(baseVersion, config.version)

    TPConfig.getBundleInstructions(config)(art) match {
      case Some(instr) =>
        // artifacts (whether prebundled or not) for which we have BND instructions always get the suffixed version string, unless
        // the instruction explicitly set a version
        instr.getOrElse(Analyzer.BUNDLE_VERSION, baseVersionWithSuffix)
      case None =>
        if (isPrebundled)
          // prebundled artifacts which we don't alter should keep their version string
          baseVersion
        else
          // unbundled artifacts should get 
          baseVersionWithSuffix
    }
  }

  def addKNIMEVersionSuffix(baseVersion: String, suffix: String): String = {
    if (baseVersion.endsWith(suffix))
      return baseVersion
    
    val parsedVersion = Version.parseVersion(baseVersion)
    val qualifierExists = parsedVersion.getQualifier != null && !parsedVersion.getQualifier.isEmpty

    if (qualifierExists)
      baseVersion + "-" + suffix
    else
      baseVersion + "." + suffix
  }

  def withBundle(art: Artifact, 
      bundleInfo: BundleInfo, 
      artifactJar: Option[File] = None, 
      srcFile: Option[File] = None): Artifact = {
    
    Artifact(group = art.group,
      artifact = art.artifact,
      version = art.version,
      packaging = art.packaging,
      classifier = art.classifier,
      bundle = Some(bundleInfo),
      file = artifactJar,
      isMerged = art.isMerged,
      mergedArtifacts = art.mergedArtifacts,
      sourceFile = srcFile)
  }

  private def isAlreadyBundled(artifact: org.apache.maven.artifact.Artifact): Boolean = {
    val file = artifact.getFile

    if (file.exists()) {
      val jar = new JarFile(file, false)

      Option(jar.getManifest()) match {
        case Some(manifest) => jar.getManifest().getMainAttributes().containsKey(new Name(Analyzer.BUNDLE_SYMBOLICNAME))
        case _ => false
      }
    } else {
      false
    }
  }
}