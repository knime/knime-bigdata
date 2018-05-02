package com.knime.tpbuilder.osgi

import java.io.File

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.JavaConverters.mutableMapAsJavaMapConverter
import scala.collection.Map
import scala.collection.mutable.Buffer
import scala.collection.mutable.{ Map => MutableMap }
import scala.collection.mutable.Seq
import scala.collection.mutable.Set

import org.apache.commons.io.FileUtils
import org.apache.maven.plugin.logging.SystemStreamLog
import org.osgi.framework.Version
import org.reficio.p2.bundler.ArtifactBundlerInstructions
import org.reficio.p2.bundler.ArtifactBundlerRequest
import org.reficio.p2.bundler.impl.AquteBundler
import org.reficio.p2.logger.Logger
import org.reficio.p2.utils.{ JarUtils => ReficioJarUtils }

import com.knime.tpbuilder.Artifact
import com.knime.tpbuilder.BundleInfo
import com.knime.tpbuilder.TPConfigReader.TPConfig
import com.knime.tpbuilder.TPConfigReader.TPMavenOverride

import aQute.bnd.header.OSGiHeader
import aQute.bnd.osgi.Instructions
import aQute.lib.osgi.Analyzer
import aQute.lib.osgi.Jar
import aQute.libg.glob.Glob

object Bundler {

  def createBundles(depGraph: Map[Artifact, Set[Artifact]], outputDir: File, config: TPConfig) = {
    Logger.initialize(new SystemStreamLog)

    // we have 3 types of artifacts to bundle:
    //  - multi-artifact (result of merging)
    //  - single-artifact & prebundled
    //  - single-artifact & not prebundled 
    for ((art, artDeps) <- depGraph.iterator) {

      if (art.isMerged) {
        println("Creating merged bundle %s-%s".format(art.bundle.get.bundleSymbolicName, art.bundle.get.bundleVersion.toString))
        createMergedBundle(art, depGraph, outputDir, config)
      } else {
        println("Creating bundle %s-%s".format(art.bundle.get.bundleSymbolicName, art.bundle.get.bundleVersion.toString))
        createNewBundle(art, depGraph, outputDir, config)
      }

      ReficioJarUtils.removeSignature(createOutputFile(outputDir, art.bundle.get))
      println()
    }
  }

  

  private def removeBundleFileExcludes(origJar: File, bundleFileExcludes: scala.collection.Seq[String]): File = {
    if (!bundleFileExcludes.isEmpty) {
      val globs = bundleFileExcludes.map(globString => new Glob(globString))

      val jar = new Jar(origJar)
      val resourcePaths = Set() ++= jar.getResources.keySet().asInstanceOf[java.util.Set[String]].asScala

      for (glob <- globs) {
        for (resourcePath <- resourcePaths) {
          if (glob.matcher(resourcePath).matches()) {
            println(s"Removing excluded file ${resourcePath} from jar")
            jar.remove(resourcePath)
          }
        }
      }

      val filteredJar = new File(origJar.getAbsolutePath + ".filtered")
      jar.write(filteredJar)
      jar.close()
      filteredJar
    } else origJar
  }

  private def filterFiles(jar: Jar, filter: String) = {
    val param = OSGiHeader.parseHeader(filter)
    val instructions = new Instructions(param)

  }

  private def createMergedBundle(art: Artifact, depGraph: Map[Artifact, Set[Artifact]], outputDir: File, config: TPConfig) = {
    val outputJar = createOutputFile(outputDir, art.bundle.get)
    val bundleInfo = art.bundle.get
    val mergedArts = art.mergedArtifacts.get
    val bundler = new AquteBundler(false)
    
    val filesToCleanUp = Buffer[File]()

    try {
      // merge jars into a single one
      val mergedJar = new File(outputJar.getAbsolutePath + ".tmp")
      if (mergedJar.exists())
        require(mergedJar.delete(), "Failed to delete preexisting file " + mergedJar.getAbsolutePath)
      filesToCleanUp += mergedJar
      JarMerger.mergeJars(mergedJar, art.mergedArtifacts.get.map(art => art.file.get))

      // filter out any excluded files
      val filteredJar = removeBundleFileExcludes(mergedJar, TPConfig.getBundleFileExcludes(config)(art))
      filesToCleanUp += filteredJar

      val sourceJars = art.mergedArtifacts.get.filter(_.sourceFile.isDefined).map(_.sourceFile.get)
      val mergedSourceJar =
        if (!sourceJars.isEmpty) {
          val srcJar = new File(outputJar.getAbsolutePath + ".src.tmp")
          JarMerger.mergeJars(srcJar, sourceJars)
          filesToCleanUp += srcJar
          Some(srcJar)
        } else None
      val srcOutputJar =
        if (!sourceJars.isEmpty)
          Some(createSourceOutputFile(outputDir, art.bundle.get))
        else None

      createSourceOutputFile(outputDir, art.bundle.get)
      // create bundle from merged jar using optionally specified BND instructions from config
      createBundle(art,
        depGraph,
        filteredJar,
        mergedSourceJar,
        outputJar,
        srcOutputJar,
        config)

    } finally filesToCleanUp.foreach(FileUtils.deleteQuietly)
  }

  private def createNewBundle(art: Artifact, depGraph: Map[Artifact, Set[Artifact]], outputDir: File, config: TPConfig) = {
    val outputJar = createOutputFile(outputDir, art.bundle.get)
    val srcOutputJar =
      if (art.sourceFile.isDefined)
        Some(createSourceOutputFile(outputDir, art.bundle.get))
      else None

    val filesToCleanUp = Buffer[File]()

    try {
      // copy original jar to a temp file
      val tmpJar = new File(outputJar.getAbsolutePath + ".tmp")
      filesToCleanUp += tmpJar
      FileUtils.copyFile(art.file.get, tmpJar)

      // remove excluded files from the temp file
      val filteredJar = removeBundleFileExcludes(tmpJar, TPConfig.getBundleFileExcludes(config)(art))
      filesToCleanUp += filteredJar

      // create bundle using optionally specified BND instructions from config
      createBundle(art,
        depGraph,
        filteredJar,
        art.sourceFile,
        outputJar,
        srcOutputJar,
        config)
    } finally filesToCleanUp.foreach(FileUtils.deleteQuietly)
  }

  private def createOutputFile(outputDir: File, bundleInfo: BundleInfo): File = {
    new File(outputDir, bundleInfo.bundleSymbolicName + "_" + bundleInfo.bundleVersion.toString + ".jar")
  }

  private def createSourceOutputFile(outputDir: File, bundleInfo: BundleInfo): File = {
    new File(outputDir, bundleInfo.bundleSymbolicName + ".source" + "_" + bundleInfo.bundleVersion.toString + ".jar")
  }

  private def createBundle(art: Artifact,
    depGraph: Map[Artifact, Set[Artifact]],
    inputJar: File,
    srcInputJar: Option[File],
    outputJar: File,
    srcOutputJar: Option[File],
    config: TPConfig): Unit = {

    // create instructions with substitutions
    val instr: Map[String, String]  =  TPConfig.getBundleInstructions(config)(art) match {
      case Some(rawInstr) => rawInstr.mapValues(TPConfig.evalVars(art, config))
      case _ => Map()
    }

    doCreateBundle(art, depGraph, instr, inputJar, srcInputJar, outputJar, srcOutputJar, config)
  }

  private def mkDependencyInstructions(art: Artifact,
    depGraph: Map[Artifact, Set[Artifact]],
    instr: scala.collection.Map[String, String],
    config: TPConfig): Seq[Tuple2[String, String]] = {

    val deps = depGraph(art)

    val importPackageOverrides = deps
      .map(dep => dep -> TPMavenOverride.maybeOverride(config.importPackageOverrides, dep))
      .filter(_._2.isDefined)
      .map(t => t._1 -> t._2.get)
      .toMap

    val requireBundleOverrides = deps
      .map(dep => dep -> TPMavenOverride.maybeOverride(config.requireBundleOverrides, dep))
      .filter(_._2.isDefined)
      .map(t => t._1 -> t._2.get)
      .toMap
      
    val requireBundleInjections = TPConfig.getRequireBundleInjections(config)(art)

    val reqBundleInstrOption = mkRequireBundle(art, depGraph, instr, importPackageOverrides, requireBundleOverrides, requireBundleInjections)
    val importPkgInstr = mkImportPackage(art, deps, instr, importPackageOverrides)

    reqBundleInstrOption match {
      case Some(reqBundleInstr) => Seq(Analyzer.IMPORT_PACKAGE -> importPkgInstr,
        Analyzer.REQUIRE_BUNDLE -> reqBundleInstr)
      case None => Seq(Analyzer.IMPORT_PACKAGE -> importPkgInstr)
    }
  }

  private def mkImportPackage(art: Artifact,
    deps: Set[Artifact],
    instr: scala.collection.Map[String, String],
    importPackageOverrides: scala.collection.Map[Artifact, String]): String = {

    // handle Import-Package instructions
    if (instr.contains(Analyzer.IMPORT_PACKAGE)) {
      if (importPackageOverrides.isEmpty) {
        println(s"Applying Import-Package instruction: ${instr(Analyzer.IMPORT_PACKAGE)}")
      } else {
        println(s"Ignoring Import-Package override(s) ${importPackageOverrides} due to explicitly " +
          "defined Import-Package instruction: ${instr(Analyzer.IMPORT_PACKAGE)}")
      }
      return instr(Analyzer.IMPORT_PACKAGE)
    } else if (!importPackageOverrides.isEmpty) {
      println(s"Applying Import-Package override(s) ${importPackageOverrides}")
      return importPackageOverrides.values.mkString(",")
    } else {
      return ""
    }
  }

  private def mkRequireBundle(art: Artifact,
    depGraph: Map[Artifact, Set[Artifact]],
    instr: scala.collection.Map[String, String],
    importPackageOverrides: scala.collection.Map[Artifact, String],
    requireBundleOverrides: scala.collection.Map[Artifact, String],
    requireBundleInjections: scala.collection.Seq[String]): Option[String] = {

    val deps = depGraph(art)
    val effectiveDeps = deps -- importPackageOverrides.keys
    val effectiveReqBundeOverrides = requireBundleOverrides -- importPackageOverrides.keys

    // handle Require-Bundle instructions
    if (instr.contains(Analyzer.REQUIRE_BUNDLE)) {
      if (!requireBundleOverrides.isEmpty) {
        println(s"Ignoring Require-Bundle override(s) due to manually defined Require-Bundle directive ${instr(Analyzer.REQUIRE_BUNDLE)}")
      }
      return Some(instr(Analyzer.REQUIRE_BUNDLE))
    } else if (!instr.contains(Analyzer.IMPORT_PACKAGE) && (!effectiveDeps.isEmpty || !requireBundleInjections.isEmpty)) {
      return Some(toRequireBundleHeaderValue(effectiveDeps, effectiveReqBundeOverrides, requireBundleInjections, depGraph))
    } else {
      if (!requireBundleOverrides.isEmpty) {
        println(s"Ignoring Require-Bundle override(s) due to manually defined Import-Package directive")
      }
      return None
    }
  }

  private def toRequireRange(version: Version): String = {
    "[%d.%d.0,%d.%d.0)".format(version.getMajor, version.getMinor, version.getMajor, version.getMinor + 1)
  }

  private def toRequireBundleHeaderValue(deps: Set[Artifact],
    requireBundleOverrides: scala.collection.Map[Artifact, String],
    requireBundleInjections: scala.collection.Seq[String],
    depGraph: Map[Artifact, Set[Artifact]]): String = {

    val generatedClauses = deps.map(dep => {
      requireBundleOverrides.get(dep) match {
        case Some(reqBundleClause) =>
          println(s"Overriding maven dependency on ${dep.mvnCoordinate} with Require-Bundle clause ${reqBundleClause}")
          reqBundleClause
        case None =>
          val clause = "%s;bundle-version=\"%s\"".format(dep.bundle.get.bundleSymbolicName,
            toRequireRange(dep.bundle.get.bundleVersion))

          if (!depGraph.contains(dep)) {
            clause + ";resolution:=optional"
          } else {
            clause
          }
      }
    })
    
    OsgiUtil.sanitizeRequireBundleHeaderClauses(generatedClauses ++ requireBundleInjections).mkString(",")
  }

  private def doCreateBundle(art: Artifact,
    depGraph: Map[Artifact, Set[Artifact]],
    instr: scala.collection.Map[String, String],
    inputJar: File,
    srcInputJar: Option[File],
    outputJar: File,
    srcOutputJar: Option[File],
    config: TPConfig) = {

    val bundleInfo = art.bundle.get
    val bundleName = instr.getOrElse(Analyzer.BUNDLE_NAME, bundleInfo.bundleSymbolicName)

    val bndInstructions = MutableMap(Analyzer.BUNDLE_DESCRIPTION ->
      instr.getOrElse(Analyzer.BUNDLE_DESCRIPTION, createBundleDescription(art)),
      Analyzer.BUNDLE_LICENSE -> mkBundleLicenseHeader(art))

    if (bundleInfo.vendor.isDefined)
      bndInstructions += (Analyzer.BUNDLE_VENDOR -> bundleInfo.vendor.get)

    if (bundleInfo.docUrl.isDefined)
      bndInstructions += (Analyzer.BUNDLE_DOCURL -> bundleInfo.docUrl.get)

    bndInstructions ++= mkDependencyInstructions(art, depGraph, instr, config)

    // handle Export-Package / Export-Contents instructions
    if (instr.contains(Analyzer.EXPORT_CONTENTS)) {
      println(s"Applying -exportcontents instruction ${instr(Analyzer.EXPORT_CONTENTS)}")
      bndInstructions += (Analyzer.EXPORT_CONTENTS -> instr(Analyzer.EXPORT_CONTENTS))
    } else if (instr.contains(Analyzer.EXPORT_PACKAGE)) {
      println(s"Overriding default Export-Package instruction with ${instr(Analyzer.EXPORT_PACKAGE)}")
      bndInstructions += (Analyzer.EXPORT_PACKAGE -> instr(Analyzer.EXPORT_PACKAGE))
    } else {
      bndInstructions += (Analyzer.EXPORT_PACKAGE -> TPConfig.evalVars(art, config)("*;version=${bundle.version}"))
    }

    bndInstructions ++= (instr -- Seq(Analyzer.IMPORT_PACKAGE,
      Analyzer.REQUIRE_BUNDLE,
      Analyzer.EXPORT_CONTENTS,
      Analyzer.EXPORT_PACKAGE,
      Analyzer.BUNDLE_NAME,
      Analyzer.BUNDLE_SYMBOLICNAME,
      Analyzer.BUNDLE_VERSION))

    var bundlerInstructions = ArtifactBundlerInstructions.builder()
      .name(bundleName)
      .symbolicName(bundleInfo.bundleSymbolicName)
      .symbolicNameWithOptions(bundleInfo.bundleSymbolicName)
      .version(bundleInfo.bundleVersion.toString)
      .proposedVersion(bundleInfo.bundleVersion.toString)
      .instructions(bndInstructions.asJava)
      .snapshot(false)

    if (srcInputJar.isDefined) {
      bundlerInstructions = bundlerInstructions
        .sourceSymbolicName(bundleInfo.bundleSymbolicName + ".source")
        .sourceName(bundleInfo.bundleSymbolicName + " (sources)")
    }
    
    val shouldBundle = !bundleInfo.isPrebundled || TPConfig.getBundleInstructions(config)(art).isDefined

    val bundler = new AquteBundler(false)
    val bundlerRequest = new ArtifactBundlerRequest(inputJar,
      outputJar,
      srcInputJar.getOrElse(null),
      srcOutputJar.getOrElse(null),
      shouldBundle);

    bundler.execute(bundlerRequest, bundlerInstructions.build())
  }

  def createBundleDescription(art: Artifact): String = {
    if (art.bundle.get.isPrebundled) {
      s"Rebundled ${art.mvnCoordinate} for use within KNIME Big Data target platform."
    } else if (art.isMerged) {
      s"Bundle created by merging maven artifacts ${art.mergedArtifacts.get.map(_.mvnCoordinate).mkString(", ")} " +
        "for use within KNIME Big Data target platform."
    } else {
      s"Bundle created from maven artifact ${art.mvnCoordinate} for use within KNIME Big Data target platform."
    }
  }

  private def mkBundleLicenseHeader(art: Artifact) = {
    val license = art.bundle.get.licenses.head
    s"${license.name};link=${license.url}"
  }

}
