package com.knime.tpbuilder

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import scala.collection.mutable.HashSet
import org.osgi.framework.Version
import com.knime.tpbuilder.TPConfigReader.TPConfig
import java.util.regex.Pattern
import com.knime.tpbuilder.osgi.OsgiUtil
import aQute.lib.osgi.Analyzer
import com.knime.tpbuilder.TPConfigReader.TPMergeOverrides

object Merger {

  def merge(depGraph: Map[Artifact, Set[Artifact]], config: TPConfig) = {
    for (mergeUnit <- computeMergeableUnits(depGraph.keys, config)) {
      
      var mergedArt = Artifact(group = mergeUnit.commonGroup,
        artifact = "merged" + (mergeUnit.commonScalaVersion match {
            case Some(scalaVer) => "_" + scalaVer
            case None => ""
          }),
        version = mergeUnit.commonVersion,
        isMerged = true,
        mergedArtifacts = Some(mergeUnit.artifacts))
        
      
      val mergedBundleName = proposeMergedBundleSymbolicName(mergedArt, mergeUnit, config)
      val mergedBundleVersion = proposeMergedBundleVersion(mergedArt, mergeUnit, config)
      

      if (mergeUnit.artifacts.groupBy(_.bundle.get.licenses).size != 1) {
        System.err.println(s"  NOT MERGING due to mixed licenses: ${mergedArt.mvnCoordinate} (bundle: ${mergedArt.bundle.get.bundleSymbolicName} version: ${mergedArt.bundle.get.bundleVersion.toString}):")
        System.err.println(mergeUnit.artifacts.map(artToMerge => s"    ${artToMerge.mvnCoordinate} (Licenses: ${artToMerge.bundle.get.licenses.map(_.toString).mkString(", ")})").mkString("\n"))
      } else {
        mergedArt = OsgiUtil.withBundle(mergedArt, 
            BundleInfo(mergedBundleName, 
                Version.parseVersion(mergedBundleVersion), 
                false, 
                mergeUnit.artifacts.head.bundle.get.licenses,
                mergeUnit.artifacts.head.bundle.get.vendor,
                mergeUnit.artifacts.head.bundle.get.docUrl))
  
        println(s"  Merge info: ${mergedArt.mvnCoordinate} (bundle: ${mergedArt.bundle.get.bundleSymbolicName} version: ${mergedArt.bundle.get.bundleVersion.toString}):\n" + 
            mergeUnit.artifacts.map(artToMerge => "    " + artToMerge.mvnCoordinate).mkString("\n"))
  
        doReplace(mergeUnit.artifacts, mergedArt, depGraph)
      }
    }

    Base.assertValidDepGraph(depGraph, config)
  }

  case class MergeableUnit(
    commonGroup: String,
    commonVersion: String,
    commonScalaVersion: Option[String],
    artifacts: Set[Artifact])

  def computeMergeableUnits(artifacts: Iterable[Artifact], config: TPConfig): Seq[MergeableUnit] = {
    // group, version, scalaVersion (None for Java artifacts)
    val units = HashMap[(String, String, Option[String]), Set[Artifact]]()

    for (art <- artifacts) {
      // only consider artifacts for merging that are not prebundled
      
    	def isNeitherPrebundledNorBlacklisted = !(art.bundle.get.isPrebundled || TPMergeOverrides.isBlacklisted(config.mergeOverrides, art))
      def isPrebundledAndWhitelisted = art.bundle.get.isPrebundled && TPMergeOverrides.isWhitelisted(config.mergeOverrides, art)
      
      if (isNeitherPrebundledNorBlacklisted || isPrebundledAndWhitelisted) {
        units.getOrElseUpdate((art.group, art.version, deriveScalaVersion(art)), HashSet[Artifact]()).add(art)
      }
    }
    
    

    units
      .filter(entry => entry._2.size > 1)
      .map(entry => MergeableUnit(entry._1._1, entry._1._2, entry._1._3, entry._2))
      .toSeq
  }

  private val scalaVersionPattern = Pattern.compile(".+_(2\\.[0-9]*)")

  private def deriveScalaVersion(art: Artifact): Option[String] = {
    val matcher = scalaVersionPattern.matcher(art.artifact)
    if (matcher.matches()) {
      Some(matcher.group(1))
    } else {
      None
    }
  }

  private def longestCommonPrefix(strings: Iterable[String]): String = {
    strings.foldLeft(strings.head)((l, r) => (l, r).zipped.takeWhile(Function.tupled(_ == _)).map(_._1).mkString)
  }

  private def proposeMergedBundleSymbolicName(mergedArt: Artifact, mergeUnit: MergeableUnit, config: TPConfig): String = {
    
    val defaultMergedBundleName = computeDefaultMergedBundleSymblicName(mergeUnit)
    
    TPConfig.getBundleInstructions(config)(mergedArt) match {
      case Some(instr) =>
        instr.getOrElse(Analyzer.BUNDLE_SYMBOLICNAME, defaultMergedBundleName)
      case None =>
        defaultMergedBundleName
    }
  }
  
  def proposeMergedBundleVersion(mergedArt: Artifact, mergeUnit: MergeableUnit, config: TPConfig): String = {
    val defaultMergedBundleVersion = OsgiUtil.addKNIMEVersionSuffix(mergeUnit.artifacts.head.bundle.get.bundleVersion.toString,
        config.version)
        
    TPConfig.getBundleInstructions(config)(mergedArt) match {
      case Some(instr) =>
        if(instr.contains(Analyzer.BUNDLE_VERSION))
            OsgiUtil.addKNIMEVersionSuffix(instr(Analyzer.BUNDLE_VERSION), config.version)
        else
          defaultMergedBundleVersion
      case None =>
        defaultMergedBundleVersion
    }
  }
   
  private def computeDefaultMergedBundleSymblicName(mergeUnit: MergeableUnit): String = {
    val bundleNames = mergeUnit.artifacts.map(_.bundle.get.bundleSymbolicName)

    val commonPrefix = longestCommonPrefix(bundleNames)
    // val commonSuffix = longestCommonPrefix(bundleNames.map(_.reverse)).reverse
    val commonSuffix = mergeUnit.commonScalaVersion match {
      case Some(ver) => "_" + ver
      case None => ""
    }

    if (Set(".", "-").contains(commonPrefix.last.toString)) {
      commonPrefix.substring(0, commonPrefix.length() - 1) + "-merged" + commonSuffix
    } else {
      // commonPrefix.substring(0, Math.max(commonPrefix.lastIndexOf("."), commonPrefix.lastIndexOf("-"))) + commonSuffix
      commonPrefix + "-merged" + commonSuffix
    }
  }

  def doReplace(artsToReplace: Iterable[Artifact], replacement: Artifact, depGraph: Map[Artifact, Set[Artifact]]) = {

    // first we compute the dependencies of the replacement (i.e. merged) artifact
    // these are collected from the dependencies of the artifacts to replace
    val depsOfReplacement = HashSet[Artifact]()
    for (toReplace <- artsToReplace) {
      depsOfReplacement ++= depGraph(toReplace)
      depGraph -= toReplace
    }
    // make sure the dependencies of the replacement (i.e. merged) artifact
    // do not contain any artifacts to replace
    depsOfReplacement --= artsToReplace

    for (someDeps <- depGraph.values) {
      val sizeBefore = someDeps.size
      // remove the artifacts to replace from each set of dependencies in the dependency graph
      someDeps --= artsToReplace

      // detect whether we actually removed something, and if so, we need to
      // add the replacement artifact as a dependency
      if (someDeps.size < sizeBefore)
        someDeps += replacement
    }

    depGraph += (replacement -> depsOfReplacement)
  }
}