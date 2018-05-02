package com.knime.tpbuilder

import java.io.File

import java.io.FileReader

import scala.collection.mutable.Set
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.annotation.JsonCreator
import java.util.regex.Pattern
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import aQute.lib.osgi.Analyzer

object TPConfigReader {

  case class TPMultiArtifact(var group: String,
    var version: String,
    var artifacts: Seq[String],
    var transitive: Option[Boolean],
    var source: Option[Boolean],
    var transitiveSourceDepth: Int)

  case class TPSingleArtifact(var id: String,
    var transitive: Option[Boolean],
    var source: Option[Boolean],
    var transitiveSourceDepth: Int)

  case class TPArtifactGroup(var name: String,
    var multiArtifacts: Seq[TPMultiArtifact],
    var singleArtifacts: Seq[TPSingleArtifact])

  object TPArtifactGroup {
    
    def toArtifactSet(artGroup: TPArtifactGroup): Set[Artifact] = {
      val set = Set[Artifact]()
      set ++= artGroup.multiArtifacts.flatMap(toArtifactSet)
      set ++= artGroup.singleArtifacts.map(toArtifact)
    }

    def toArtifactSet(multiArt: TPMultiArtifact): Set[Artifact] = {
      Set() ++= multiArt.artifacts.map((artId) => Artifact(
        group = multiArt.group,
        artifact = artId,
        version = multiArt.version))
    }

    def toArtifact(singleArt: TPSingleArtifact): Artifact = {
      singleArt.id.split(":") match {
        case Array(group, artifact, version) =>
          Artifact(group = group,
            artifact = artifact,
            version = version,
            classifier = None)
        case Array(group, artifact, packaging, version) =>
          Artifact(group = group,
            artifact = artifact,
            version = version,
            packaging = packaging,
            classifier = None)
        case Array(group, artifact, packaging, classifier, version) =>
          Artifact(group = group,
            artifact = artifact,
            version = version,
            packaging = packaging,
            classifier = Some(classifier))
        case _ => throw new IllegalArgumentException(s"id ${singleArt.id} is not a valid maven coordinate. " + 
            "Required format: groupId:artifactId:packaging[:classifier]:version")
      }
    }
  }

  case class TPMavenOverride(var coordPattern: String,
    var replacement: String)

  object TPMavenOverride {
    def maybeOverride(overrides: Seq[TPMavenOverride], art: Artifact): Option[String] = {
      val mvnCoord = art.mvnCoordinate

      for (anOverride <- overrides) {
        val matcher = Pattern.compile(anOverride.coordPattern).matcher(mvnCoord)
        if (matcher.matches()) {
          val matchingGroupResolveMap = HashMap[String, String]()
          for (i <- 0 to matcher.groupCount()) {
            matchingGroupResolveMap += (s"$${${i}}" -> matcher.group(i))
          }

          return Some(eval(matchingGroupResolveMap)(anOverride.replacement))
        }
      }

      return None
    }
  }
  
  case class TPMergeOverrides(var whitelist: Seq[String],
      var blacklist: Seq[String])
      
  object TPMergeOverrides {
    def isWhitelisted(overrides: TPMergeOverrides, art: Artifact) = 
      mvnCoordinateMatches(overrides.whitelist, art)
      
    def isBlacklisted(overrides: TPMergeOverrides, art: Artifact) = 
      mvnCoordinateMatches(overrides.blacklist, art)
  }

  case class TPBundleInstruction(var coordPattern: String,
    var fileExcludes: Seq[String],
    var requireBundleInjections: Seq[String],
    var instructions: Map[String, String])
    
  case class TPMavenRepo(var id: String,
    var url: String)
    
  case class TPLicense(var id: Seq[String],
    var licenseName: String,
    var licenseUrl: String)

  case class TPConfig(
    var version: String,
    var source: Option[Boolean],
    var properties: Map[String, String],
    var mavenBlacklist: Seq[String],
    var mavenDependencyOverrides: Seq[TPMavenOverride],
    var mavenDependencyBlacklist: Seq[String],
    var duplicateRemovalBlacklist: Seq[String],
    var requireBundleOverrides: Seq[TPMavenOverride],
    var importPackageOverrides: Seq[TPMavenOverride],
    var mergeOverrides: TPMergeOverrides,
    var bundleInstructions: Seq[TPBundleInstruction],
    var mavenRepositories: Seq[TPMavenRepo],
    var licenses: Seq[TPLicense],
    var artifactGroups: Seq[TPArtifactGroup])

  object TPConfig {

    def isMavenArtifactBlacklisted(config: TPConfig)(art: Artifact) = 
      mvnCoordinateMatches(config.mavenBlacklist, art)

    def isMavenDependencyBlacklisted(config: TPConfig)(dep: Artifact) = 
      mvnCoordinateMatches(config.mavenDependencyBlacklist, dep)
      
    def isDuplicateRemovalBlacklisted(config: TPConfig)(art: Artifact) = 
      mvnCoordinateMatches(config.duplicateRemovalBlacklist, art)

    def getBundleInstructions(config: TPConfig)(art: Artifact): Option[Map[String, String]] = {
      val mvnCoord = art.mvnCoordinate

      for (instruction <- config.bundleInstructions) {
        if (Pattern.compile(instruction.coordPattern).matcher(mvnCoord).matches())
          return Some(instruction.instructions)
      }
      None
    }

    def getBundleFileExcludes(config: TPConfig)(art: Artifact): Seq[String] = {
      val mvnCoord = art.mvnCoordinate

      for (instruction <- config.bundleInstructions) {
        if (Pattern.compile(instruction.coordPattern).matcher(mvnCoord).matches())
          return instruction.fileExcludes
      }
      Seq()
    }
    
    def getRequireBundleInjections(config: TPConfig)(art: Artifact): Seq[String] = {
      val mvnCoord = art.mvnCoordinate

      for (instruction <- config.bundleInstructions) {
        if (Pattern.compile(instruction.coordPattern).matcher(mvnCoord).matches())
          return instruction.requireBundleInjections.map(TPConfig.evalVars(art, config))
      }
      Seq()
    }
    
    def getLicense(config: TPConfig)(art: Artifact): Option[License] = {
      val mvnCoord = art.mvnCoordinate
      config.licenses.filter(_.id.contains(mvnCoord)).headOption match {
        case Some(license) => Some(License(license.licenseName, license.licenseUrl, null, null))
        case _ => None
      }
    }


    def evalVars(art: Artifact, config: TPConfig, instructionMap: Map[String, String]): Map[String, String] = {
      Map() ++= instructionMap.mapValues(evalVars(art, config))
    }

    def evalVars(art: Artifact, config: TPConfig)(someString: String): String = {
      val bundle = art.bundle.get
      val resolveMap = Map(
        "${tp.version}" -> config.version,
        "${bundle.symbolicName}" -> bundle.bundleSymbolicName,
        "${bundle.version}" -> bundle.bundleVersion.toString())

      eval(resolveMap)(someString)
    }
    
    def getArtifactCoordsWithSource(depGraph: Map[Artifact, Set[Artifact]], config: TPConfig): Set[Artifact] = {
      val ret = HashSet[Artifact]()
      
      for (group <- config.artifactGroups) {
        for (singleArt <- group.singleArtifacts) {
          if (singleArt.source.get) {
            val art = TPArtifactGroup.toArtifact(singleArt)
            ret += art
            addTransitiveSources(art, singleArt.transitiveSourceDepth, depGraph, ret)
          }
        }

        for (multiArt <- group.multiArtifacts) {
          if (multiArt.source.get) {
            for (art <- TPArtifactGroup.toArtifactSet(multiArt)) {
              ret += art
              addTransitiveSources(art, multiArt.transitiveSourceDepth, depGraph, ret)
            }
          }
        }
      }

      ret
    }
  }

  private def addTransitiveSources(art: Artifact,
      transitiveSourceDepth: Int,
      depGraph: Map[Artifact, Set[Artifact]],
      alreadyAdded: Set[Artifact]): Unit = {

    alreadyAdded += art

    if (transitiveSourceDepth > 0 || transitiveSourceDepth < 0) {
      for (dep <- depGraph.getOrElse(art, Set())) {
        if (!alreadyAdded(dep))
          addTransitiveSources(dep, transitiveSourceDepth - 1, depGraph, alreadyAdded)
      }
    }
  }

  def read(configFile: String): TPConfig = {
    require(configFile != null, "Configuration file must be provided")
    read(new File(configFile))
  }

  def read(configFile: File): TPConfig = {
    require(configFile.canRead(), "Cannot read configuration file " + configFile.getAbsolutePath)

    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)

    val config = mapper.readValue(new FileReader(configFile), classOf[TPConfig])
    require(config.artifactGroups != null && !config.artifactGroups.isEmpty, "At least one artifactGroup required")

    fixNullsAndValidate(config)
    resolveStrings(config)

    config
  }

  private def fixNullsAndValidate(config: TPConfig) = {
    config.source = Some(config.source.getOrElse(true))
    config.properties = Option(config.properties).getOrElse(Map())
    config.mavenBlacklist = Option(config.mavenBlacklist).getOrElse(Seq())
    config.mavenDependencyBlacklist = Option(config.mavenDependencyBlacklist).getOrElse(Seq())
    config.duplicateRemovalBlacklist = Option(config.duplicateRemovalBlacklist).getOrElse(Seq())
    config.bundleInstructions = Option(config.bundleInstructions).getOrElse(Seq())
    config.mavenRepositories = Option(config.mavenRepositories).getOrElse(Seq())

    require(config.version != null && !config.version.isEmpty, "You need to define a version for your target platform, as it will become part of bundle versions.")

    val numericProps = config.properties.keys.filter(prop => Pattern.compile("[0-9]+").matcher(prop).matches())
    require(numericProps.isEmpty,
      s"Numeric property names are reserved. Please change property name(s) ${numericProps.map(p => s"'${p}'").mkString(", ")} to include letters.")

    require(config.properties.keys.filter(prop => Pattern.compile("bundle\\.symbolicName").matcher(prop).matches()).isEmpty,
      "Property name 'bundle.symbolicName' is reserved. Please use a different property name.")

    require(config.properties.keys.filter(prop => Pattern.compile("bundle\\.version").matcher(prop).matches()).isEmpty,
      "Property name 'bundle.version' is reserved. Please use a different property name.")
      
    require(config.properties.keys.filter(prop => Pattern.compile("tp\\.version").matcher(prop).matches()).isEmpty,
      "Property name 'bundle.version' is reserved. Please use a different property name.")

    config.requireBundleOverrides = Option(config.requireBundleOverrides).getOrElse(Seq())
    config.requireBundleOverrides.foreach((o) =>
      require(o.coordPattern != null && o.replacement != null,
        "Each override under requireBundleOverrides must specify coordPattern regex and a replacement (i.e. a valid clause for the Require-Bundle directive)"))

    config.importPackageOverrides = Option(config.importPackageOverrides).getOrElse(Seq())
    config.importPackageOverrides.foreach((o) =>
      require(o.coordPattern != null && o.replacement != null,
        "Each override under importPackageOverrides must specify coordPattern regex and a replacement (i.e. a valid clause for the Import-Package directive)"))

    config.mavenDependencyOverrides = Option(config.mavenDependencyOverrides).getOrElse(Seq())
    config.mavenDependencyOverrides.foreach((o) =>
      require(o.coordPattern != null && o.replacement != null,
        "Each override under mavenDependencyOverrides must specify coordPattern regex and a replacement"))

    config.mergeOverrides = Option(config.mergeOverrides).getOrElse(TPMergeOverrides(Seq(), Seq()))
    config.mergeOverrides.whitelist = Option(config.mergeOverrides.whitelist).getOrElse(Seq())
    config.mergeOverrides.blacklist = Option(config.mergeOverrides.blacklist).getOrElse(Seq())
        
    config.bundleInstructions.foreach(i => {
      require(i.coordPattern != null || !i.coordPattern.isEmpty,
        "Each bundle instruction must specify a bundleCoordPattern regex")

      i.fileExcludes = Option(i.fileExcludes).getOrElse(Seq())
      i.requireBundleInjections = Option(i.requireBundleInjections).getOrElse(Seq())
      i.instructions = Option(i.instructions).getOrElse(Map())

      require(!i.instructions.isEmpty || i.fileExcludes.isEmpty,
        s"Bundle instruction with pattern ${i.coordPattern} must at least specify a map of instructions or a list of fileExcludes")

      i.instructions.keys.foreach(instrName => {
        require(instrName != Analyzer.BUNDLE_LICENSE, 
            s"Bundle instructions for ${i.coordPattern} specify a Bundle-License. Please use the licenses section in the YAML config instead.")
      })
    })
    
    config.mavenRepositories.foreach(r => {
      require(r.id != null && !r.id.isEmpty,
        "For each maven repository you have to specify a unique id")
      require(r.url != null && !r.url.isEmpty,
        "For each maven repository you have to specify a URL")
        
      require(config.mavenRepositories.map(_.id).toSet.size == config.mavenRepositories.size,
          "The IDs of your maven repositories are not unique.")
    })

    config.licenses = Option(config.licenses).getOrElse(Seq())
    config.licenses.foreach(l => {
      require(l.id != null, "There is a license entry that does not specify a list of maven coordinates (id attribute).")
      require(!l.id.isEmpty, "There is a license entry that does not specify a list of maven coordinates (id attribute).")
      require(l.licenseName != null, "There is a license entry that does not specify a license name (name attribute).")
      require(l.licenseUrl != null, "There is a license entry that does not specify a license URL (url attribute).")
    })

    config.artifactGroups = Option(config.artifactGroups).getOrElse(Seq())

    config.artifactGroups.foreach((g) => {
      require(g.name != null, "Each artifact group must have a name")

      g.singleArtifacts = Option(g.singleArtifacts).getOrElse(Seq())
      g.multiArtifacts = Option(g.multiArtifacts).getOrElse(Seq())

      require(!g.multiArtifacts.isEmpty || !g.singleArtifacts.isEmpty,
        "Each artifact group must specify at least one multiArtifact or singleArtifact")

      g.singleArtifacts.foreach((a) => {
        a.source = Some(a.source.getOrElse(true))
        a.transitive = Some(a.transitive.getOrElse(true))
        require(a.id != null && !a.id.isEmpty, s"In artifactGroup ${g.name} there is a singleArtifact without an id (i.e. maven coordinate)")
      })

      g.multiArtifacts.foreach((m) => {
        m.source = Some(m.source.getOrElse(true))
        m.transitive = Some(m.transitive.getOrElse(true))
        require(m.group != null && !m.group.isEmpty, s"In artifactGroup ${g.name} there is a multiArtifact without a group ID")
        require(m.version != null && !m.version.isEmpty, s"In artifactGroup ${g.name} there is a multiArtifact without a version")
        require(m.artifacts != null && !m.artifacts.isEmpty, s"In artifactGroup ${g.name} there is a multiArtifact without any artifact IDs")
      })
    })
  }

  private def resolveStrings(config: TPConfig) = {

    val resolveMap = Map[String, String]()
    config.properties.foreach((entry: Tuple2[String, String]) => resolveMap += (s"$${${entry._1}}" -> entry._2))
    resolveMap += "${tp.version}" -> config.version

    config.mavenBlacklist = config.mavenBlacklist.map(eval(resolveMap))

    config.mavenDependencyBlacklist = config.mavenDependencyBlacklist.map(eval(resolveMap))

    config.requireBundleOverrides.foreach((o) => {
      o.coordPattern = eval(resolveMap)(o.coordPattern)
      o.replacement = eval(resolveMap)(o.replacement)
    })

    config.importPackageOverrides.foreach((o) => {
      o.coordPattern = eval(resolveMap)(o.coordPattern)
      o.replacement = eval(resolveMap)(o.replacement)
    })

    config.mavenDependencyOverrides.foreach((o) => {
      o.coordPattern = eval(resolveMap)(o.coordPattern)
      o.replacement = eval(resolveMap)(o.replacement)
    })
    
    config.mergeOverrides.whitelist = config.mergeOverrides.whitelist.map(eval(resolveMap))
    config.mergeOverrides.blacklist = config.mergeOverrides.blacklist.map(eval(resolveMap))

    config.bundleInstructions.foreach(i => {
      i.coordPattern = eval(resolveMap)(i.coordPattern)
      i.fileExcludes = i.fileExcludes.map(eval(resolveMap))
      i.instructions = Map() ++= i.instructions.mapValues(eval(resolveMap))
    })

    config.artifactGroups.foreach((g) => {
      g.singleArtifacts.foreach((sa) => {
        sa.id = eval(resolveMap)(sa.id)
      })

      g.multiArtifacts.foreach((ma) => {
        ma.group = eval(resolveMap)(ma.group)
        ma.version = eval(resolveMap)(ma.version)
        ma.artifacts = ma.artifacts.map(eval(resolveMap))
      })
    })
  }

  private def eval(properties: Map[String, String])(toEval: String): String = {
    var scratch = toEval

    for (property <- properties.keys) {
      scratch = scratch.replaceAllLiterally(property, properties(property))
    }
    scratch
  }

  private def evalOption(properties: Map[String, String])(toEval: Option[String]): Option[String] = {
    toEval match {
      case Some(str) => Some(eval(properties)(str))
      case None => None
    }
  }
  
  private def mvnCoordinateMatches(regexes: Seq[String], art: Artifact): Boolean = {
      val mvnCoord = art.mvnCoordinate
      
      for (regex <- regexes) {
        if (Pattern.compile(regex).matcher(mvnCoord).matches()) {
          return true
        }
      }
      
      return false
  }
}