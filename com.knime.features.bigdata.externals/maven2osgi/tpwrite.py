#!/usr/bin/env python3
import subprocess
import os
import collections
import shutil
import zipfile
import sys

tmpFolder = os.path.sep.join([os.path.dirname(__file__), "tmp"])
if not os.path.exists(tmpFolder):
    os.mkdir(tmpFolder)

Artifact = collections.namedtuple("Artifact", [ "group", "artifact", "version", "classifier", "bundleSymbolicName", "bundleVersion", "isBundled"])

directDeps = [ 
    Artifact("org.apache.spark", "spark-core_2.11", "2.0.2", None, None, None, None),
    Artifact("org.apache.spark", "spark-sql_2.11", "2.0.2", None, None, None, None),
    Artifact("org.apache.spark", "spark-mllib_2.11", "2.0.2", None, None, None, None),
    Artifact("org.apache.spark", "spark-hive_2.11", "2.0.2", None, None, None, None)
 ]
 
def get_level(line):
    level = line.find("\\-")
    
    if level == -1:
        level = line.find("+-")
        
    return level // 3
        

def mvnversion2osgi(mvnversion):
    v = []
    curr = None
    lastDigit = False
    lastChar = False
    for c in mvnversion:
        if c.isdigit():
            if lastDigit:
                curr += c
            else:
                if curr != None:
                    v.append(curr)
                curr = c
                
            lastDigit = True
            lastChar = False
        elif c.isalpha():
            if lastChar:
                curr += c
            else:
                if curr != None:
                    v.append(curr)
                curr = c

            lastDigit = False
            lastChar = True
        else:
            if curr != None:
                v.append(curr)
            
            curr = None
            lastDigit = False
            lastChar = False
    
    if curr != None:
        v.append(curr)
        
    major = 0
    minor = 0
    micro = 0
    qualifier = ""
            
    if v[0].isdigit():
        major = int(v[0])
    else:
        qualifier = "".join(v[0:])
        return "%d.%d.%d.%s" % (major, minor, micro, qualifier)
    
    if (len(v) > 1) and (v[1].isdigit()):
        minor = int(v[1])
    elif len(v) > 1:
        qualifier = "".join(v[1:])
        return "%d.%d.%d.%s" % (major, minor, micro, qualifier)

    if (len(v) > 2) and (v[2].isdigit()):
        micro = int(v[2])
    elif len(v) > 2:
        qualifier = "".join(v[2:])
        return "%d.%d.%d.%s" % (major, minor, micro, qualifier)
        
    if len(v) > 3:
        qualifier = "".join(v[3:])
        return "%d.%d.%d.%s" % (major, minor, micro, qualifier)
        
    return "%d.%d.%d" % (major, minor, micro)

def resolve_artifacts():
    retcode = subprocess.run(["mvn", 
        "dependency:resolve"], stdout=subprocess.PIPE).returncode
    
    if retcode > 0:
        raise Exception("Failed to resolve dependencies" % artifact)

def get_artifact_path(artifact):
    if artifact.classifier == None:
        jarFilename = "-".join([artifact.artifact, artifact.version]) + ".jar"
    else:
        jarFilename = "-".join([artifact.artifact, artifact.version, artifact.classifier]) + ".jar"

    return os.path.sep.join([os.path.expanduser("~"), 
        ".m2",
        "repository",
        os.path.sep.join(artifact.group.split(".")),
        artifact.artifact,
        artifact.version,
        jarFilename])

def try_osgi_manifest(artifact):
    from jarmanifest import manifest

    artifactPath = get_artifact_path(artifact)

    tmpManifest = os.path.sep.join([tmpFolder, "MANIFEST.MF"])
    with zipfile.ZipFile(artifactPath) as z:
        if not ("META-INF/MANIFEST.MF" in z.namelist()):
            return (None, None)

        with z.open("META-INF/MANIFEST.MF") as src, open(tmpManifest, 'wb') as tgt:
            shutil.copyfileobj(src,tgt)
    
    m = manifest.getAttributes(tmpManifest)
    os.remove(tmpManifest)
    
    bsn = None
    if "Bundle-SymbolicName" in m:
        bsn = m["Bundle-SymbolicName"].split(";")[0].strip()

    bv = None
    if "Bundle-Version" in m:
        bv = m["Bundle-Version"].strip()
    
    return (bsn, bv)
    
def find_first_package_with_classes(artifact):
    artifactPath = get_artifact_path(artifact)

    with zipfile.ZipFile(artifactPath) as z:
        # entries look like this for example: [ 'org/', 'org/znerd/', 'org/znerd/xmlenc/', 'org/znerd/xmlenc/InvalidXMLException.class' ]
        shortestPackageWithClassfile = None
        shortestPackageLength = sys.maxsize
        
        for entry in z.namelist():
            if entry.endswith(".class"):
                splits = entry.split("/")
                if (len(splits) == 1):
                    return None
                    
                # cut away the filename
                splits = splits[0:-1]
                if len(splits) < shortestPackageLength:
                    shortestPackageWithClassfile = ".".join(splits)
                    shortestPackageLength =  len(splits)
    
        return shortestPackageWithClassfile

    
def with_osgi_name_and_version(artifact):
    # This reimplements the rules for bundle name generation described here:
    # http://felix.apache.org/documentation/subprojects/apache-felix-maven-bundle-plugin-bnd.html
    bundleSymbolicName = None    
    bundleVersion = None
    
    # if artifact.getFile is not null and the jar contains a OSGi Manifest with Bundle-SymbolicName property then that value is returned
    (bundleSymbolicName, bundleVersion) = try_osgi_manifest(artifact)
    isBundled = bundleSymbolicName != None
    
    if bundleSymbolicName == None:
        artId = artifact.artifact.lower()
        groupSplits = artifact.group.lower().split(".")
        
        # if groupId has only one section (no dots) and artifact.getFile is not null then the first package name
        # with classes is returned. eg. commons-logging:commons-logging -> org.apache.commons.logging
        if len(groupSplits) == 1:
            bundleSymbolicName = find_first_package_with_classes(artifact)
        elif artId == groupSplits[-1]:
            # if artifactId is equal to last section of groupId then groupId is returned. eg. org.apache.maven:maven -> org.apache.maven
            bundleSymbolicName = artifact.group.lower()
        elif artId.startswith(groupSplits[-1]):
            # if artifactId starts with last section of groupId that portion is removed. eg. org.apache.maven:maven-core -> org.apache.maven.core 
            artId = artId[len(groupSplits[-1]):]
            while not artId[0].isalnum():
                artId = artId[1:]
            bundleSymbolicName = artifact.group.lower() + "." + artId
        else:
            # default case: Get the symbolic name as groupId + "." + artifactId
            bundleSymbolicName = artifact.group.lower() + "." + artId
    
    if bundleVersion == None:
        bundleVersion = mvnversion2osgi(artifact.version)
    
    return artifact._replace(bundleSymbolicName=bundleSymbolicName, bundleVersion = bundleVersion, isBundled=isBundled)

def get_artifact(line):
    splits = line.split(":")
    
    if len(splits) == 5:
        return with_osgi_name_and_version(Artifact(group=splits[0], 
            artifact=splits[1], 
            version=splits[3],
            classifier=None,
            bundleSymbolicName=None,
            bundleVersion=None,
            isBundled=False))
    elif len(splits) == 6:
        return with_osgi_name_and_version(Artifact(group=splits[0], 
            artifact=splits[1], 
            version=splits[4],
            classifier=splits[3],
            bundleSymbolicName=None,
            bundleVersion=None,
            isBundled=False))    
 
def get_dep_tree(pomfile):
    resolve_artifacts()
    output = subprocess.check_output(["mvn", "-f", pomfile, "dependency:tree"]).decode()
    
    tree = {}
    stack = []
    level = -1

    preScan = True
    skip = False
    for line in output.splitlines():
        if preScan:
            if line.startswith("[INFO] --- maven-dependency-plugin:"):
                preScan = False
                skip = True
            continue
            
        if skip:
            skip = False
            continue
            
        # cut away [INFO] stuff
        line = line[7:]
        
        newLevel = get_level(line)
        if newLevel < 0:
            break
        
        artifact = get_artifact(line[newLevel*3 + 3:])
        tree.setdefault(artifact, set())
        
        if newLevel < level:
            while newLevel < level:
                level -= 1
                stack.pop()
            stack[level] = artifact
        elif newLevel > level:
            if newLevel > level+1:
                raise Exception("More than one level indented")
            stack.append(artifact)
            level += 1
        else:
            stack[level] = artifact
            
        if level > 0:
            tree[stack[-2]].add(stack[-1])

    return tree

def write_pom_with_replacements(pomTemplateFile, pomTargetFile, replacements):
    with open(pomTemplateFile) as f:
        tmpl = f.read()

    toWrite = tmpl
    for (replaceKey, replaceValue) in replacements.items():
        toWrite = toWrite.replace(replaceKey, replaceValue)

    with open(pomTargetFile, "w") as f:
        f.write(toWrite)


def write_pom(dependencies):
    depTmpl = "<dependency><groupId>%s</groupId><artifactId>%s</artifactId><version>%s</version></dependency>"
    
    depsXml = ""
    for depArt in dependencies:
        depXml = depTmpl % (depArt.group, depArt.artifact, depArt.version)
        depsXml = "\n".join([depsXml, depXml])
    
    
    write_pom_with_replacements("pom.xml.tmpl", "pom.xml", {"###DEPS###" : depsXml })
        

def get_deps_by_group(deptree):
    deps_by_group = {}

    for artifact in deptree.keys():
        deps_by_group.setdefault(artifact.group, set())
        deps_by_group[artifact.group].add(artifact)
        
    return deps_by_group
    
def propose_merged_bundle_symbolic_name(artifacts):
    art_bsns = list(map(lambda art: art.bundleSymbolicName, artifacts))
    common_bsn_prefix = os.path.commonprefix(art_bsns)
    if common_bsn_prefix.endswith("."):
        common_bsn_prefix = common_bsn_prefix[:-1]
        
    common_bsn_suffix = os.path.commonprefix(list(map(lambda bsn: bsn[::-1], art_bsns)))[::-1]
    return common_bsn_prefix + common_bsn_suffix

def get_artifacts_to_merge(deps_by_group):
    to_merge = {}
    
    for group in deps_by_group.keys():
        # filter out those maven artifacts from merging that are already bundled
        # those must not be merged with others as this may break the bundles
        artsToMerge = set(filter(lambda art: not art.isBundled, deps_by_group[group]))

        if len(artsToMerge) < 2:
            continue

        merged_bsn = propose_merged_bundle_symbolic_name(artsToMerge)
        to_merge[Artifact(group=group, 
            artifact=None, 
            version=list(artsToMerge)[0].version, 
            classifier=None,
            bundleSymbolicName=merged_bsn,
            bundleVersion=list(artsToMerge)[0].bundleVersion,
            isBundled=False)] = artsToMerge

    return to_merge


def get_deps_to_merge(deptree):
    deps_by_group = get_deps_by_group(deptree)
    to_merge = get_artifacts_to_merge(deps_by_group)
    return to_merge
    

def merge_deps(deptree, to_merge):
    # merging means that we have new merged artifacts that
    # completely take the place of the artifacts they contain

    for merged in to_merge:
        merged_deps = set()
        for art in to_merge[merged]:
            # remove the dependencies of art from deptree
            # and collect them in merged_deps
            for artdep in deptree[art]:
                merged_deps.add(artdep)
            del deptree[art]
            
            # every artifact that depends on art must now depend on merged
            for artDeps in deptree.values():
                for mergedArt in merged_deps:
                    if mergedArt in artDeps:
                        artDeps.remove(mergedArt)
                        artDeps.add(merged)

        # remove the merged artifacts from the set of merged
        # dependencies (because the merged artifacts might depend on each other)
        for art in to_merge[merged]:
            merged_deps.discard(art)

        deptree[merged] = merged_deps


def build_bundle(artifact, deptree, merged, buildfolder):
    pomTemplate = None
    replacements = {}
    dependencies = None
    
    if artifact in merged:
        pomTemplate = "pom_build.xml.tmpl"
        dependencies = merged[artifact]
    elif artifact.isBundled:
        pomTemplate = "pom_buildbundled.xml.tmpl"
        dependencies = [artifact]
    else:
        pomTemplate = "pom_build.xml.tmpl"
        dependencies = [artifact]
    
    # build dependencies XML
    depTmpl = "<dependency><groupId>%s</groupId><artifactId>%s</artifactId><version>%s</version></dependency>"
    depTmplWithClassifier = "<dependency><groupId>%s</groupId><artifactId>%s</artifactId><version>%s</version><classifier>%s</classifier></dependency>"
    depXmls = []
    for depArt in dependencies:
        if depArt.classifier == None:
            depXml = depTmpl % (depArt.group, depArt.artifact, depArt.version)
        else:
            depXml = depTmplWithClassifier % (depArt.group, depArt.artifact, depArt.version, depArt.classifier)
        depXmls.append(depXml)
    replacements["###DEPS###"] = "\n".join(depXmls)

    replacements["###VERSION###"] = artifact.bundleVersion
    replacements["###BSN###"] = artifact.bundleSymbolicName

    # build Require-Bundle MANIFEST.MF header
    requires = []
    for depArt in deptree[artifact]:
        requiresEntry = "%s;bundle-version=\"%s\"" % (depArt.bundleSymbolicName, depArt.bundleVersion)
        requires.append(requiresEntry)
    replacements["###REQUIRES###"] = ",".join(requires)

    write_pom_with_replacements(pomTemplate, os.path.sep.join([buildfolder, "pom.xml"]), replacements)
    
    completedProcess = subprocess.run(["mvn", "org.apache.felix:maven-bundle-plugin:bundle"],
        cwd=buildfolder,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE)

    if completedProcess.returncode > 0:
        raise Exception("Failed to build OSGI bundle %s_%s. Message: %s" % (artifact.bundleSymbolicName,
            artifact.bundleVersion,
            completedProcess.stdout))
            
def run():
    print("Determining dependency tree")
    write_pom(directDeps)
    deptree = get_dep_tree("pom.xml")
    print("Got dependency tree with %d artifacts" % len(deptree))
    
    print("Merging artifacts by group where possible")
    to_merge = get_deps_to_merge(deptree)
    merge_deps(deptree, to_merge)
    print("Got merged dependency tree with %d artifacts" % len(deptree))
    
    for artifact in deptree.keys():
        print("Building OSGI bundle %s_%s from maven artifact %s:%s:%s" % (artifact.bundleSymbolicName,
            artifact.bundleVersion,
            artifact.group,
            artifact.artifact,
            artifact.version))
            
        build_bundle(artifact, deptree, to_merge, tmpFolder)
        os.rename(
            os.path.sep.join([tmpFolder, "target", "targetplatform-%s.jar" % artifact.bundleVersion]), 
            os.path.sep.join(["plugins", "%s_%s.jar" % (artifact.bundleSymbolicName, artifact.bundleVersion)]))
        shutil.rmtree(os.path.join(tmpFolder, "target"))
        os.rename(os.path.join(tmpFolder, "pom.xml"), os.path.join(tmpFolder, "%s_%s_pom.xml" % (artifact.bundleSymbolicName, artifact.bundleVersion)))
