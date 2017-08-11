#
# KNusters webapp written with web.py (http://webpy.org/)
#
# 
# 
# Prerequisites: Install web.py via pip:
#   $ pip install web.py
#

import web
import os
import subprocess
import shutil
import string

render = web.template.render('templates/')

CLUSTER_DIR = '/home/bjoern/.clusters'
CLUSTER_SCRIPT_HOME = '../cluster'
CLUSTERS = [ ('cdh-5.3','dev'), 
	('cdh-5.4', 'dev'), 
	('cdh-5.5', 'dev'), 
	('cdh-5.6', 'dev'),
	('cdh-5.7', 'dev'),
	('cdh-5.8', 'dev'),
	('cdh-5.7-secured', 'dev'),
	('cdh-5.8-secured', 'dev'),
	('cdh-5.9-secured', 'dev'),
	('cdh-5.10-secured', 'dev'),
	('cdh-5.11-secured', 'dev'),
	('cdh-5.12-secured', 'dev'),
	('hdp-2.4.2-secured', 'dev'),
	('hdp-2.5-secured', 'dev'),
	('cdh-5.8-train', 'train')]

urls = (
    '/(.*)', 'Index'
)

def short_cluster_name(clusterFolder):
    return string.join(clusterFolder.split('-')[0:-2], '-')

def read_clusters(privateIps = False):
    if not os.path.isdir(CLUSTER_DIR):
        return []

    clusters = []

    for clusterFolder in os.listdir(CLUSTER_DIR):
        cluster = { 'name' : clusterFolder, 'shortName' : short_cluster_name(clusterFolder) }


	masterFileName = "master"
	if privateIps:
	    masterFileName = "masterPrivate"

        with open("%s/%s/%s" % (CLUSTER_DIR, clusterFolder, masterFileName)) as masterFile:
            masterline = masterFile.readline().lstrip().rstrip()
            if (len(masterline) > 0) and (masterline != "null"):
                cluster['master'] = masterline
	
	
	workerFileName = "worker"
	if privateIps:
	    workerFileName = "workerPrivate"
        
        workers = []
        with open("%s/%s/%s" % (CLUSTER_DIR, clusterFolder, workerFileName)) as workerFile:
            for workerLine in workerFile.xreadlines():
                workerLineStripped = workerLine.lstrip().rstrip()
                if (len(workerLineStripped) > 0) and (workerLineStripped != "null"):
                    workers.append(workerLineStripped)
        
        cluster['workers'] = workers
        
        if 'master' in cluster:
            clusters.append(cluster)

    clusters.sort(key=lambda cluster: cluster['name'])
    
    return clusters

def refresh_clusters():
    shutil.rmtree(CLUSTER_DIR, ignore_errors=True)
    failed = []
    
    for cluster in CLUSTERS:
        print("Refreshing cluster " + str(cluster))
        try:
            subprocess.check_call(CLUSTER_SCRIPT_HOME + ("/refresh-cluster-ips %s %s" % cluster),
                shell=True)
        except:
            failed.append(cluster)
            
    return failed

class Index:
    def GET(self, command):
        
        if command == "refresh":
            refresh_clusters()
            raise web.seeother("/")
        else:
            if command == "ansible":
	        clusters = read_clusters()
                return render.index_ansible(clusters)
            elif command == "ansiblePrivate":
	        clusters = read_clusters(privateIps=True)
                return render.index_ansible(clusters)
            elif command == "private":
	        clusters = read_clusters(privateIps=True)
                return render.index_ansible(clusters)
            else:
	        clusters = read_clusters()
                return render.index(clusters)

if __name__ == "__main__": 
    app = web.application(urls, globals())
    app.run()
