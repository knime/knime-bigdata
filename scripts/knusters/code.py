import web
import os
import subprocess
import shutil
import string

render = web.template.render('templates/')

CLUSTER_DIR = '/home/bjoern/.clusters'
CLUSTER_SCRIPT_HOME = '/home/bjoern/knusters/cluster'
CLUSTERS = ['cdh-5.3', 'cdh-5.4', 'cdh-5.5', 'cdh-5.6', 'cdh-5.7', 'cdh-5.8', 'cdh-5.7-secured', 'hdp-2.4.2-secured']

urls = (
    '/(.*)', 'Index'
)

def short_cluster_name(clusterFolder):
    return string.join(clusterFolder.split('-')[0:-2], '-')

def read_clusters():
    if not os.path.isdir(CLUSTER_DIR):
        return []

    clusters = []

    for clusterFolder in os.listdir(CLUSTER_DIR):
        cluster = { 'name' : clusterFolder, 'shortName' : short_cluster_name(clusterFolder) }

        with open("%s/%s/%s" % (CLUSTER_DIR, clusterFolder, "master")) as masterFile:
            masterline = masterFile.readline().lstrip().rstrip()
            if (len(masterline) > 0) and (masterline != "null"):
                cluster['master'] = masterline
        
        workers = []
        with open("%s/%s/%s" % (CLUSTER_DIR, clusterFolder, "worker")) as workerFile:
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
        print("Refreshing cluster " + cluster)
        try:
            subprocess.check_call(CLUSTER_SCRIPT_HOME + '/refresh-cluster-public-ips ' + cluster,
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
            clusters = read_clusters()
            if command == "ansible":
                return render.index_ansible(clusters)
            else:
                return render.index(clusters)

if __name__ == "__main__": 
    app = web.application(urls, globals())
    app.run()
