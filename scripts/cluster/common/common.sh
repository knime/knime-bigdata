export DEFAULT_CNAME=cdh-5.3
export DEFAULT_CENV=dev
export DEFAULT_COWNER=bjoern

export BASE_DIR="${HOME}/.clusters"

# Arguments: name, environment, owner
define_locations() {
	export METADIR="${BASE_DIR}/$1-$2-$3"

	# file locations
	export ALL_JSON="$METADIR/json-all"
	export ENV_JSON="$METADIR/json-env"
	export CLUSTER_JSON="$METADIR/json-cluster"
	export WORKER_JSON="$METADIR/json-worker"
	export MASTER_JSON="$METADIR/json-master"
	export MANAGER_JSON="$METADIR/json-manager"

	export ENV_HOSTS="$METADIR/env"

	export CLUSTER_HOSTS="$METADIR/cluster"
	export CLUSTER_PRIVATE_HOSTS="$METADIR/clusterPrivate"

	export WORKER_HOSTS="$METADIR/worker"
	export WORKER_PRIVATE_HOSTS="$METADIR/workerPrivate"

	export MASTER_HOSTS="$METADIR/master"
	export MASTER_PRIVATE_HOSTS="$METADIR/masterPrivate"

	export MANAGER_HOSTS="$METADIR/manager"
	export MANAGER_PRIVATE_HOSTS="$METADIR/managerPrivate"
}

