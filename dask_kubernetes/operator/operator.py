import asyncio

from datetime import datetime, timedelta
import copy

import kopf
import kubernetes_asyncio as kubernetes
from distributed.core import rpc

from uuid import uuid4

from dask_kubernetes.common.auth import ClusterAuth


def build_scheduler_pod_spec(name, spec):
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": f"{name}-scheduler",
            "labels": {
                "dask.org/cluster-name": name,
                "dask.org/component": "scheduler",
                "sidecar.istio.io/inject": "false",
            },
        },
        "spec": {
            "selector": {
                "matchLabels": {
                    "dask.org/cluster-name": name,
                    "dask.org/component": "scheduler",
                }
            },
            "template": {
                "metadata": {
                    "labels": {
                        "dask.org/cluster-name": name,
                        "dask.org/component": "scheduler",
                    }
                },
                "spec": spec,
            },
        },
    }


def build_scheduler_service_spec(name, spec):
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": f"{name}-service",
            "labels": {
                "dask.org/cluster-name": name,
            },
        },
        "spec": spec,
    }


def build_worker_pod_spec(worker_group_name, namespace, cluster_name, uuid, spec):
    worker_name = f"{worker_group_name}-worker-{uuid}"
    worker_spec = copy.deepcopy(spec)

    pod_spec = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": worker_name,
            "labels": {
                "dask.org/cluster-name": cluster_name,
                "dask.org/workergroup-name": worker_group_name,
                "dask.org/component": "worker",
                "sidecar.istio.io/inject": "false",
            },
        },
        "spec": worker_spec,
    }
    for i in range(len(pod_spec["spec"]["containers"])):
        pod_spec["spec"]["containers"][i]["env"].extend(
            [
                {
                    "name": "DASK_WORKER_NAME",
                    "value": worker_name,
                },
                {
                    "name": "DASK_SCHEDULER_ADDRESS",
                    "value": f"tcp://{cluster_name}-service.{namespace}.svc.cluster.local:8786",
                },
            ]
        )
    return pod_spec


def build_worker_group_spec(name, spec):
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskWorkerGroup",
        "metadata": {"name": f"{name}-default-worker-group"},
        "spec": {
            "cluster": name,
            "worker": spec,
        },
    }


def build_cluster_spec(name, worker_spec, scheduler_spec):
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskCluster",
        "metadata": {"name": name},
        "spec": {"worker": worker_spec, "scheduler": scheduler_spec},
    }


def build_autoscaler_spec(cluster_name, minimum, maximum):
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskWorkerGroup",
        "metadata": {"name": "dask-autoscaler"},
        "spec": {
            "cluster": cluster_name,
            "minimum": minimum,
            "maximum": maximum,
        },
    }


async def wait_for_service(api, service_name, namespace):
    """Block until service is available."""
    while True:
        try:
            await api.read_namespaced_service(service_name, namespace)
            break
        except Exception:
            await asyncio.sleep(0.1)


@kopf.on.startup()
async def startup(**kwargs):
    await ClusterAuth.load_first()


@kopf.on.create("daskcluster")
async def daskcluster_create(spec, name, namespace, logger, **kwargs):
    logger.info(
        f"A DaskCluster has been created called {name} in {namespace} with the following config: {spec}"
    )
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)
        apps_api = kubernetes.client.AppsV1Api(api_client)
        # TODO Check for existing scheduler pod

        scheduler_spec = spec.get("scheduler", {})
        data = build_scheduler_pod_spec(name, scheduler_spec.get("spec"))
        kopf.adopt(data)
        await apps_api.create_namespaced_deployment(
            namespace=namespace,
            body=data,
        )
        # await wait_for_scheduler(name, namespace)
        logger.info(
            f"A scheduler pod has been created called {data['metadata']['name']} in {namespace}"
        )

        # TODO Check for existing scheduler service
        data = build_scheduler_service_spec(name, scheduler_spec.get("service"))
        kopf.adopt(data)
        await api.create_namespaced_service(
            namespace=namespace,
            body=data,
        )
        await wait_for_service(api, data["metadata"]["name"], namespace)
        logger.info(
            f"A scheduler service has been created called {data['metadata']['name']} in {namespace}"
        )

        worker_spec = spec.get("worker", {})
        data = build_worker_group_spec(name, worker_spec)
        # TODO: Next line is not needed if we can get worker groups adopted by the cluster
        kopf.adopt(data)
        api = kubernetes.client.CustomObjectsApi(api_client)
        await api.create_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskworkergroups",
            namespace=namespace,
            body=data,
        )
        logger.info(
            f"A worker group has been created called {data['metadata']['name']} in {namespace}"
        )


@kopf.on.create("daskworkergroup")
async def daskworkergroup_create(spec, name, namespace, logger, **kwargs):
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CustomObjectsApi(api_client)
        cluster = await api.get_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskclusters",
            namespace=namespace,
            name=spec["cluster"],
        )
        new_spec = dict(spec)
        kopf.adopt(new_spec, owner=cluster)
        api.api_client.set_default_header(
            "content-type", "application/merge-patch+json"
        )
        await api.patch_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskworkergroups",
            namespace=namespace,
            name=name,
            body=new_spec,
        )
        logger.info(f"Successfully adopted by {spec['cluster']}")

    await daskworkergroup_update(
        spec=spec, name=name, namespace=namespace, logger=logger, **kwargs
    )


@kopf.on.update("daskworkergroup")
async def daskworkergroup_update(spec, name, namespace, logger, **kwargs):
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)

        workers = await api.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"dask.org/workergroup-name={name}",
        )
        current_workers = len(workers.items)
        desired_workers = spec["worker"]["replicas"]
        workers_needed = desired_workers - current_workers

        if workers_needed > 0:
            for _ in range(workers_needed):
                data = build_worker_pod_spec(
                    worker_group_name=name,
                    namespace=namespace,
                    cluster_name=spec["cluster"],
                    uuid=uuid4().hex[:10],
                    spec=spec["worker"]["spec"],
                )
                kopf.adopt(data)
                await api.create_namespaced_pod(
                    namespace=namespace,
                    body=data,
                )
            logger.info(
                f"Scaled worker group {name} up to {spec['worker']['replicas']} workers."
            )
        if workers_needed < 0:
            async with rpc(
                f"tcp://{spec['cluster']}-service.{namespace}.svc.cluster.local:8786"
            ) as scheduler:
                logger.info(f"Retiring {-workers_needed} workers")

                killed_workers = await scheduler.retire_workers(n=-workers_needed)

            # TODO: Check that were deting workers in the right worker group
            for worker in killed_workers.values():
                await api.delete_namespaced_pod(
                    name=worker["name"],
                    namespace=namespace,
                )

            logger.info(
                f"Scaled worker group {name} down to {spec['worker']['replicas']} workers."
            )


@kopf.timer("daskworkergroup", interval=10.0)
async def daskworkergroup_manage(spec, name, namespace, logger, **kwargs):
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)

        logger.info("Checking number of active workers matches replica requirements.")

        workers = await api.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"dask.org/workergroup-name={name}",
        )

        current_workers = len(workers.items)
        desired_workers = spec["worker"]["replicas"]
        workers_needed = desired_workers - current_workers

        if current_workers < desired_workers:
            logger.info(
                f"Current number of workers active ({current_workers}) "
                f"does not match required replicas ({desired_workers}), restarting"
            )
            # One or more of our workers has falled over, so we should restart some
            for _ in range(workers_needed):
                data = build_worker_pod_spec(
                    worker_group_name=name,
                    namespace=namespace,
                    cluster_name=spec["cluster"],
                    uuid=uuid4().hex[:10],
                    spec=spec["worker"]["spec"],
                )
                kopf.adopt(data)
                await api.create_namespaced_pod(
                    namespace=namespace,
                    body=data,
                )
        else:
            logger.info("Cluster worker status is OK")


@kopf.timer("daskautoscaler", interval=10.0)
async def adapt(spec, name, namespace, logger, memo: kopf.Memo, **kwargs):
    logger.info(f"Starting autoscaling for cluster {spec['cluster']}")

    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CustomObjectsApi(api_client)

        async with rpc(
            f"tcp://{spec['cluster']}-service.{namespace}.svc.cluster.local:8786"
        ) as scheduler:
            workers = await scheduler.adaptive_target()

            logger.info(f"Scheduler requests {workers} workers")
            if workers > spec["maximum"]:
                desired_workers = spec["maximum"]
            elif workers < spec["minimum"]:
                desired_workers = spec["minimum"]
            else:
                desired_workers = workers

        # TODO: How to manage scaling when you have multiple worker groups?
        worker_group = await api.get_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskworkergroups",
            namespace=namespace,
            name=f"{spec['cluster']}-default-worker-group",
        )

        last_scale = memo.get("last_scale", None)
        current_workers = worker_group["spec"]["worker"]["replicas"]

        logger.info(f"Current workers running: {current_workers}")
        if desired_workers != current_workers and last_scale is None:
            # We haven't registered a scaling event, so we'll set it as now
            memo["last_scale"] = datetime.now()
        elif desired_workers > current_workers:
            # We are scaling up, so lets set the time to now
            memo["last_scale"] = datetime.now()
        elif desired_workers < current_workers and last_scale is not None:
            # We have scaled down once, and we want to now check the time
            delta = datetime.now() - last_scale
            if delta < timedelta(minutes=5):
                logger.info(
                    f"Autoscaler is requesting a scale down. Last scale down was {delta} ago. Skipping."
                )
                return
            else:
                # Its been more than 5 minutes, so we'll set the current time as a scale event
                memo["last_scale"] = datetime.now()

        new_spec = [
            {"op": "replace", "path": "/spec/worker/replicas", "value": desired_workers}
        ]

        logger.info(f"Submitting new spec: {new_spec}")
        logger.info(f"Patching the {spec['cluster']}-default-worker-group spec")
        api.api_client.set_default_header("content-type", "application/json-patch+json")
        response = await api.patch_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskworkergroups",
            namespace=namespace,
            name=f"{spec['cluster']}-default-worker-group",
            body=new_spec,
        )
        logger.info(f"Got response from API: {response}")
        logger.info("Completed autoscaling run, retry in 10s")
