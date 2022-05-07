import asyncio

from distributed.core import rpc

import kopf
import kubernetes_asyncio as kubernetes

from uuid import uuid4

from dask_kubernetes.common.auth import ClusterAuth
from dask_kubernetes.common.networking import (
    get_scheduler_address,
)


def build_scheduler_pod_spec(name, spec):
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"{name}-scheduler",
            "labels": {
                "dask.org/cluster-name": name,
                "dask.org/component": "scheduler",
            },
        },
        "spec": spec,
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


def build_worker_pod_spec(name, scheduler_name, n, spec):
    worker_name = f"{name}-worker-{n}"
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": worker_name,
            "labels": {
                "dask.org/cluster-name": scheduler_name,
                "dask.org/workergroup-name": name,
                "dask.org/component": "worker",
            },
        },
        "spec": spec,
    }


def build_worker_group_spec(name, spec):
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskWorkerGroup",
        "metadata": {"name": f"{name}-worker-group"},
        "spec": {
            "worker": spec,
        },
    }


def build_cluster_spec(name, image, replicas, resources, env):
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskCluster",
        "metadata": {"name": f"{name}-cluster"},
        "spec": {
            "imagePullSecrets": None,
            "image": image,
            "imagePullPolicy": "IfNotPresent",
            "protocol": "tcp",
            "scheduler": {
                "resources": resources,
                "env": env,
                "serviceType": "ClusterIP",
            },
            "replicas": replicas,
            "resources": resources,
            "env": env,
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

        # TODO Check for existing scheduler pod
        scheduler_spec = spec.get("scheduler", {})
        data = build_scheduler_pod_spec(name, scheduler_spec.get("spec"))
        kopf.adopt(data)
        await api.create_namespaced_pod(
            namespace=namespace,
            body=data,
        )
        # await wait_for_scheduler(name, namespace)
        logger.info(
            f"A scheduler pod has been created called {data['metadata']['name']} in {namespace} \
            with the following config: {data['spec']}"
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
            f"A scheduler service has been created called {data['metadata']['name']} in {namespace} \
            with the following config: {data['spec']}"
        )

        worker_spec = spec.get("worker", {})
        data = build_worker_group_spec(f"{name}-default", worker_spec)
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
            f"A worker group has been created called {data['metadata']['name']} in {namespace} \
            with the following config: {data['spec']}"
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

        scheduler_name = cluster["items"][0]["metadata"]["name"]
        worker = spec.get("worker")
        num_workers = worker["replicas"]
        for i in range(num_workers):
            data = build_worker_pod_spec(
                name, scheduler_name, uuid4().hex, worker["spec"]
            )
            kopf.adopt(data)
            worker_pod = await api.create_namespaced_pod(
                namespace=namespace,
                body=data,
            )
        logger.info(f"{num_workers} Worker pods in created in {namespace}")


@kopf.on.update("daskworkergroup")
async def daskworkergroup_update(spec, name, namespace, logger, **kwargs):
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)
        workers = await api.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"dask.org/workergroup-name={name}",
        )
        current_workers = len(workers.items)
        desired_workers = spec["replicas"]
        workers_needed = desired_workers - current_workers
        if workers_needed > 0:
            for _ in range(workers_needed):
                data = build_worker_pod_spec(
                    name, scheduler_name, uuid4().hex, spec.get("worker")
                )
                kopf.adopt(data)
                await api.create_namespaced_pod(
                    namespace=namespace,
                    body=data,
                )
            logger.info(f"Scaled worker group {name} up to {spec['replicas']} workers.")
        if workers_needed < 0:
            service_name = f"{name.split('-')[0]}-cluster"
            address = await get_scheduler_address(service_name, namespace)
            async with rpc(address) as scheduler:
                worker_ids = await scheduler.workers_to_close(
                    n=-workers_needed, attribute="name"
                )
            # TODO: Check that were deting workers in the right worker group
            logger.info(f"Workers to close: {worker_ids}")
            for wid in worker_ids:
                await api.delete_namespaced_pod(
                    name=wid,
                    namespace=namespace,
                )
            logger.info(
                f"Scaled worker group {name} down to {spec['replicas']} workers."
            )
