# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

# pylint: disable=too-many-arguments

import os
import time
import queue
import logging
import threading
from typing import Optional
from functools import cached_property, wraps

import kubernetes as k8s

from sdcm.remote import LOCALRUNNER
from sdcm.sct_config import sct_abs_path
from sdcm.utils.docker_utils import ContainerManager, DockerException, Container


KUBECTL_BIN = "kubectl"
HELM_IMAGE = "alpine/helm:3.3.4"

KUBECTL_TIMEOUT = 300  # seconds

K8S_CONFIGS = sct_abs_path("sdcm/k8s_configs")

JSON_PATCH_TYPE = "application/json-patch+json"

LOGGER = logging.getLogger(__name__)

logging.getLogger("kubernetes.client.rest").setLevel(logging.INFO)


class ApiCallRateLimiter(threading.Thread):
    """Simple and not very accurate rate limiter.

    Allow 1 call each `1 / rate_limit' seconds interval.
    If some call not able to start after `queue_size / rate_limit' seconds then raise `queue.Full' for caller.
    """

    def __init__(self, rate_limit: int, queue_size: int):
        super().__init__(name=type(self).__name__, daemon=True)
        self._lock = threading.Lock()
        self.rate_limit = rate_limit  # ops/s
        self.queue_size = queue_size
        self.running = threading.Event()

    def wait(self, message, *args, **kwargs):
        LOGGER.info(f"({type(self).__name__}) add to queue: {message}", *args, **kwargs)
        if not self._lock.acquire(timeout=self.queue_size / self.rate_limit):
            LOGGER.error("k8s API call rate limiter queue size limit has been reached")
            raise queue.Full
        LOGGER.info(f"({type(self).__name__}) allow to run: {message}", *args, **kwargs)

    def stop(self):
        self.running.clear()
        self.join()

    def run(self) -> None:
        LOGGER.info("k8s API call rate limiter started: rate_limit=%s, queue_size=%s",
                    self.rate_limit, self.queue_size)

        self.running.set()

        while self.running.is_set():
            if self._lock.locked():
                self._lock.release()
            time.sleep(1 / self.rate_limit)


class NoRateLimit:
    @staticmethod
    def wait() -> None:
        pass

    @staticmethod
    def stop() -> None:
        pass


class KubernetesOps:
    @staticmethod
    def create_k8s_configuration(kluster):
        k8s_configuration = k8s.client.Configuration()
        if kluster.k8s_server_url:
            k8s_configuration.host = kluster.k8s_server_url
        else:
            k8s.config.load_kube_config(client_configuration=k8s_configuration)
        return k8s_configuration

    @classmethod
    def api_client(cls, kluster):
        conf = getattr(kluster, "k8s_configuration", None) or cls.create_k8s_configuration(kluster)
        client = k8s.client.ApiClient(conf)
        orig_call_api = client._ApiClient__call_api

        @wraps(orig_call_api)
        def call_api(*args, **kwargs):
            kluster.api_call_rate_limiter.wait("k8s API call: args=%s, kwargs=%s", args, kwargs)
            return orig_call_api(*args, **kwargs)

        client._ApiClient__call_api = call_api
        LOGGER.debug("k8s ApiClient %s patched to be rate limited", client)

        return client

    @classmethod
    def dynamic_client(cls, kluster):
        return k8s.dynamic.DynamicClient(cls.api_client(kluster))

    @classmethod
    def dynamic_api(cls, kluster, api_version, kind):
        return cls.dynamic_client(kluster).resources.get(api_version=api_version, kind=kind)

    @classmethod
    def core_v1_api(cls, kluster):
        return getattr(kluster, "_k8s_core_v1_api", None) or k8s.client.CoreV1Api(cls.api_client(kluster))

    @classmethod
    def list_pods(cls, kluster, namespace=None, **kwargs):
        if namespace is None:
            return cls.core_v1_api(kluster).list_pod_for_all_namespaces(watch=False, **kwargs).items
        return cls.core_v1_api(kluster).list_namespaced_pod(namespace=namespace, watch=False, **kwargs).items

    @classmethod
    def list_services(cls, kluster, namespace=None, **kwargs):
        if namespace is None:
            return cls.core_v1_api(kluster).list_service_all_namespaces(watch=False, **kwargs).items
        return cls.core_v1_api(kluster).list_namespaced_service(namespace=namespace, watch=False, **kwargs).items

    @staticmethod
    def kubectl_cmd(kluster, *command, namespace=None, ignore_k8s_server_url=False):
        cmd = [KUBECTL_BIN, ]
        if not ignore_k8s_server_url and kluster.k8s_server_url is not None:
            cmd.append(f"--server={kluster.k8s_server_url}")
        if namespace:
            cmd.append(f"--namespace={namespace}")
        cmd.extend(command)
        return " ".join(cmd)

    @classmethod
    def kubectl(cls, kluster, *command, namespace=None, timeout=KUBECTL_TIMEOUT, remoter=None):
        cmd = cls.kubectl_cmd(kluster, *command, namespace=namespace, ignore_k8s_server_url=bool(remoter))
        if remoter is None:
            remoter = LOCALRUNNER

        kluster.api_call_rate_limiter.wait(cmd)

        return remoter.run(cmd, timeout=timeout)

    @classmethod
    def apply_file(cls, kluster, config_path, namespace=None, timeout=KUBECTL_TIMEOUT, envsubst=True):
        if envsubst:
            config_path = f"<(envsubst<{config_path})"
        cls.kubectl(kluster, "apply", "-f",  config_path, namespace=namespace, timeout=timeout)

    @classmethod
    def copy_file(cls, kluster, src, dst, container=None, timeout=KUBECTL_TIMEOUT):
        command = ["cp", src, dst]
        if container:
            command.extend(("-c", container))
        cls.kubectl(kluster, *command, timeout=timeout)

    @classmethod
    def expose_pod_ports(cls, kluster, pod_name, ports, labels=None, selector=None, namespace=None, timeout=KUBECTL_TIMEOUT):
        command = ["expose pod", pod_name, "--type=LoadBalancer",
                   "--port", ",".join(map(str, ports)),
                   f"--name={pod_name}-loadbalancer", ]
        if labels:
            command.extend(("--labels", labels))
        if selector:
            command.extend(("--selector", selector))
        cls.kubectl(kluster, *command, namespace=namespace, timeout=timeout)

    @classmethod
    def unexpose_pod_ports(cls, kluster, pod_name, namespace=None, timeout=KUBECTL_TIMEOUT):
        cls.kubectl(kluster, f"delete service {pod_name}-loadbalancer", namespace=namespace, timeout=timeout)


class HelmContainerMixin:
    def helm_container_run_args(self) -> dict:
        volumes = {os.path.expanduser("~/.kube"): {"bind": "/root/.kube", "mode": "rw"},
                   os.path.expanduser("~/.helm"): {"bind": "/root/.helm", "mode": "rw"},
                   K8S_CONFIGS: {"bind": "/apps", "mode": "rw"}, }
        return dict(image=HELM_IMAGE,
                    entrypoint="/bin/cat",
                    tty=True,
                    name=f"{self.name}-helm",
                    network_mode="host",
                    volumes=volumes)

    @cached_property
    def _helm_container(self) -> Container:
        return ContainerManager.run_container(self, "helm")

    def helm(self, kluster, *command: str, namespace: Optional[str] = None) -> str:
        cmd = ["helm", ]
        if kluster.k8s_server_url:
            cmd.extend(("--kube-apiserver", kluster.k8s_server_url, ))
        if namespace:
            cmd.extend(("--namespace", namespace, ))
        cmd.extend(command)
        cmd = " ".join(cmd)

        kluster.api_call_rate_limiter.wait(cmd)

        LOGGER.debug("Execute `%s'", cmd)
        res = self._helm_container.exec_run(["sh", "-c", cmd])
        if res.exit_code:
            raise DockerException(f"{self._helm_container}: {res.output.decode('utf-8')}")
        return res.output.decode("utf-8")
