import subprocess
import os
import yaml
import json
import sys
from . import utils


class EasyCloudRun():

    def __init__(self, layers_path="", service_layers_path="layers", network_name="easy_cloudrun"):
        self._utils_handler = utils.Utils()
        self._project_name = self._get_project_name()
        self._layers_path = layers_path
        self._service_layers_path = service_layers_path
        self._network_name = network_name

    def run(self, service_name, dockerfile_dir, environ={}, port=8080, user_command="", test=False):

        self._validate_service(dockerfile_dir)

        self._deploy_layers(dockerfile_dir)

        self.build(service_name, dockerfile_dir)

        self._run_docker(service_name, environ, port, user_command, test)

    def run_cloud(self, service_name, environ={}, port=8080, user_command="", test=False):

        self._utils_handler.check_output(
            f"docker rmi {self._get_image_path(service_name)}")

        self._run_docker(service_name, environ, port, user_command, test)

    def build(self, service_name, dockerfile_dir):
        self._validate_service(dockerfile_dir)

        print(f"Building docker {service_name}")

        self._deploy_layers(dockerfile_dir)

        image_path = self._get_image_path(service_name)

        # service_path = self._get_service_path(service_name)
        command = [f"cd {dockerfile_dir}",
                   f"docker build --tag {image_path} ."]

        self._utils_handler.check_output(command)

        print(f"The {service_name} docker build is complete.")

    def build_push(self, service_name, dockerfile_dir):
        self.build(service_name, dockerfile_dir)
        self.push(service_name)

    def build_push_deploy(self, service_name, dockerfile_dir, environ={}, commands={}):
        self.build(service_name, dockerfile_dir)
        self.push(service_name)
        self.deploy(service_name, environ, commands)

    def deploy(self, service_name, environ={}, commands={}):

        image_path = self._get_image_path(service_name)

        default_commands = {
            "--platform": "managed",
            "--region": self._get_region(),
            "--no-allow-unauthenticated": "",
            "--concurrency": "1000",
        }
        
        commands.update({"--set-env-vars": environ})
        add_args = self._get_cloudrun_deploy_command({**default_commands, **commands})

        run_args = " ".join(add_args)

        self._utils_handler.check_output(
            f"gcloud run deploy {service_name} --image {image_path} " + run_args)

    def push(self, service_name):

        print(f"Pusing docker {service_name}")

        image_path = self._get_image_path(service_name)
        command = [f"docker push {image_path}"]
        self._utils_handler.check_output(command)

        print(f"The {service_name} docker push completed.")

    def rmi(self, service_name):

        print(f"Deleting image {service_name} in the cloud.")

        command = f"gcloud container images list-tags gcr.io/{self._project_name}/{service_name} --format=yaml"
        images = subprocess.check_output(command, shell=True)
        containers = yaml.full_load_all(images)

        [self._utils_handler.check_output(
            f"gcloud container images delete gcr.io/{self._project_name}/{service_name}@{container['digest']} --force-delete-tags -q") for container in containers]

        print(f"Deleted image {service_name} in the cloud.")


    def _get_region(self):
        return subprocess.check_output("gcloud config get-value compute/region", shell=True).decode().split("\n")[0].strip()

    def _get_project_name(self):
        return subprocess.check_output("gcloud config get-value project", shell=True).decode().split("\n")[0].strip()

    def _run_docker(self, service_name, environ={}, port=8080, user_command="", test=False):

        print(f"Start running {service_name} ...")

        self._create_network(self._network_name)

        self._kill_docker(service_name)

        changes = {}
        changes["PORT"] = str(port)

        if test:
            changes.update({"TEST": "true"})

        command = []

        command.extend(
            ["docker", "run", "--rm", f"--network {self._network_name}"])
        command.extend(["--name", service_name])
        command.extend(["-p", f"{str(port)}:{str(port)}"] if port else [])
        command.extend([f"-e {name}={value}" for name,
                        value in {**environ, **changes}.items()])
        command.extend([user_command])
        command.extend([self._get_image_path(service_name)])

        self._utils_handler.check_output(" ".join(command))

    def _create_network(self, network_name):
        try:
            self._utils_handler.check_output(
                f"docker network create --driver bridge {network_name}")
        except:
            pass

    def _get_image_path(self, service_name):
        return f'gcr.io/{self._project_name}/{service_name}'

    def _get_cloudrun_deploy_command(self, commands):
        result = []

        for key in commands:
            if key == "--set-env-vars":
                env_vars = commands[key]
                value = ",".join(
                    [f"{key}={env_vars[key]}" for key in env_vars])
                result.extend([key, value])
            else:
                value = commands[key]
                result.extend([key, value]) if value else result.append(key)

        return result

    def _kill_docker(self, service_name):
        command = f"docker stop {service_name} && docker rm {service_name}"
        self._utils_handler.check_output(command)

    def _deploy_layers(self, dockerfile_dir):
        if self._layers_path:
            self._validate_service(dockerfile_dir)
            from_layer_path = self._layers_path
            to_layer_path = f"{dockerfile_dir}/{self._service_layers_path}"

            self._utils_handler.copy_directory(from_layer_path, to_layer_path)

    def _validate_service(self, dockerfile_dir):
        if not os.path.isdir(dockerfile_dir):
            raise ValueError(f"{dockerfile_dir} is not exists.")
