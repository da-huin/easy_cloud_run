import subprocess
import os
import yaml
import json
import sys
import utils

class EasyCloudRun():

    def __init__(self, services_path, project_name):
        self._utils_handler = utils.Utils()
        self._network_name = "public"
        self._services_path = services_path
        self._project_name = project_name

    def remove_cloud_image(self, service_name):
        print(f"Deleting image {service_name} in the cloud.")
        command = f"gcloud container images list-tags gcr.io/{self._project_name}/{service_name} --format=yaml"
        result = subprocess.check_output(command, shell=True)
        containers = yaml.full_load_all(result)

        for container in containers:
            command = f"gcloud container images delete gcr.io/{self._project_name}/{service_name}@{container['digest']} --force-delete-tags -q"
            print(command)
            result = subprocess.check_output(command, shell=True)
            print(result)

        print(f"Deleted image {service_name} in the cloud.")

    def test(self, service_name):
        service_path = self.get_service_path(service_name)
        command = [f"cd {service_path}/src", f" pytest -s test.py"]
        return self._utils_handler.check_output(command)

    def deploy(self, service_name):
        self.push_local_docker(service_name)
        image_path = self._utils_handler.get_image_path(service_name)
        add_args = self._get_deploy_command(service_name)

        run_args = " ".join(add_args)

        self._utils_handler.check_output(
            f"gcloud run deploy {service_name} --image {image_path} " + run_args)

    def run_local(self, service_name, port):

        changes = {
            "LOCAL_TEST": "true",
            "PORT": str(port),
        }

        self._utils_handler.check_output([f"cd {self.get_service_path(service_name)}/src"] +
                                self._get_environ_command("local", service_name, changes) + ["python3 app.py"])


    def build_local_docker(self, service_name):
        print(f"{service_name} 도커를 빌드하는 중입니다.")
        
        self._deploy_layers(service_name)
        
        image_path = self._utils_handler.get_image_path(service_name)
        service_path = self.get_service_path(service_name)
        command = [f"cd {service_path}", f"docker build --tag {image_path} ."]


        self._utils_handler.check_output(command)
        print(f"{service_name} 도커 빌드가 완료되었습니다.")

    def push_local_docker(self, service_name):
        print(f"{service_name} 도커를 푸쉬하는 중입니다.")
        image_path = self._utils_handler.get_image_path(service_name)
        command = [f"docker push {image_path}"]
        self._utils_handler.check_output(command)
        print(f"{service_name} 도커 푸쉬를 완료했습니다.")

    def run_docker(self, kind, service_name, test, port=8080, add_command=""):
        self._deploy_layers(service_name)
        if kind == "local":
            self._validate_service(service_name)
        try:
            self._utils_handler.check_output(
                f"docker network create --driver bridge {self._network_name}")
        except:
            pass

        print(f"[{kind}]{service_name} 도커를 실행하는 중입니다.")
        docker_image_path = self._utils_handler.get_image_path(service_name)
        self._kill_local_docker(service_name)

        if kind == "cloud":
            self._utils_handler.check_output(f"docker rmi {docker_image_path}")
        if kind == "local":
            self.build_local_docker(service_name)

        changes = {
            "LOCAL_TEST": "true",
            "PORT": str(port)
        }

        if test:
            changes["TEST"] = "true"

        add_command_array = [add_command]
        port_command = []
        if port:
            port_command = ["-p", f"{str(port)}:{str(port)}"]

        docker_test_env_list = self._get_environ_command("docker",
                                                            service_name, changes)

        command = " ".join(["docker", "run", "--rm", "--network public"] + ["--name", service_name] +
                           port_command + docker_test_env_list + add_command_array + 
                           [docker_image_path])

        self._utils_handler.check_output(command)



    ### GET PATH ###
    def get_service_path(self, service_name):
        return self._utils_handler.get_unique_service_path(self._utils_handler.info["cloud_run_services_path"], service_name)

    def get_create_path(self):
        return self._utils_handler.get_path("resources_cloud_run") + "/create/flask" 

    def get_layer_path(self, kind, service_name):

        if kind == "from":
            return self._utils_handler.get_path("resources_general_layers")
        elif kind == "to":
            return self.get_service_path(service_name) + "/src/layers"

    def get_packaged_service(self, service_name):
        if service_name not in self.packaged:
            return self.packaged["public"]
        else:
            return self.packaged[service_name]

    def _get_env(self, service_name):

        if service_name in self.packaged:
            env = self.get_packaged_service(service_name)["--set-env-vars"]
        else:
            env = self.packaged["public"]["--set-env-vars"]
        return env

    def _get_environ_command(self, kind, service_name, changes={}):

        result = []
        env = self._get_env(service_name)
        copyed_env = env.copy()

        copyed_env.update(changes)

        for env_name in copyed_env:
            set_env_command = self._utils_handler.get_set_environ_command()
            env_value = copyed_env[env_name]
            if kind == "local":
                result.append(f"{set_env_command} {env_name}={env_value}")
            elif kind == "docker":
                result.append(f"-e {env_name}={env_value}")

        return result


    def _get_deploy_command(self, service_name):
        result = []
        env = self._get_env(service_name)

        for key in self.get_packaged_service(service_name):
            if key == "--set-env-vars":
                if len(env) > 0:
                    value = ",".join([f"{key}={env[key]}" for key in env])
                    result.append(key)
                    result.append(value)
            else:
                value = self.get_packaged_service(service_name)[key]
                result.append(key)
                if value != "":
                    result.append(value)

        return result

    def _deploy_layers(self, service_name):
        print("Layer 를 배포하는 중입니다.")
        self._validate_service(service_name)
        from_layer_path = self.get_layer_path("from", service_name)
        to_layer_path = self.get_layer_path("to", service_name)

        self._utils_handler.copy_directory(from_layer_path, to_layer_path)
        print("Layer 를 배포했습니다.")

    def _kill_local_docker(self, service_name):
        print(f"{service_name} 도커가 실행되어 있으면 제거하는 중입니다.")
        command = f"docker stop {service_name} && docker rm {service_name}"
        self._utils_handler.check_output(command)

    ### TOOTLS ###
    def _set_var(self, file_path, changes):
        with open(file_path, "r", encoding="utf-8") as fp:
            readed = fp.read()

        for key in changes:
            value = changes[key]
            readed = readed.replace("{{"+key+"}}", value)

        with open(file_path, "w", encoding="utf-8") as fp:
            fp.write(readed)

    def _validate_service(self, service_name):
        try:
            service_path = self.get_service_path(service_name)
        except:
            raise ValueError(f"존재하지 않는 서비스 이름입니다. 서비스 이름은 {service_name} 입니다.")



    def work(self, job, args):
        if job == "run-docker":
            self.run_docker("local", args.service_name,
                            args.test, args.port, args.add_port, args.add_command)
        elif job == "test":
            self.test(args.service_name)
        elif job == "run-local":
            self._utils_handler.hook_start("cloud_run_flask", args.service_name)
            self.run_local(args.service_name, args.port)
        elif job == "run-cloud-docker":
            self.run_docker(
                "cloud", args.service_name, args.test, args.port, args.add_port)
        elif job == "docker-build-push":
            self.build_local_docker(args.service_name)
            self.push_local_docker(args.service_name)
        elif job == "deploy-direct":
            self.deploy_direct(args.service_name)
        elif job == "docker-build-deploy-direct":
            self.build_local_docker(args.service_name)
            self.deploy_direct(args.service_name)
        elif job == "git-push":
        elif job == "rmi":
            self.remove_cloud_image(args.service_name)
        else:
            raise ValueError(f"invalid job {job}")
