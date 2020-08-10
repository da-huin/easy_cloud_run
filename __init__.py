import subprocess
import os
import yaml
import json
import requests
import sys
import importlib
import paramiko
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
swagger = importlib.import_module("swagger")


class CloudRun():

    def __init__(self, utils):
        self.swagger_handler = swagger.Swagger(utils)
        self.utils = utils
        self.network_name = "public"
        self.deploy_setting = self.utils.settings_handler.get("deploy")[
            "service"]["cloud_run"]["deploy_setting"]
        self.packaged = self.pasre_template(self.deploy_setting)

    ### GET PATH ###
    def get_service_path(self, service_name):
        return self.utils.get_unique_service_path(self.utils.info["cloud_run_services_path"], service_name)

    def get_create_path(self, kind):
        return self.utils.get_path("resources_cloud_run") + "/create/" + kind

    def get_layer_path(self, kind, service_name):
        if kind == "from":
            return self.utils.get_path("resources_general_layers")
        elif kind == "to":
            return self.get_service_path(service_name) + "/src/layers"

    def get_nuxt_layers_path(self, kind, service_name=""):
        if kind == "from":
            return self.utils.get_path("resources_cloud_run") + "/nuxt_layers"
        elif kind == "to":
            return self.get_service_path(service_name) + "/src/nuxt/layers"

    def get_nuxt_path(self, service_name):
        return self.get_service_path(service_name) + "/src/nuxt"

    def get_service_nuxt_path(self, service_name):
        return self.get_service_path(service_name) + "/src/nuxt"

    def get_cloudbuild_path(self, service_name):
        return self.get_service_path(service_name) + "/cloudbuild.yaml"

    def get_packaged_service(self, service_name):
        if service_name not in self.packaged:
            return self.packaged["public"]
        else:
            return self.packaged[service_name]

    def get_env(self, service_name, raw=False):
        if raw:
            try:
                env = self.deploy_setting[service_name]["--set-env-vars"]
            except:
                print("cannot found env.")
                env = {}
        else:
            # env = self.get_env(service_name)
            if service_name in self.packaged:
                env = self.get_packaged_service(service_name)["--set-env-vars"]
            else:
                env = self.packaged["public"]["--set-env-vars"]
        return env

    def get_host(self, service_name):

        public_env = self.get_env("public")

        host = public_env.get(
            service_name.upper().replace("-", "_") + "_URI", "")
        host = host.replace("https://", "")

        return host

    def get_export_env_string_list(self, kind, service_name, changes={}, raw=False):

        result = []
        env = self.get_env(service_name, raw)
        copyed_env = env.copy()

        copyed_env.update(changes)

        for env_name in copyed_env:
            set_env_command = self.utils.get_set_environ_command()
            env_value = copyed_env[env_name]
            if kind == "local":
                result.append(f"{set_env_command} {env_name}={env_value}")
            elif kind == "docker":
                result.append(f"-e {env_name}={env_value}")

        return result


    def get_template_setting_list(self, service_name, raw=False):
        result = []
        env = self.get_env(service_name, raw)
        # if raw:
        #     try:
        #         env = self.deploy_setting[service_name]["--set-env-vars"]
        #     except:
        #         print("cannot found env.")
        #         env = {}
        # else:
        #     env = self.get_env(service_name)

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

    ### LOADER ###
    def pasre_template(self, raw):

        self.utils.logging(f"템플릿을 불러오는 중입니다.")
        parsed = {}
        public = raw.get("public", {})
        public_env = public.get("--set-env-vars", {})

        parsed["public"] = public
        parsed["public"]["--set-env-vars"] = public_env

        for service_name in raw:
            if service_name == "public":
                continue

            service_piece = raw[service_name]
            if service_piece == None:
                service_piece = {}
            if not isinstance(service_piece, dict):
                raise ValueError(
                    f"{service_name} service's spec is not a dict.")

            copyed_public = public.copy()
            copyed_public_env = public_env.copy()

            service_env = service_piece.get("--set-env-vars", {})

            copyed_public.update(service_piece.copy())
            copyed_public_env.update(service_env.copy())
            copyed_public["--set-env-vars"] = copyed_public_env
            parsed[service_name] = copyed_public

        self.utils.logging(f"템플릿을 불러왔습니다.")

        return parsed

    def make_cloudbuild(self, service_name, add_args, is_test, kind):
        self.utils.logging("CloudBuild 를 불러오는 중입니다.")
        image_path = self.utils.get_image_path(service_name)
        run_args = ['beta', 'run', 'deploy', service_name,
                    "--image", image_path] + add_args

        changes = {}
        if is_test:
            changes = {
                "TEST": "true"
            }

        docker_test_env_list = self.get_export_env_string_list("docker",
                                                               service_name, changes)

        test_command = ["run", "--rm"] + docker_test_env_list
        test_command.append(image_path)
        steps = []

        steps.append({
            "name": 'gcr.io/cloud-builders/docker',
                    "args": ['build', '-t', image_path, '.'],
        }),
        if is_test:
            steps.append({
                "name": 'gcr.io/cloud-builders/docker',
                "args": test_command
            })

        steps.append({
            "name": 'gcr.io/cloud-builders/docker',
            "args": ['push', image_path]
        })

        steps.append({
            "name": 'gcr.io/cloud-builders/gcloud',
            "args": run_args
        })

        cloudbuild = {
            "steps": steps,
            "images": [image_path]
        }

        self.utils.logging("CloudBuild 를 불러왔습니다.")

        return cloudbuild

    ### WORKER ###
    def create_service(self, kind, service_name, base_dir, backend_type=""):
        self.utils.logging(f"{service_name} 서비스를 생성하는 중입니다.")
        try:
            service_path = self.get_service_path(service_name)
        except:
            pass
        else:
            # if os.path.isdir(service_path):
            raise ValueError("이미 존재하는 서비스입니다.")

        service_path = self.utils.get_path(
            "service_cloud_run") + "/" + base_dir + "/" + service_name
        service_src_path = service_path + "/src"
        self.utils.mkdir(service_src_path)
        self.utils.copy_directory(self.get_create_path(kind), service_path)

        self.set_var(service_src_path + "/settings.json", {
            "service_name": service_name
        })

        if kind == "nuxt":
            self.change_nuxt_file(service_name)
            self.deploy_nuxt_layers(service_name)
            self.deploy_nuxt_env(service_name)
            self.install_node_modules(service_name)

        self.deploy_layers(service_name)

        self.utils.logging(f"{service_name} 서비스를 생성했습니다.")

    def deploy(self, kind, service_name, test):
        self.deploy_layers(service_name)
        self.deploy_cloudbuild(service_name, test, kind)

        if kind == "nuxt":
            self.deploy_nuxt_layers(service_name)
            self.deploy_nuxt_env(service_name)

        self.utils.git_push(self.get_service_path(service_name))
        self.deploy_swagger(service_name)

    def build_local_docker(self, service_name, raw=False):
        self.utils.logging(f"{service_name} 도커를 빌드하는 중입니다.")
        
        if not raw:
            self.deploy_layers(service_name)
            if self.is_nuxt_service(service_name):
                self.deploy_nuxt_env(service_name)
                self.deploy_nuxt_layers(service_name)
        
        image_path = self.utils.get_image_path(service_name)
        service_path = self.get_service_path(service_name)
        command = [f"cd {service_path}", f"docker build --tag {image_path} ."]


        self.utils.check_output(command)
        self.utils.logging(f"{service_name} 도커 빌드가 완료되었습니다.")

    def push_local_docker(self, service_name):
        self.utils.logging(f"{service_name} 도커를 푸쉬하는 중입니다.")
        image_path = self.utils.get_image_path(service_name)
        command = [f"docker push {image_path}"]
        self.utils.check_output(command)
        self.utils.logging(f"{service_name} 도커 푸쉬를 완료했습니다.")

    def run_docker(self, kind, service_name, test, port=8080, add_port=-1, raw=False, add_command=""):
        if not raw:
            self.deploy_layers(service_name)
            if kind == "local":
                self.valid_service(service_name)
            try:
                self.utils.check_output(
                    f"docker network create --driver bridge {self.network_name}")
            except:
                pass

        self.utils.logging(f"[{kind}]{service_name} 도커를 실행하는 중입니다.")
        docker_image_path = self.utils.get_image_path(service_name)
        self.kill_local_docker(service_name)

        if kind == "cloud":
            self.utils.check_output(f"docker rmi {docker_image_path}")
        if kind == "local":
            self.build_local_docker(service_name, raw)

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

        if add_port != -1:
            port_command += ["-p", f"{str(add_port)}:{str(add_port)}"]

        docker_test_env_list = self.get_export_env_string_list("docker",
                                                            service_name, changes, raw)

        command = " ".join(["docker", "run", "--rm", "--network public"] + ["--name", service_name] +
                           port_command + docker_test_env_list + add_command_array + 
                           [docker_image_path])

        self.utils.check_output(command)

    def run_local(self, service_name, port):

        changes = {
            "LOCAL_TEST": "true",
            "PORT": str(port),
        }

        self.utils.check_output([f"cd {self.get_service_path(service_name)}/src"] +
                                self.get_export_env_string_list("local", service_name, changes) + ["python3 app.py"])

    def run_local_nuxt(self, service_name, port, backend_port):

        changes = {
            "LOCAL_TEST": "true",
            "PORT": str(port),
        }

        self.deploy_nuxt_env(service_name, True, backend_port)
        self.deploy_nuxt_layers(service_name)
        self.utils.logging(f"{service_name} nuxt를 로컬로 실행하는 중입니다.")
        service_path = self.get_nuxt_path(service_name)
        self.utils.check_output([f"cd {service_path}"] + self.get_export_env_string_list(
            "local", service_name, changes)+["npm run dev"])

    def deploy_cloudbuild(self, service_name, is_test, kind):

        self.valid_service(service_name)
        cloudbuild = self.make_cloudbuild(
            service_name, self.get_template_setting_list(service_name), is_test, kind)
        self.utils.logging("CloudBuild 를 배포하는 중입니다.")

        cloudbuild_path = self.get_cloudbuild_path(service_name)
        with open(cloudbuild_path, "w", encoding="utf-8") as fp:
            fp.write(yaml.dump(cloudbuild, encoding="utf-8").decode("utf-8"))

        self.utils.logging("CloudBuild 를 배포했습니다.")

    def deploy_layers(self, service_name):
        self.utils.logging("Layer 를 배포하는 중입니다.")
        self.valid_service(service_name)
        from_layer_path = self.get_layer_path("from", service_name)
        to_layer_path = self.get_layer_path("to", service_name)

        self.utils.copy_directory(from_layer_path, to_layer_path)
        self.utils.logging("Layer 를 배포했습니다.")

    def deploy_nuxt_layers(self, service_name):
        self.utils.logging(f"{service_name} Nuxt 레이어를 배포하는 중입니다.")
        from_layer_path = self.get_nuxt_layers_path("from")
        to_layer_path = self.get_nuxt_layers_path("to", service_name)
        self.utils.copy_directory(from_layer_path, to_layer_path)

        self.utils.logging(f"{service_name} Nuxt 레이어 배포를 완료했습니다.")

    def deploy_direct(self, service_name, raw=False):
        self.push_local_docker(service_name)
        image_path = self.utils.get_image_path(service_name)
        add_args = self.get_template_setting_list(service_name, raw=raw)

        run_args = " ".join(add_args)

        self.utils.check_output(
            f"gcloud run deploy {service_name} --image {image_path} " + run_args)
        if not raw:
            self.deploy_swagger(service_name)

    def install_node_modules(self, service_name):
        service_path = self.get_service_nuxt_path(service_name)
        self.utils.check_output([f"cd {service_path}", "npm install"])

    def change_nuxt_file(self, service_name):

        service_path = self.get_service_nuxt_path(service_name)
        package_path = service_path + "/package.json"
        package_lock_path = service_path + "/package-lock.json"
        readme_path = service_path + "/README.md"

        changes = {
            "service_name": service_name
        }

        self.set_var(package_path, changes)
        self.set_var(package_lock_path, changes)
        self.set_var(readme_path, changes)

    def kill_all_local_docker(self):
        self.utils.logging(f"모든 도커를 제거 중입니다.")
        self.utils.check_output(
            "FOR /f \"tokens=*\" %i IN ('docker ps -a -q') DO docker stop %i")
        self.utils.check_output(
            "FOR /f \"tokens=*\" %i IN ('docker ps -a -q') DO docker rm %i")
        self.utils.logging(f"모든 도커를 제거했습니다.")

    def kill_local_docker(self, service_name):
        self.utils.logging(f"{service_name} 도커가 실행되어 있으면 제거하는 중입니다.")
        command = f"docker stop {service_name} && docker rm {service_name}"
        self.utils.check_output(command)

    def is_nuxt_service(self, service_name):
        nuxt_folder_path = self.get_service_path(service_name) + "/src/nuxt"
        return os.path.isdir(nuxt_folder_path)

    def deploy_nuxt_env(self, service_name, local=False, backend_port=8080):
        self.utils.logging("nuxt env 를 배포하는 중입니다.")
        env_path = self.get_service_path(service_name) + "/src/nuxt/.env"
        result = {}

        # with open(env_path, "r", encoding="utf-8") as fp:
        #     origin_env_list = fp.read().split("\n")
        #     for origin_env_str in origin_env_list:
        #         splited = origin_env_str.split("=")

        #         key = splited[0]
        #         value = "=".join(splited[1:])
        #         if key == "" or value == "":
        #             continue
        #         result[key] = value

        nuxt_environ = self.utils.settings_handler.get(
            "deploy")["service"]["cloud_run"]["nuxt"]["environ"]
        for key in nuxt_environ:
            value = nuxt_environ[key]
            result[key] = value

        print(result)

        if local:
            result["BACKEND_URI"] = f"http://localhost:{backend_port}"

        with open(env_path, "w", encoding="utf-8") as fp:
            msg = ""
            for key in result:
                value = result[key]
                msg += key + "=" + value + "\n"

            fp.write(msg)

        self.utils.logging("nuxt env 를 배포했습니다.")

    def ssh_pull(self, ssh_name, service_name, port, add_port=-1, raw=False):

        self.utils.logging(
            f"{service_name} SSH 를 접속해 풀 받는 중입니다. 포트는 {port}, {add_port} 입니다.")
        docker_image_path = self.utils.get_image_path(service_name)
        ssh_info = self.utils.info["ssh"][ssh_name]
        cloud_run_ssh_pem_path = ssh_info["pem_path"]
        cloud_run_ssh_host = ssh_info["host"]
        cloud_run_ssh_username = ssh_info["username"]
        cloud_run_ssh_password = ssh_info["password"]
        cli = paramiko.SSHClient()
        cli.set_missing_host_key_policy(paramiko.AutoAddPolicy)

        args = {
            "port": 22,
            "username": cloud_run_ssh_username
        }

        if cloud_run_ssh_pem_path != "":
            k = paramiko.RSAKey.from_private_key_file(cloud_run_ssh_pem_path)
            args["pkey"] = k
        if cloud_run_ssh_password != "":
            args["password"] = cloud_run_ssh_password

        cli.connect(cloud_run_ssh_host, **args)

        changes = {
            "PORT": str(port)
        }

        env_str = " ".join(self.get_export_env_string_list("docker",
                                                        service_name, changes, raw))

        pull_command = f"sudo docker pull {docker_image_path}"
        kill_command = f"sudo docker stop {service_name}"
        port_str = f"-p {port}:{port}"
        if add_port != -1:
            port_str += f" -p {add_port}:{add_port} "

        run_command = f"sudo docker run -d --rm --name {service_name} {port_str} {env_str} {docker_image_path}"

        print(pull_command)
        _, stdout, stderr = cli.exec_command(pull_command, timeout=300)
        print("[STDOUT] ", stdout.readlines())
        print("[ERROR]", stderr.readlines())

        print(kill_command)
        _, stdout, stderr = cli.exec_command(kill_command, timeout=300)
        print("[STDOUT] ", stdout.readlines())
        print("[ERROR]", stderr.readlines())        

        print(run_command)
        _, stdout, stderr = cli.exec_command(run_command, timeout=300)
        print("[STDOUT] ", stdout.readlines())
        print("[ERROR]", stderr.readlines())        

        self.utils.logging(f"{service_name} SSH 를 접속해 풀 받았습니다.")

    ### TOOTLS ###
    def set_var(self, file_path, changes):
        with open(file_path, "r", encoding="utf-8") as fp:
            readed = fp.read()

        for key in changes:
            value = changes[key]
            readed = readed.replace("{{"+key+"}}", value)

        with open(file_path, "w", encoding="utf-8") as fp:
            fp.write(readed)

    def valid_service(self, service_name):
        try:
            service_path = self.get_service_path(service_name)
        except:
            raise ValueError(f"존재하지 않는 서비스 이름입니다. 서비스 이름은 {service_name} 입니다.")

    def remove_cloud_image(self, service_name):
        self.utils.logging(f"클라우드에 있는 {service_name} 이미지를 삭제하는 중입니다.")
        project_name = self.utils.info["gcp_project_name"]
        command = f"gcloud container images list-tags gcr.io/{project_name}/{service_name} --format=yaml"
        result = subprocess.check_output(command, shell=True)
        containers = yaml.full_load_all(result)

        for container in containers:

            command = f"gcloud container images delete gcr.io/{project_name}/{service_name}@{container['digest']} --force-delete-tags -q"
            print(command)
            result = subprocess.check_output(command, shell=True)
            print(result)

        self.utils.logging(f"클라우드에 있는 {service_name} 이미지를 삭제했습니다.")

    def remove_cloud_image_all(self, include):

        services = self.utils.get_all_service_names(
            "base_dockerfile", include) + self.utils.get_all_service_names("cloud-run", include)
        for service_name in services:
            try:
                self.remove_cloud_image(service_name)
            except Exception as e:
                self.utils.logging(f"{service_name} 이미지 삭제를 실패했지만 패스했습니다. {e}")

    def deploy_swagger(self, service_name):
        service_path = self.get_service_path(service_name)
        self.swagger_handler.deploy_swagger(
            "cloud_run", self.get_host(service_name), service_path, service_name)

    def deploy_swagger_all(self, include):

        services = self.utils.get_all_service_names("cloud-run", include)
        for service_name in services:
            self.deploy_swagger(service_name)
    def test(self, service_name):
        service_path = self.get_service_path(service_name)
        command = [f"cd {service_path}/src", f" pytest -s test.py"]
        return self.utils.check_output(command)


    def work(self, job, args):
        if job == "create-flask":
            self.create_service("flask", args.service_name, args.base_dir)
        elif job == "create-nuxt":
            self.create_service("nuxt", args.service_name, args.base_dir, args.backend_type)
        elif job == "deploy-layers":
            self.deploy_layers(args.service_name)
        elif job == "deploy-nuxt-layers":
            self.deploy_nuxt_layers(args.service_name)
        elif job == "deploy-nuxt-local-env":
            self.deploy_nuxt_env(args.service_name, True, args.port)
        elif job == "install-node-modules":
            self.install_node_modules(args.service_name)
        elif job == "deploy-cloudbuild":
            self.deploy_cloudbuild(
                args.service_name, args.test, args.cloud_run_kind)
        elif job == "deploy-flask":
            self.deploy("flask", args.service_name, args.test)
        elif job == "run-docker":
            self.run_docker("local", args.service_name,
                            args.test, args.port, args.add_port, args.raw, args.add_command)
        elif job == "test":
            self.test(args.service_name)
            # self.run_docker("local", args.service_name,
            #                 args.test, args.port, args.add_port, args.raw, args.add_command)                            
        elif job == "raw-run-docker":
            self.run_docker("local", args.service_name,
                            args.test, args.port, args.add_port, True, args.add_command)                            
        elif job == "run-local":
            self.utils.hook_start("cloud_run_flask", args.service_name)
            self.run_local(args.service_name, args.port)
        elif job == "run-local-nuxt":
            self.utils.hook_start("cloud_run_nuxt", args.service_name)
            self.run_local_nuxt(args.service_name,
                                args.port, args.backend_port)
        elif job == "run-cloud-docker":
            self.run_docker(
                "cloud", args.service_name, args.test, args.port, args.add_port)
        elif job == "docker-build-push":
            self.build_local_docker(args.service_name)
            self.push_local_docker(args.service_name)
        elif job == "docker-build-push-ssh-pull":
            self.utils.git_push(self.get_service_path(args.service_name))
            self.build_local_docker(args.service_name)
            self.push_local_docker(args.service_name)
            self.ssh_pull(args.ssh_name, args.service_name,
                          args.port, args.add_port)

        elif job == "raw-docker-build-push-ssh-pull":
            self.utils.git_push(self.get_service_path(args.service_name))
            self.build_local_docker(args.service_name, raw=True)
            self.push_local_docker(args.service_name)
            self.ssh_pull(args.ssh_name, args.service_name,
                          args.port, args.add_port, raw=True)

        elif job == "raw-docker-build-deploy-direct":
            self.build_local_docker(args.service_name, raw=True)
            self.deploy_direct(args.service_name, raw=True)
            
        elif job == "ssh-pull":
            self.utils.git_push(self.get_service_path(args.service_name))
            self.ssh_pull(args.ssh_name, args.service_name,
                          args.port, args.add_port, raw=args.raw)
        elif job == "deploy-direct":
            self.deploy_direct(args.service_name, raw=args.raw)
        elif job == "docker-build-deploy-direct":
            self.build_local_docker(args.service_name)
            self.deploy_direct(args.service_name, raw=args.raw)
        elif job == "git-push":
            self.utils.git_push(self.get_service_path(args.service_name))
        elif job == "rmi":
            self.remove_cloud_image(args.service_name)
        elif job == "rmi-all":
            self.utils.git_push(self.utils.get_path("service_cloud_run"))
            self.remove_cloud_image_all(args.include)
        elif job == "deploy-swagger":
            self.deploy_swagger(args.service_name)
        elif job == "deploy-swagger-all":
            self.deploy_swagger_all(args.include)

        else:
            raise ValueError(f"invalid job {job}")
