if [command -v microk8s ]; then
  echo "microk8s already installed yay!"
elif [ command -v snap ]; then
  sudo snap install microk8s --classic
  MYUSER=$USER
  sudo usermod -a -G microk8s $MYUSER
  sudo chown -R $MYUSER ~/.kube
  newgrp microk8s
elif [ command -v homebrew ]; then
  brew install ubuntu/microk8s/microk8s
  microk8s install
  echo "Find your cert (see https://www.waitingforcode.com/apache-spark/setting-up-apache-spark-kubernetes-microk8s/read)"
  exit 1
fi
if [ command -v microk8s ]; then
  sudo microk8s status --wait-ready
  sudo microk8s enable dns 
  sudo microk8s enable dashboard
  sudo microk8s enable storage
  sudo microk8s enable registry
fi
kube_host=$(microk8s kubectl cluster-info  |grep control |grep http |sed -ne 's/.*\(http[^"]*\).*/\1/p' | cut -d \' -f 1)
if [ -n "${kube_host}" ]; then
  cd "${SPARK_HOME}"
  repo=localhost:32000/local-spark
  tag=magic
  image="${repo}/spark:${tag}"
  ./bin/docker-image-tool.sh -r "localhost:32000/local-spark" -t ${tag} build
  cert_path=/var/snap/microk8s/current/certs/ca.crt
  backup_config="config-$(date +%s)"
  cp ~/.kube/config ~/.kube/${backup_config}
  microk8s config > ~/.kube/config
  KUBECONFIG="$(pwd)/microk8sconfig"
  kubeconfig="$(pwd)/microk8sconfig"
  export KUBECONFIG
  export kubeconfig
  docker push "${image}"
fi
