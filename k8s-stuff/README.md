## Local environment preparation

### Manual installation
Install [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux) and [minikube](https://minikube.sigs.k8s.io/docs/start) 
if you don't already have them.

Also install [Helm](https://helm.sh/docs/intro/install) (version 3, as it's incompatibile with version 2)
and [Golang](https://go.dev/doc/install).

I also advise adding alias to `kubectl` tool as `k`.

### Installation with script
I also provide script to install kubectl, minikube, helm and golang.
`cd` into this directory and run `install.sh` script. Pay attention, as the script is prepared at x86-64 architecture
and some urls may not work if you run it on another. Refer to urls in [manual installation](#manual-installation) section
for further information.

### Sample pod

Then you can run minikube with `minikube start`. If it doesn't work, act like IT and google it on stack overflow.

To deploy one sample pod, go into `k8s-stuff` directory and in terminal (while minikube is started), execute commands:
```bash
kubectl apply -f configmap.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
```

make sure to have `tryout` namespace. If you don't, here's command:
`kubectl create namespace tryout`

After that one pod should be running (or at least creating). You can check it with `kubectl get pods -n tryout` command.
If you want to check connectivity to the port, you can tap into cluster with another pod, e.g.:
`kubectl run -it ubuntu --image=ubuntu --restart=Never -n tryout -- /bin/bash`

You get pod with ubuntu, so you can install curl (don't forget about `apt update` and `apt upgrade` before trying to install curl).
Then you can curl to pod, using this sample url: `http://peer1-from-peerset1-service.tryout.svc.cluster.local:8080/_meta/metrics`.
If you don't know, how this url was created, here I come to your aid: it's actually http://<service name>.<namespace in which your service is>.svc.cluster.local:<service port>/<path> - k8s feature.

```
Note, that yaml files here are just sample for one pod.
```

### Prometheus and Grafana
Here we're going to use helm, out of pure simplicity and generosity of defaults (for now I don't think we'll need anything more). To install prometheus
release on the cluster, you have to have prometheus charts repository in your helm tool.
```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add stable https://charts.helm.sh/stable
helm repo update
```
then type:
```bash
helm install prometheus prometheus-community/prometheus -n prometheus --create-namespace
```
Important! If you already have prometheus release (you've already typed that command and it ended with success), then you need to use `upgrade` instead of install.
Also if you need to do this with sudo (I had to), you need to do `sudo` with all helm commands (including adding repos). At least I needed.

With Grafana it's almost the same:
```bash
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
helm install grafana grafana/grafana -n grafana --create-namespace
```
After installing, there should be instruction how to actually log into grafana. As for grafana configuration, maybe I'll do some bash script but I'm not quite
sure if that's necessary.

Connecting to Grafana requires using `port-forwarding`:
`kubectl port-forward <grafana pod name - can get with kubectl get pods -n grafana> 3000:3000 -n grafana`

If you want to create dashboards with metrics, you need to configure datasource.
![image1](screens/datasource-1.png)

Then you click big blue button "Add datasource" and pick "Prometheus type". The only thing you need to do is paste this as url:
`http://prometheus-server.prometheus.svc.cluster.local:80` (at least if you left everything as described here) and click `Save & Test` button.
Now you have prometheus datasource and can begin fun with grafana.

That's actually it - we have all kube_metrics metrics (those exported from k8s) and metrics from our apps (if only you didn't change prometheus annotations
in yamls). Feel free to use metrics explorer (icon of compass - third from up) to see what metrics with what labels are scraped (if you're familiar with 
PromQL you can change from `Builder` tab to `Code` tab in right up corner of metric view).

## What's next?
Feel free to familiarize with golang command to actually deploy multiple peers in multiple peersets.
