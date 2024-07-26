kubectl delete elasticsearch elastic
kubectl apply -f es.yaml
PASSWORD=$(kubectl get secret elastic-es-elastic-user -o go-template='{{.data.elastic | base64decode}}') && echo $PASSWORD
