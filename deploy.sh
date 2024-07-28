kubectl delete elasticsearch elastic
kubectl apply -f es.yaml
PASSWORD=$(kubectl get secret elastic-es-elastic-user -o go-template='{{.data.elastic | base64decode}}') && echo $PASSWORD
# kubectl port-forward service/elastic-kb-http 5601
# kubectl port-forward service/elastic-es-http 9200