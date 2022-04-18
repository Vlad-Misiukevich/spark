* Setup needed requirements into your env `pip install -r requirements.txt`
* Add your code in `src/main/`
* Test your code with `src/tests/`
* Package your artifacts
* Modify dockerfile if needed
* Build and push docker image
* Deploy infrastructure with terraform
```
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
az aks get-credentials \
    --resource-group rg-vmisiukevich-westeurope \
    --name aks-vmisiukevich-westeurope \
    --subscription 9e5b0b80-8805-4b33-8b84-410263caf100
kubectl proxy
kubectl create serviceaccount spark2 
kubectl create clusterrolebinding spark2-role \
    --clusterrole=edit \
    --serviceaccount=default:spark2 \
    --namespace=default
spark-submit \
    --deploy-mode cluster \
    --master k8s://http://127.0.0.1:8001 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark2 \
    --name spark2 \
    --conf spark.executor.instances=3 \
    --conf spark.kubernetes.container.image=vmisiukevich/spark_img:latest \
    C:/Spark/spark-3.1.3-bin-hadoop3.2/examples/src/main/python/pi.py
    ...
```