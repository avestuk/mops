# MOPS

# Usage

```bash
go build

# Set the KUBECONFIG envvar to point to your kube config
export KUBECONFIG=~/.kube/config.yaml

# Run MOPS and specify the seldon-model.yaml as input Quite a lot of events are
# published so tee'ing the output to a file can help to see what took place.
./mops -file=./seldon-model.yaml | tee output

cat seldon-model.yaml | ./mops -file=- | tee output
```
