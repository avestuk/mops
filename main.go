package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	v1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	serializerYaml "k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	FieldManager    string = "mops"
	mlSeldon        string = "machinelearning.seldon.io"
	mlSeldonVersion string = "v1"
	//TODO
	//mlSeldonResource   string = "SeldonDeployments"
	mlSeldonAPIVersion string = mlSeldon + "/" + mlSeldonVersion

	// Durations
	defaultTimeout time.Duration = 10 * time.Second
	pollTimeout    time.Duration = time.Minute
)

// TODO
//var mlResource = schema.GroupVersionResource{
//	Group:    mlSeldon,
//	Version:  mlSeldonVersion,
//	Resource: "SeldonDeployment",
//	//Resource: "seldondeployments" - I think this is actually correct
//}

// TODO - Validate Kind
var gk = schema.GroupKind{
	Group: "machinelearning.seldon.io",
	Kind:  "SeldonDeployment",
}

// * Create a standalone program in Go which takes in a Seldon Core Custom Resource and creates it over the Kubernetes API
// * Watch the created resource to wait for it to become available.
// * Scale the resource to 2 replicas.
// * When it is available delete the Custom Resource.
// * In parallel to the last 3 steps list the Kubernetes Events with descriptions emitted by the created custom resource until it is deleted.

func main() {
	// setup input argument parsing
	var file string

	flag.StringVar(&file, "file", "", "The file to read a SeldonDeployment from")
	flag.Parse()

	// Parse input
	switch file {
	case "-":
		file = "/dev/stdin"
	case "":
		exitWithError("no input file passed")
	default:
	}

	fileContents, err := ioutil.ReadFile(file)
	if err != nil {
		exitWithError(fmt.Sprintf("failed to read file: %s, got err: %s", file, err))
	}

	// Create objects from parsed input
	objects, err := decodeInput([]byte(fileContents))
	if err != nil {
		exitWithError(fmt.Sprintf("failed to decode input, got err: %s", err))
	}

	// Create a config
	config, err := buildConfig()
	if err != nil {
		exitWithError(fmt.Sprintf("failed to build client config, got err: %s", err))
	}

	// Create a typed and dynamic client
	client, err := typedClientInit(config)
	if err != nil {
		exitWithError(fmt.Sprintf("failed to build typed client, got err: %s", err))
	}

	dynamicClient, err := dynamicClientInit(config)
	if err != nil {
		exitWithError(fmt.Sprintf("failed to build dynamic client, got err: %s", err))
	}

	// Get GroupMappings for K8s API resources.
	gr, err := restmapper.GetAPIGroupResources(client.Discovery())
	if err != nil {
		exitWithError(fmt.Sprintf("failed to get API group resources, got err: %s", err))
	}

	mapper := restmapper.NewDiscoveryRESTMapper(gr)
	_, err = mapper.RESTMapping(gk)
	if err != nil {
		exitWithError(fmt.Sprintf("failed to get rest mapping, got err; %s", err))
	}

	// Create a serializer that can decode
	decodingSerializer := serializerYaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)

	for _, object := range objects {
		obj := &unstructured.Unstructured{}

		// Decode the object into a k8s runtime Object. This also
		// returns the GroupValueKind for the object. GVK identifies a
		// kind. A kind is the implementation of a K8s API resource.
		// For instance, a pod is a resource and it's v1/Pod
		// implementation is its kind.
		runtimeObj, gvk, err := decodeRawObjects(decodingSerializer, object.Raw, obj)
		if err != nil {
			exitWithError(fmt.Sprintf("failed to decode object, got err: %s", err))
		}

		// Find the resource mapping for the GVK extracted from the
		// object. A resource type is uniquely identified by a Group,
		// Version, Resource tuple where a kind is identified by a
		// Group, Version, Kind tuple. You can see these mappings using
		// kubectl api-resources.
		gvr, err := getResourceMapping(mapper, gvk)
		if err != nil {
			exitWithError(fmt.Sprintf("failed to get gvr, got err: %s", err))
		}

		// Establish a REST mapping for the GVR. For instance
		// for a Pod the endpoint we need is: GET /apis/v1/namespaces/{namespace}/pods/{name}
		// As some objects are not namespaced (e.g. PVs) a namespace may not be required.
		dr := getRESTMapping(dynamicClient, gvr.Scope.Name(), obj.GetNamespace(), gvr.Resource)

		// Marshall our runtime object into json. All json is
		// valid yaml but not all yaml is valid json. The
		// APIServer works on json.
		data, err := marshallRuntimeObj(runtimeObj)
		if err != nil {
			exitWithError(fmt.Sprintf("failed to marshal json to runtime obj, got err: %s", err))
		}

		// Attempt to ServerSideApply the provided object.
		k8sObj, err := applyObjects(dr, obj, data)
		if err != nil {
			exitWithError(fmt.Sprintf("failed to apply obj, got err: %s", err))
		}

		fmt.Printf("\n%s %s/%s updated\n", k8sObj.GetKind(), k8sObj.GetNamespace(), k8sObj.GetName())
	}

}

func buildConfig() (*rest.Config, error) {
	kubeconfigPath := os.Getenv("KUBECONFIG")
	if kubeconfigPath == "" {
		return nil, fmt.Errorf("KUBECONFIG was empty")
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read kubeconfig from file: %s", kubeconfigPath)
	}

	return config, nil
}

func typedClientInit(config *rest.Config) (*kubernetes.Clientset, error) {
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to build k8s client, got err: %w", err)
	}

	return client, nil
}

func dynamicClientInit(config *rest.Config) (dynamic.Interface, error) {
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return dynamicClient, nil
}

// buildK8sClients returns a typed K8s client and a dynamic k8s client.
func buildK8sClients() (*kubernetes.Clientset, dynamic.Interface, error) {
	config, err := buildConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build config, got err: %w", err)
	}

	client, err := typedClientInit(config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build k8s client, got err: %w", err)
	}

	dynamicClient, err := dynamicClientInit(config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build dynmaic client, got err: %w", err)
	}

	return client, dynamicClient, nil
}

func getRESTMapping(dynamicClient dynamic.Interface, name meta.RESTScopeName, namespace string, resource schema.GroupVersionResource) dynamic.ResourceInterface {
	var dr dynamic.ResourceInterface
	if name == meta.RESTScopeNameNamespace {
		dr = dynamicClient.Resource(resource).Namespace(namespace)
	} else {
		dr = dynamicClient.Resource(resource)
	}

	return dr
}

func buildWatcher(client kubernetes.Interface) (watch.Interface, error) {
	// Watch SeldonDeployment
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	watcher, err := client.EventsV1beta1().Events("default").Watch(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return watcher, nil
}

func decodeInput(fileContents []byte) ([]*runtime.RawExtension, error) {
	y := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(fileContents), 4096)
	objects := []*runtime.RawExtension{}

	for {
		obj := &runtime.RawExtension{}
		if err := y.Decode(obj); err != nil {
			// We expect an EOF error when decoding is done,
			// anything else should count as a function fail.
			if err.Error() != "EOF" {
				return nil, err
			}
			return objects, nil
		}
		objects = append(objects, obj)
	}
}

func decodeRawObjects(decoder runtime.Serializer, data []byte, into *unstructured.Unstructured) (runtime.Object, *schema.GroupVersionKind, error) {
	return decoder.Decode(data, nil, into)
}

func getResourceMapping(mapper meta.RESTMapper, gvk *schema.GroupVersionKind) (*meta.RESTMapping, error) {
	return mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
}

func marshallRuntimeObj(obj runtime.Object) ([]byte, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal json to runtime obj, got err: %w", err)
	}

	return data, nil
}

func applyObjects(dr dynamic.ResourceInterface, obj *unstructured.Unstructured, data []byte) (*unstructured.Unstructured, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	return dr.Patch(ctx, obj.GetName(), types.ApplyPatchType, data, metav1.PatchOptions{
		FieldManager: FieldManager,
	})
}

func exitWithError(message string) {
	fmt.Print(message)
	os.Exit(1)
}

func waitForDeploymentReady(client kubernetes.Interface, namespace string) (*v1.Deployment, error) {
	start := time.Now()
	deadline := time.Now().Add(pollTimeout)
	seldonDeployment := &v1.Deployment{}

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()
		var err error

		if seldonDeployment.Name != "" {
			seldonDeployment, err = client.AppsV1().Deployments(namespace).Get(ctx, seldonDeployment.Name, metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to get deployment %s in namespace: %s, got err: %w", seldonDeployment.Name, namespace, err)
			}

			// Find the seldon Deployment if it has not already been found
		} else {
			deployments, err := client.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to list deployments in namespace: %s, got err: %w", namespace, err)
			}

			for _, deployment := range deployments.Items {
				deployment := deployment
				for _, or := range deployment.OwnerReferences {
					if or.APIVersion == mlSeldonAPIVersion {
						seldonDeployment = &deployment
						break
					}
				}
			}

		}

		// The deployment may not have been created yet so keep polling.
		if seldonDeployment == nil {
			continue
		}

		if seldonDeployment.Status.ReadyReplicas == *seldonDeployment.Spec.Replicas {
			fmt.Printf("\n took: %s for deployment ready", time.Since(start))
			return seldonDeployment, nil
		}
	}

	return nil, fmt.Errorf("timed out waiting for deployment: %s/%s to have %d ready pods", seldonDeployment.Namespace, seldonDeployment.Name, *seldonDeployment.Spec.Replicas)
}
