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
	eventsv1beta1 "k8s.io/api/events/v1beta1"
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
	// K8s consts
	FieldManager       string = "mops"
	mlSeldon           string = "machinelearning.seldon.io"
	mlSeldonVersion    string = "v1"
	mlSeldonAPIVersion string = mlSeldon + "/" + mlSeldonVersion

	// Durations
	defaultTimeout time.Duration = 10 * time.Second
	pollTimeout    time.Duration = time.Minute

	// Output
	colourGreen string = "\033[32m"
	colourReset string = "\033[0m"
)

// Create a standalone program in Go which takes in a Seldon Core Custom Resource and creates it over the Kubernetes API
// Watch the created resource to wait for it to become available.
// Scale the resource to 2 replicas.
// When it is available delete the Custom Resource.
// In parallel to the last 3 steps list the Kubernetes Events with descriptions emitted by the created custom resource until it is deleted.
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

	// Create a typed and dynamic client
	client, dynamicClient, err := buildK8sClients()
	if err != nil {
		exitWithError(fmt.Sprintf("failed to build typed client, got err: %s", err))
	}

	// Get GroupMappings for K8s API resources.
	gr, err := restmapper.GetAPIGroupResources(client.Discovery())
	if err != nil {
		exitWithError(fmt.Sprintf("failed to get API group resources, got err: %s", err))
	}
	// Create mappings from the GroupResource to the REST API.
	mapper := restmapper.NewDiscoveryRESTMapper(gr)

	// Create a serializer that can decode our YAML input.
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
		if obj.GetNamespace() == "" {
			obj.SetNamespace("default")
		}
		dr := getRESTMapping(dynamicClient, gvr.Scope.Name(), obj.GetNamespace(), gvr.Resource)

		// Marshall our runtime object into json. All json is
		// valid yaml but not all yaml is valid json. The
		// APIServer works on json.
		data, err := marshallRuntimeObj(runtimeObj)
		if err != nil {
			exitWithError(fmt.Sprintf("failed to marshal json to runtime obj, got err: %s", err))
		}

		// Create an event watcher
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		watcher, err := getEvents(ctx, client, obj.GetNamespace())
		if err != nil {
			exitWithError(fmt.Sprintf("failed to create event watcher, got err: %s", err))
		}

		go func() {
			results := watcher.ResultChan()
			for ev := range results {
				switch ev.Type {
				case watch.Added, watch.Modified, watch.Deleted:
					event := ev.Object.(*eventsv1beta1.Event)
					if event.Regarding.APIVersion == mlSeldonAPIVersion {
						fmt.Printf("\n%s: %s\n", time.Now(), event.Note)
					}
				case watch.Error:
					fmt.Printf("event watcher watcher returned an error: %s", ev.Object)
				}
			}
		}()

		// Attempt to ServerSideApply the provided object. This results
		// in object creation if the resource does not already exist.
		k8sObj, err := applyObjects(dr, obj, data)
		if err != nil {
			exitWithError(fmt.Sprintf("failed to apply obj, got err: %s", err))
		}
		// Defer deleting the parsed object so we always ensure cleanup.
		defer func(obj *unstructured.Unstructured) {
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()
			fmt.Printf("\n%sMOPS: attempting cleanup%s", colourGreen, colourReset)
			err = dr.Delete(ctx, obj.GetName(), *metav1.NewDeleteOptions(0))
			if err != nil {
				exitWithError(fmt.Sprintf("failed to cleanup resources, got err: %s", err))
			}
			fmt.Printf("\n%sMOPS: resource %s %s/%s cleaned up%s\n", colourGreen, gvk.Group, obj.GetNamespace(), obj.GetName(), colourReset)
		}(obj)

		fmt.Printf("\n%sMOPS: %s %s %s/%s updated\n%s", colourGreen, time.Now(), gvk.Group, k8sObj.GetNamespace(), k8sObj.GetName(), colourReset)

		// Wait for the deployment to be ready
		_, err = waitForDeploymentReady(client, obj.GetNamespace())
		if err != nil {
			exitWithError(fmt.Sprintf("failed to wait for deployment ready, got err: %s", err))
		}

		// Update the replica count
		fmt.Printf("\n%sMOPS: updating replica count for %s %s/%s %s", colourGreen, gvk.Group, k8sObj.GetNamespace(), k8sObj.GetName(), colourReset)
		k8sObj, err = updateReplicas(dr, k8sObj)
		if err != nil {
			exitWithError(fmt.Sprintf("failed to update replica count, got err: %s", err))
		}

		fmt.Printf("\n%sMOPS: %s %s %s/%s updated\n%s", colourGreen, time.Now(), gvk.Group, k8sObj.GetNamespace(), k8sObj.GetName(), colourReset)

		// Wait for the deployment to be ready
		_, err = waitForDeploymentReady(client, obj.GetNamespace())
		if err != nil {
			exitWithError(fmt.Sprintf("failed to wait for deployment ready, got err: %s", err))
		}
	}
}

// buildConfig creates a kubernetes client config for use in buildilng
// kubernetes clients.
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

// typedClientInit creates a typed kubernetes client.
func typedClientInit(config *rest.Config) (*kubernetes.Clientset, error) {
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to build k8s client, got err: %w", err)
	}

	return client, nil
}

// dynamicClientInit creates a dynamic kubernetes client.
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

// getRESTMapping returns a dynamic client with the REST API mapping for a specific resource.
func getRESTMapping(dynamicClient dynamic.Interface, name meta.RESTScopeName, namespace string, resource schema.GroupVersionResource) dynamic.ResourceInterface {
	var dr dynamic.ResourceInterface
	if name == meta.RESTScopeNameNamespace {
		dr = dynamicClient.Resource(resource).Namespace(namespace)
	} else {
		dr = dynamicClient.Resource(resource)
	}

	return dr
}

// decodeInput accepts a []bytes containing YAML or JSON and decodes it to a
// common runtime object.
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

// decodeRawObjects returns a runtime object, and the GVK for that object.
func decodeRawObjects(decoder runtime.Serializer, data []byte, into *unstructured.Unstructured) (runtime.Object, *schema.GroupVersionKind, error) {
	return decoder.Decode(data, nil, into)
}

// getResourceMapping returns the resource that maps to a GroupKind.
func getResourceMapping(mapper meta.RESTMapper, gvk *schema.GroupVersionKind) (*meta.RESTMapping, error) {
	return mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
}

// marshallRuntimeObj marhsalls a runtime.Object in a JSON []bytes.
func marshallRuntimeObj(obj runtime.Object) ([]byte, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal json to runtime obj, got err: %w", err)
	}

	return data, nil
}

// applyObjects uses the apply patch type to create or update a resource.
func applyObjects(dr dynamic.ResourceInterface, obj *unstructured.Unstructured, data []byte) (*unstructured.Unstructured, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	return dr.Patch(ctx, obj.GetName(), types.ApplyPatchType, data, metav1.PatchOptions{
		FieldManager: FieldManager,
	})
}

// updateReplicas increases the replica count for a SeldonDeployment by one.
func updateReplicas(dr dynamic.ResourceInterface, k8sObj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	// Get replica count
	replicas, err := getReplicas(k8sObj)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	// Bump replica count
	replicas += 1

	// Create a patch
	patchInterface := []interface{}{
		map[string]interface{}{
			"op":    "replace",
			"path":  "/spec/predictors/0/replicas",
			"value": replicas,
		},
	}

	// Marshall the patch to json
	patchJson, err := json.Marshal(patchInterface)
	if err != nil {
		return nil, fmt.Errorf("failed to marshall patch to json, got err: %w", err)
	}

	// Apply the patch to bump the replica count.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	k8sObj, err = dr.Patch(ctx, k8sObj.GetName(), types.JSONPatchType, patchJson, metav1.PatchOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to patch %s %s/%s", k8sObj.GetAPIVersion(), k8sObj.GetNamespace(), k8sObj.GetName())
	}

	return k8sObj, nil
}

// getReplicas returns the number of replicas specified in a SeldonDeployment .spec.predictors[0].replicas field.
func getReplicas(k8sObj *unstructured.Unstructured) (int64, error) {
	// Get predictors field.
	// Predictors is a []map[string]interface{} so we have to do a little magic to access the replicas field.
	predictors, found, err := unstructured.NestedSlice(k8sObj.UnstructuredContent(), "spec", "predictors")
	if err != nil || !found {
		return 0, fmt.Errorf("spec.predictors field was not found for object: %s/%s, presence: %t, got err: %w", k8sObj.GetNamespace(), k8sObj.GetName(), found, err)
	}

	// Cast the first element of the predictors slice to map[string]interface{}
	replicas, found, err := unstructured.NestedInt64(predictors[0].(map[string]interface{}), "replicas")
	if err != nil || !found {
		return 0, fmt.Errorf("spec.predictors[0].replicas field was not found for object: %s/%s, presence: %t, got err: %w", k8sObj.GetNamespace(), k8sObj.GetName(), found, err)
	}

	return replicas, nil
}

// exitWithError prints a message and exits 1.
func exitWithError(message string) {
	fmt.Print(message)
	os.Exit(1)
}

// waitForDeploymentReady waits until a deployment has an equal number of desired and ready replicas.
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

		// Spec.Replicas can be nil causing a panic so just protect against it.
		if seldonDeployment.Spec.Replicas != nil && seldonDeployment.Status.ReadyReplicas == *seldonDeployment.Spec.Replicas {
			fmt.Printf("\n%sMOPS: took: %s for deployment ready%s", colourGreen, time.Since(start), colourReset)
			return seldonDeployment, nil
		}
	}

	return nil, fmt.Errorf("timed out waiting for deployment: %s/%s to have %d ready pods", seldonDeployment.Namespace, seldonDeployment.Name, *seldonDeployment.Spec.Replicas)
}

// getEvents returns function for logging events, a function to close the events channel and an error.
func getEvents(ctx context.Context, client kubernetes.Interface, namespace string) (watch.Interface, error) {
	// Watch SeldonDeployment
	watcher, err := client.EventsV1beta1().Events(namespace).Watch(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create event watcher")
	}

	return watcher, nil
}
