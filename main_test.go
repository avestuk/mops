package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	eventsv1beta1 "k8s.io/api/events/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	serializerYaml "k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/restmapper"
)

// Validate Kind
var gk = schema.GroupKind{
	Group: "machinelearning.seldon.io",
	Kind:  "SeldonDeployment",
}

var seldonModel = `
apiVersion: machinelearning.seldon.io/v1
kind: SeldonDeployment
metadata:
  name: seldon-model
spec:
  name: test-deployment
  predictors:
  - componentSpecs:
    - spec:
        containers:
        - image: seldonio/mock_classifier:1.9.1
          name: classifier
    graph:
      name: classifier
      type: MODEL
      endpoint:
        type: REST
    name: example
    replicas: 1
`

func TestClientCreation(t *testing.T) {
	config, err := buildConfig()
	require.NoError(t, err, "failed to build config")

	// Build typed client
	client, err := typedClientInit(config)
	require.NoError(t, err, "failed to build client")

	// Build dynamic client
	dClient, err := dynamicClientInit(config)
	require.NoError(t, err, "failed to build dynamic client")

	// Get pods from kube-system to prove client works
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pods, err := client.CoreV1().Pods("kube-system").List(ctx, metav1.ListOptions{})
	require.NoError(t, err, "failed to get pods")
	require.Greater(t, len(pods.Items), 1, "expected kube-system to return more than 1 pod")

	// Get pods from kube-system to prove the dynamic client works
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dpods, err := dClient.Resource(schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "pods",
	}).Namespace("kube-system").List(ctx, metav1.ListOptions{})
	require.NoError(t, err, "failed to get pods")
	require.Greater(t, len(dpods.Items), 1, "expected kube-system to return more than 1 pod")
}

func TestGetSeldonDeployment(t *testing.T) {
	config, err := buildConfig()
	require.NoError(t, err, "failed to build config")

	// Build typed client
	client, err := typedClientInit(config)
	require.NoError(t, err, "failed to build client")

	// Build dynamic client
	dClient, err := dynamicClientInit(config)
	require.NoError(t, err, "failed to build dynamic client")

	// Get GroupMappings for K8s API resources.
	grs, err := restmapper.GetAPIGroupResources(client.Discovery())
	require.NoError(t, err, "failed to get API group resources")
	for _, gr := range grs {
		if gr.Group.Name == gk.Group {
			fmt.Printf("%#v\n", gr)
		}
	}

	// Build a mapper
	mapper := restmapper.NewDiscoveryRESTMapper(grs)
	gvr, err := mapper.RESTMapping(gk, mlSeldonVersion)
	require.NoError(t, err, "failed to get resource mapping")

	dr := getRESTMapping(dClient, gvr.Scope.Name(), "default", gvr.Resource)

	// Get SeldonDeployment
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	k8sObj, err := dr.Get(ctx, "seldon-model", metav1.GetOptions{})
	require.NoError(t, err, "failed to get seldondeployment")

	// Get predictors field.
	// Predictors is a []map[string]interface{} so we have to do a little magic to access the replicas field.
	predictors, found, err := unstructured.NestedSlice(k8sObj.UnstructuredContent(), "spec", "predictors")
	require.NoError(t, err, "failed to get field")
	require.True(t, found, "field predictors not found")
	t.Logf("predictors: %s", predictors)

	// Cast the first element of the predictors slice to map[string]interface{}
	replicas, found, err := unstructured.NestedInt64(predictors[0].(map[string]interface{}), "replicas")
	require.NoError(t, err, "failed to get field")
	require.True(t, found, "field predictors not found")
	t.Logf("replicas: %d", replicas)
}

func TestWatchSeldonDeployment(t *testing.T) {
	// Build typed client
	client, dclient, err := buildK8sClients()
	require.NoError(t, err, "failed to build client")

	// Watch SeldonDeployment
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	watcher, err := getEvents(ctx, client, "default")
	require.NoError(t, err, "failed to get seldondeployment")
	defer watcher.Stop()

	go func() {
		results := watcher.ResultChan()
		for ev := range results {
			switch ev.Type {
			case watch.Added, watch.Modified, watch.Deleted:
				event := ev.Object.(*eventsv1beta1.Event)
				if event.Regarding.APIVersion == mlSeldonAPIVersion {
					t.Logf("\nnote: %s\n", event.Note)
				}
			case watch.Error:
				t.Logf("watcher returned an error: %s", ev.Object)
			}
		}
		t.Logf("channel closed")
	}()

	// Create a Seldon Deployment
	obj, _ := createSeldonDeployment(t, seldonModel, client, dclient)

	// Wait for the deployment to be ready
	_, err = waitForDeploymentReady(client, obj.GetNamespace())
	require.NoError(t, err, "deployment failed to get ready")
}

func TestDecode(t *testing.T) {
	cases := []struct {
		Input string
	}{
		{
			seldonModel,
		},
	}

	for _, tt := range cases {
		objects, err := decodeInput([]byte(tt.Input))
		require.NoError(t, err, "failed to decode objects")

		decodingSerializer := serializerYaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
		obj := &unstructured.Unstructured{}
		runtimeObj, _, err := decodeRawObjects(decodingSerializer, objects[0].Raw, obj)
		require.NoError(t, err, "failed to decode raw objects")
		gvk := runtimeObj.GetObjectKind().GroupVersionKind()
		require.NotNil(t, gvk)
	}
}

func TestApply(t *testing.T) {
	cases := []struct {
		Name         string
		Input        string
		ApplySuccess bool
	}{
		{
			"seldon deployment",
			seldonModel,
			true,
		},
	}

	for _, tt := range cases {
		// Build required k8s clients
		client, dynamicClient, err := buildK8sClients()
		require.NoError(t, err, "failed to build client")

		// Return GroupMappings for K8s API resources.
		gr, err := restmapper.GetAPIGroupResources(client.Discovery())
		require.NoError(t, err, "failed to get API group resources")
		mapper := restmapper.NewDiscoveryRESTMapper(gr)

		// Build decoder
		objects, err := decodeInput([]byte(tt.Input))
		require.NoError(t, err, "failed to decode test input, %s", tt.Name)

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
			require.NoError(t, err, "failed to decode object")

			// Find the resource mapping for the GVK extracted from the
			// object. A resource type is uniquely identified by a Group,
			// Version, Resource tuple where a kind is identified by a
			// Group, Version, Kind tuple. You can see these mappings using
			// kubectl api-resources.
			gvr, err := getResourceMapping(mapper, gvk)
			require.NoError(t, err, "failed to get GroupVersionResource from GroupVersionKind")

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
			require.NoError(t, err, "failed to marshal json to runtime obj")

			// Attempt to ServerSideApply the provided object.
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				dr.Delete(ctx, obj.GetName(), *metav1.NewDeleteOptions(0))
			}()

			_, err = applyObjects(dr, obj, data)
			switch {
			case tt.ApplySuccess:
				require.NoError(t, err, "failed to patch object, test: %s", tt.Name)
			case !tt.ApplySuccess:
				require.Error(t, err, "expected failure to patch object but got none, test: %s", tt.Name)
			}

			// Wait for the deployment to be ready
			_, err = waitForDeploymentReady(client, obj.GetNamespace())
			require.NoError(t, err, "deployment failed to get ready")
		}
	}
}

func TestUpdate(t *testing.T) {
	cases := []struct {
		Name          string
		Input         string
		UpdateSuccess bool
	}{
		{
			"seldon deployment",
			seldonModel,
			true,
		},
	}

	for _, tt := range cases {
		// Build required k8s clients
		client, dynamicClient, err := buildK8sClients()
		require.NoError(t, err, "failed to build client")

		// Create a seldon deployment
		k8sObj, dr := createSeldonDeployment(t, tt.Input, client, dynamicClient)

		// Wait for the resource to be available
		// Easiest way to do this is to find the deployment that was created and watch that.
		_, err = waitForDeploymentReady(client, k8sObj.GetNamespace())
		require.NoError(t, err, "deployment failed to get ready")

		// Get replica count
		replicas, err := getReplicas(k8sObj)
		require.NoError(t, err, "failed to get replicas")
		replicas += 1

		k8sObj, err = updateReplicas(dr, k8sObj)
		require.NoError(t, err, "failed to update replica count")

		replicas, err = getReplicas(k8sObj)
		require.NoError(t, err, "failed to get replica from k8s object")
		require.Equal(t, int64(2), replicas)

		// Wait for the resource to be available
		// Easiest way to do this is to find the deployment that was created and watch that.
		deployment, err := waitForDeploymentReady(client, k8sObj.GetNamespace())
		require.NoError(t, err, "deployment failed to get ready")
		require.Equal(t, int32(2), *deployment.Spec.Replicas, "expected deployment to have two replicas set")
	}
}

func createSeldonDeployment(t *testing.T, inputManifest string, client kubernetes.Interface, dynamicClient dynamic.Interface) (*unstructured.Unstructured, dynamic.ResourceInterface) {
	// Return GroupMappings for K8s API resources.
	gr, err := restmapper.GetAPIGroupResources(client.Discovery())
	require.NoError(t, err, "failed to get API group resources")
	mapper := restmapper.NewDiscoveryRESTMapper(gr)

	// Build decoder
	objects, err := decodeInput([]byte(inputManifest))
	require.NoError(t, err, "failed to decode test input")
	object := objects[0]

	// Create a serializer that can decode
	decodingSerializer := serializerYaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)

	obj := &unstructured.Unstructured{}

	// Decode the object into a k8s runtime Object. This also
	// returns the GroupValueKind for the object. GVK identifies a
	// kind. A kind is the implementation of a K8s API resource.
	// For instance, a pod is a resource and it's v1/Pod
	// implementation is its kind.
	runtimeObj, gvk, err := decodeRawObjects(decodingSerializer, object.Raw, obj)
	require.NoError(t, err, "failed to decode object")

	// Find the resource mapping for the GVK extracted from the
	// object. A resource type is uniquely identified by a Group,
	// Version, Resource tuple where a kind is identified by a
	// Group, Version, Kind tuple. You can see these mappings using
	// kubectl api-resources.
	gvr, err := getResourceMapping(mapper, gvk)
	require.NoError(t, err, "failed to get GroupVersionResource from GroupVersionKind")

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
	require.NoError(t, err, "failed to marshal json to runtime obj")

	// Attempt to ServerSideApply the provided object.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		dr.Delete(ctx, obj.GetName(), *metav1.NewDeleteOptions(0))
	})

	k8sObj, err := applyObjects(dr, obj, data)
	require.NoError(t, err, "failed to apply objects")

	return k8sObj, dr
}
