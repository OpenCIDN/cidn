/*
Copyright 2025 The OpenCIDN Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package apiserver

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/registry/task/blob"
	"github.com/OpenCIDN/cidn/pkg/registry/task/chunk"
)

var (
	Scheme = runtime.NewScheme()
	Codecs = serializer.NewCodecFactory(Scheme)
)

// Adds the list of internal types to Scheme.
func addInternalTypes(scheme *runtime.Scheme) error {
	// SchemeGroupVersion is group version used to register these objects
	schemeGroupVersion := schema.GroupVersion{Group: v1alpha1.GroupName, Version: runtime.APIVersionInternal}

	scheme.AddKnownTypes(schemeGroupVersion,
		&v1alpha1.Blob{},
		&v1alpha1.BlobList{},
		&v1alpha1.Chunk{},
		&v1alpha1.ChunkList{},
	)

	v1alpha1.RegisterDefaults(Scheme)

	return nil
}

func init() {
	v1alpha1.Install(Scheme)
	addInternalTypes(Scheme)

	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	Scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
		&metav1.ListOptions{},
		&metav1.CreateOptions{},
		&metav1.UpdateOptions{},
		&metav1.PatchOptions{},
		&metav1.DeleteOptions{},
	)
}

type Config struct {
	GenericConfig *genericapiserver.RecommendedConfig
}

type CompletedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
}

func (cfg *Config) Complete() CompletedConfig {
	return CompletedConfig{cfg.GenericConfig.Complete()}
}

func (c CompletedConfig) New() (*genericapiserver.GenericAPIServer, error) {
	genericServer, err := c.GenericConfig.New("opencidn.daocloud.io", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, err
	}

	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(v1alpha1.GroupName, Scheme, metav1.ParameterCodec, Codecs)

	chunkStorage, err := chunk.NewREST(Scheme, c.GenericConfig.RESTOptionsGetter)
	if err != nil {
		return nil, err
	}

	blobStorage, err := blob.NewREST(Scheme, c.GenericConfig.RESTOptionsGetter)
	if err != nil {
		return nil, err
	}

	apiGroupInfo.VersionedResourcesStorageMap["v1alpha1"] = map[string]rest.Storage{
		"chunks": chunkStorage,
		"blobs":  blobStorage,
	}

	if err := genericServer.InstallAPIGroup(&apiGroupInfo); err != nil {
		return nil, err
	}

	return genericServer, nil
}
