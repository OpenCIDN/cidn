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
	"context"
	"fmt"
	"net"
	"net/http"
	"slices"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/apiserver/user"
	"github.com/OpenCIDN/cidn/pkg/internal/utils"
	generatedopenapi "github.com/OpenCIDN/cidn/pkg/openapi"
	"github.com/OpenCIDN/cidn/pkg/registry/task/bearer"
	"github.com/OpenCIDN/cidn/pkg/registry/task/blob"
	"github.com/OpenCIDN/cidn/pkg/registry/task/chunk"
	"github.com/OpenCIDN/cidn/pkg/registry/task/multipart"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	genericapirequestuser "k8s.io/apiserver/pkg/authentication/user"
	authorizer "k8s.io/apiserver/pkg/authorization/authorizer"
	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilcompatibility "k8s.io/apiserver/pkg/util/compatibility"
	"k8s.io/klog/v2"
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
		&v1alpha1.Bearer{},
		&v1alpha1.BearerList{},
		&v1alpha1.Multipart{},
		&v1alpha1.MultipartList{},
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

	chunkStorage, chunkStatusStorage, err := chunk.NewREST(Scheme, c.GenericConfig.RESTOptionsGetter)
	if err != nil {
		return nil, err
	}

	blobStorage, blobStatusStorage, err := blob.NewREST(Scheme, c.GenericConfig.RESTOptionsGetter)
	if err != nil {
		return nil, err
	}

	bearerStorage, bearerStatusStorage, err := bearer.NewREST(Scheme, c.GenericConfig.RESTOptionsGetter)
	if err != nil {
		return nil, err
	}

	multipartStorage, err := multipart.NewREST(Scheme, c.GenericConfig.RESTOptionsGetter)
	if err != nil {
		return nil, err
	}

	apiGroupInfo.VersionedResourcesStorageMap["v1alpha1"] = map[string]rest.Storage{
		"chunks":         chunkStorage,
		"chunks/status":  chunkStatusStorage,
		"blobs":          blobStorage,
		"blobs/status":   blobStatusStorage,
		"bearers":        bearerStorage,
		"bearers/status": bearerStatusStorage,
		"multiparts":     multipartStorage,
	}

	if err := genericServer.InstallAPIGroup(&apiGroupInfo); err != nil {
		return nil, err
	}

	return genericServer, nil
}

func NewConfig(
	secureServing *genericoptions.SecureServingOptionsWithLoopback,
	etcd *genericoptions.EtcdOptions,
	users []utils.UserValue,
) (*Config, error) {
	err := secureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{net.ParseIP("127.0.0.1")})
	if err != nil {
		return nil, fmt.Errorf("error creating self-signed certificates: %v", err)
	}

	apiservercfg := genericapiserver.NewRecommendedConfig(Codecs)
	err = secureServing.ApplyTo(&apiservercfg.SecureServing, &apiservercfg.LoopbackClientConfig)
	if err != nil {
		return nil, err
	}

	// enable OpenAPI schemas
	namer := openapinamer.NewDefinitionNamer(Scheme)
	apiservercfg.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(generatedopenapi.GetOpenAPIDefinitions, namer)
	apiservercfg.OpenAPIConfig.Info.Title = "OpenCIDN"
	apiservercfg.OpenAPIConfig.Info.Version = "0.1"

	apiservercfg.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(generatedopenapi.GetOpenAPIDefinitions, namer)
	apiservercfg.OpenAPIV3Config.Info.Title = "OpenCIDN"
	apiservercfg.OpenAPIV3Config.Info.Version = "0.1"

	apiservercfg.EffectiveVersion = utilcompatibility.DefaultBuildEffectiveVersion()

	storageFactory := serverstorage.NewDefaultStorageFactory(
		etcd.StorageConfig,
		etcd.DefaultStorageMediaType,
		Codecs,
		serverstorage.NewDefaultResourceEncodingConfigForEffectiveVersion(Scheme, nil),
		apiservercfg.MergedResourceConfig,
		nil,
	)

	err = etcd.ApplyWithStorageFactoryTo(storageFactory, &apiservercfg.Config)
	if err != nil {
		return nil, err
	}

	if len(users) != 0 {
		var usersMap = map[[2]string]*utils.UserValue{}
		for _, u := range users {
			key := [2]string{u.Name, u.Password}
			if _, ok := usersMap[key]; ok {
				return nil, fmt.Errorf("duplicated user=%s", u.Name)
			}

			klog.Infof("User added: %s, Groups: %v", u.Name, u.Groups)

			usersMap[key] = &u
		}

		apiservercfg.Authentication.Authenticator = authenticator.RequestFunc(func(req *http.Request) (*authenticator.Response, bool, error) {
			user := utils.ParseBasicAuth(req.Header.Get("Authorization"))
			if user == nil {
				klog.Infof("Authorization failed: invalid or missing credentials for ip %s", req.RemoteAddr)
				return nil, false, nil
			}

			u := user.Username()
			p, _ := user.Password()
			ui, ok := usersMap[[2]string{u, p}]
			if !ok {
				klog.Infof("Authorization failed: invalid credentials for user %s from ip %s", u, req.RemoteAddr)
				return nil, false, nil
			}

			return &authenticator.Response{User: &genericapirequestuser.DefaultInfo{
				Name:   ui.Name,
				Groups: ui.Groups,
			}}, true, nil
		})

		apiservercfg.Authorization.Authorizer = authorizer.AuthorizerFunc(func(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
			groups := a.GetUser().GetGroups()
			if slices.Contains(groups, user.ControllerManagerGroup) {
				return user.ControllerManagerAuthorizer.Authorize(ctx, a)
			}
			if slices.Contains(groups, user.RunnerGroup) {
				return user.RunnerAuthorizer.Authorize(ctx, a)
			}
			if slices.Contains(groups, user.ViewerGroup) {
				return user.ViewerAuthorizer.Authorize(ctx, a)
			}
			klog.Infof("Authorization failed: user %s with groups %v is not authorized for action %s on resource %s", a.GetUser().GetName(), a.GetUser().GetGroups(), a.GetVerb(), a.GetResource())
			return authorizer.DecisionDeny, "", nil
		})
	} else {
		return nil, fmt.Errorf("no user")
	}

	return &Config{
		GenericConfig: apiservercfg,
	}, nil
}
