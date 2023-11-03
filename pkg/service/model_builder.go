package service

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/annotations"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/aws/services"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/config"
	elbv2deploy "sigs.k8s.io/aws-load-balancer-controller/pkg/deploy/elbv2"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/deploy/tracking"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/k8s"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/model/core"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/model/core/graph"
	elbv2model "sigs.k8s.io/aws-load-balancer-controller/pkg/model/elbv2"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/networking"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	LoadBalancerTypeNLBIP          = "nlb-ip"
	LoadBalancerTypeExternal       = "external"
	LoadBalancerTargetTypeIP       = "ip"
	LoadBalancerTargetTypeInstance = "instance"
	lbAttrsDeletionProtection      = "deletion_protection.enabled"

	LoadBalancerAllocatingPortKey = "service.beta.kubernetes.io/aws-load-balancer-allocating-port"
	LoadBalancerStackKey          = "service.beta.kubernetes.io/aws-load-balancer-stack-name"
)

// ModelBuilder builds the model stack for the service resource.
type ModelBuilder interface {
	// Build model stack for service
	Build(ctx context.Context, service *corev1.Service) (core.Stack, *elbv2model.LoadBalancer, bool, error)
}

// NewDefaultModelBuilder construct a new defaultModelBuilder
func NewDefaultModelBuilder(annotationParser annotations.Parser, subnetsResolver networking.SubnetsResolver,
	vpcInfoProvider networking.VPCInfoProvider, vpcID string, trackingProvider tracking.Provider,
	elbv2TaggingManager elbv2deploy.TaggingManager, ec2Client services.EC2, featureGates config.FeatureGates, clusterName string, defaultTags map[string]string,
	externalManagedTags []string, defaultSSLPolicy string, defaultTargetType string, enableIPTargetType bool, serviceUtils ServiceUtils,
	backendSGProvider networking.BackendSGProvider, sgResolver networking.SecurityGroupResolver, enableBackendSG bool,
	disableRestrictedSGRules bool, k8sClient client.Client) *defaultModelBuilder {
	return &defaultModelBuilder{
		annotationParser:         annotationParser,
		subnetsResolver:          subnetsResolver,
		vpcInfoProvider:          vpcInfoProvider,
		trackingProvider:         trackingProvider,
		elbv2TaggingManager:      elbv2TaggingManager,
		featureGates:             featureGates,
		serviceUtils:             serviceUtils,
		clusterName:              clusterName,
		vpcID:                    vpcID,
		defaultTags:              defaultTags,
		externalManagedTags:      sets.NewString(externalManagedTags...),
		defaultSSLPolicy:         defaultSSLPolicy,
		defaultTargetType:        elbv2model.TargetType(defaultTargetType),
		enableIPTargetType:       enableIPTargetType,
		backendSGProvider:        backendSGProvider,
		sgResolver:               sgResolver,
		ec2Client:                ec2Client,
		enableBackendSG:          enableBackendSG,
		disableRestrictedSGRules: disableRestrictedSGRules,
		stackGlobalCache:         map[core.StackID]core.Stack{},
		client:                   k8sClient,
	}
}

var _ ModelBuilder = &defaultModelBuilder{}

type defaultModelBuilder struct {
	annotationParser         annotations.Parser
	subnetsResolver          networking.SubnetsResolver
	vpcInfoProvider          networking.VPCInfoProvider
	backendSGProvider        networking.BackendSGProvider
	sgResolver               networking.SecurityGroupResolver
	trackingProvider         tracking.Provider
	elbv2TaggingManager      elbv2deploy.TaggingManager
	featureGates             config.FeatureGates
	serviceUtils             ServiceUtils
	ec2Client                services.EC2
	enableBackendSG          bool
	disableRestrictedSGRules bool

	stackGlobalCache map[core.StackID]core.Stack

	clusterName         string
	vpcID               string
	defaultTags         map[string]string
	externalManagedTags sets.String
	defaultSSLPolicy    string
	defaultTargetType   elbv2model.TargetType
	enableIPTargetType  bool

	client client.Client

	initialized bool
	lock        sync.RWMutex
}

func (b *defaultModelBuilder) Build(ctx context.Context, service *corev1.Service) (core.Stack, *elbv2model.LoadBalancer, bool, error) {
	// Initialize the global cache if not initialized
	if !b.initialized {
		// if not initialized, we need to build the global cache based on existing services
		var serviceList corev1.ServiceList
		if b.client != nil {
			if err := b.client.List(ctx, &serviceList); err != nil {
				return nil, nil, false, err
			}
			for _, svc := range serviceList.Items {
				if svc.Annotations[LoadBalancerStackKey] != "" && svc.DeletionTimestamp.IsZero() {
					stackID := core.StackID(types.NamespacedName{
						Namespace: "stack",
						Name:      svc.Annotations[LoadBalancerStackKey],
					})
					b.lock.Lock()
					if b.stackGlobalCache[stackID] == nil {
						b.stackGlobalCache[stackID] = core.NewDefaultStack(stackID)
					}
					b.stackGlobalCache[stackID].AddService(&svc)
					b.lock.Unlock()
				}
			}
		}
		b.initialized = true
	}

	// For each stack ID, if we found the stack annotation, this means the service will be sharing the same stack with other services
	// If so, we should reuse the same stack in the cache so that we can reuse the load balancer with shared listeners
	stackID := core.StackID(k8s.NamespacedName(service))
	var stack core.Stack
	stack = core.NewDefaultStack(stackID)
	if service.Annotations[LoadBalancerAllocatingPortKey] == "true" {
		// service will be allocated to a stack with shared loadbalancer
		if service.Annotations[LoadBalancerStackKey] == "" {
			return nil, nil, false, errors.Errorf("service %v/%v is waiting to allocated for a stack", service.Namespace, service.Name)
		}
		stackID = core.StackID(types.NamespacedName{
			Namespace: "stack",
			Name:      service.Annotations[LoadBalancerStackKey],
		})
		b.lock.Lock()
		if b.stackGlobalCache[stackID] == nil {
			s := core.NewDefaultStack(stackID)
			b.stackGlobalCache[stackID] = s
		}
		stack = b.stackGlobalCache[stackID]
		b.lock.Unlock()
	}
	task := &defaultModelBuildTask{
		clusterName:              b.clusterName,
		vpcID:                    b.vpcID,
		annotationParser:         b.annotationParser,
		subnetsResolver:          b.subnetsResolver,
		backendSGProvider:        b.backendSGProvider,
		sgResolver:               b.sgResolver,
		vpcInfoProvider:          b.vpcInfoProvider,
		trackingProvider:         b.trackingProvider,
		elbv2TaggingManager:      b.elbv2TaggingManager,
		featureGates:             b.featureGates,
		serviceUtils:             b.serviceUtils,
		enableIPTargetType:       b.enableIPTargetType,
		ec2Client:                b.ec2Client,
		enableBackendSG:          b.enableBackendSG,
		disableRestrictedSGRules: b.disableRestrictedSGRules,

		service:   service,
		stack:     stack,
		tgByResID: make(map[string]*elbv2model.TargetGroup),

		defaultTags:                          b.defaultTags,
		externalManagedTags:                  b.externalManagedTags,
		defaultSSLPolicy:                     b.defaultSSLPolicy,
		defaultAccessLogS3Enabled:            false,
		defaultAccessLogsS3Bucket:            "",
		defaultAccessLogsS3Prefix:            "",
		defaultIPAddressType:                 elbv2model.IPAddressTypeIPV4,
		defaultLoadBalancingCrossZoneEnabled: false,
		defaultProxyProtocolV2Enabled:        false,
		defaultTargetType:                    b.defaultTargetType,
		defaultHealthCheckProtocol:           elbv2model.ProtocolTCP,
		defaultHealthCheckPort:               healthCheckPortTrafficPort,
		defaultHealthCheckPath:               "/",
		defaultHealthCheckInterval:           10,
		defaultHealthCheckTimeout:            10,
		defaultHealthCheckHealthyThreshold:   3,
		defaultHealthCheckUnhealthyThreshold: 3,
		defaultHealthCheckMatcherHTTPCode:    "200-399",
		defaultIPv4SourceRanges:              []string{"0.0.0.0/0"},
		defaultIPv6SourceRanges:              []string{"::/0"},

		defaultHealthCheckPortForInstanceModeLocal:               strconv.Itoa(int(service.Spec.HealthCheckNodePort)),
		defaultHealthCheckProtocolForInstanceModeLocal:           elbv2model.ProtocolHTTP,
		defaultHealthCheckPathForInstanceModeLocal:               "/healthz",
		defaultHealthCheckIntervalForInstanceModeLocal:           10,
		defaultHealthCheckTimeoutForInstanceModeLocal:            6,
		defaultHealthCheckHealthyThresholdForInstanceModeLocal:   2,
		defaultHealthCheckUnhealthyThresholdForInstanceModeLocal: 2,
	}

	if err := task.run(ctx); err != nil {
		return nil, nil, false, err
	}
	return task.stack, task.loadBalancer, task.backendSGAllocated, nil
}

type defaultModelBuildTask struct {
	clusterName         string
	vpcID               string
	annotationParser    annotations.Parser
	subnetsResolver     networking.SubnetsResolver
	vpcInfoProvider     networking.VPCInfoProvider
	backendSGProvider   networking.BackendSGProvider
	sgResolver          networking.SecurityGroupResolver
	trackingProvider    tracking.Provider
	elbv2TaggingManager elbv2deploy.TaggingManager
	featureGates        config.FeatureGates
	serviceUtils        ServiceUtils
	enableIPTargetType  bool
	ec2Client           services.EC2

	service *corev1.Service

	stack                    core.Stack
	loadBalancer             *elbv2model.LoadBalancer
	tgByResID                map[string]*elbv2model.TargetGroup
	ec2Subnets               []*ec2.Subnet
	enableBackendSG          bool
	disableRestrictedSGRules bool
	backendSGIDToken         core.StringToken
	backendSGAllocated       bool
	preserveClientIP         bool

	fetchExistingLoadBalancerOnce sync.Once
	existingLoadBalancer          *elbv2deploy.LoadBalancerWithTags

	defaultTags                          map[string]string
	externalManagedTags                  sets.String
	defaultSSLPolicy                     string
	defaultAccessLogS3Enabled            bool
	defaultAccessLogsS3Bucket            string
	defaultAccessLogsS3Prefix            string
	defaultIPAddressType                 elbv2model.IPAddressType
	defaultLoadBalancingCrossZoneEnabled bool
	defaultProxyProtocolV2Enabled        bool
	defaultTargetType                    elbv2model.TargetType
	defaultHealthCheckProtocol           elbv2model.Protocol
	defaultHealthCheckPort               string
	defaultHealthCheckPath               string
	defaultHealthCheckInterval           int64
	defaultHealthCheckTimeout            int64
	defaultHealthCheckHealthyThreshold   int64
	defaultHealthCheckUnhealthyThreshold int64
	defaultHealthCheckMatcherHTTPCode    string
	defaultDeletionProtectionEnabled     bool
	defaultIPv4SourceRanges              []string
	defaultIPv6SourceRanges              []string

	// Default health check settings for NLB instance mode with spec.ExternalTrafficPolicy set to Local
	defaultHealthCheckProtocolForInstanceModeLocal           elbv2model.Protocol
	defaultHealthCheckPortForInstanceModeLocal               string
	defaultHealthCheckPathForInstanceModeLocal               string
	defaultHealthCheckIntervalForInstanceModeLocal           int64
	defaultHealthCheckTimeoutForInstanceModeLocal            int64
	defaultHealthCheckHealthyThresholdForInstanceModeLocal   int64
	defaultHealthCheckUnhealthyThresholdForInstanceModeLocal int64
}

func (t *defaultModelBuildTask) run(ctx context.Context) error {
	if !t.serviceUtils.IsServiceSupported(t.service) {
		if t.serviceUtils.IsServicePendingFinalization(t.service) {
			deletionProtectionEnabled, err := t.getDeletionProtectionViaAnnotation(*t.service)
			if err != nil {
				return err
			}
			if deletionProtectionEnabled {
				return errors.Errorf("deletion_protection is enabled, cannot delete the service: %v", t.service.Name)
			}
		}

		t.cleanupStackWithRemovingService()
		return nil
	}
	t.stack.AddService(t.service)
	err := t.buildModel(ctx)
	return err
}

// When service is deleted, update resources in the stack to make sure things are cleaned up properly.
func (t *defaultModelBuildTask) cleanupStackWithRemovingService() {
	for _, port := range t.service.Spec.Ports {
		t.stack.RemoveResource(graph.ResourceUID{
			ResID:   fmt.Sprintf("%v", port.NodePort),
			ResType: reflect.TypeOf(&elbv2model.Listener{}),
		})
		svcPort := intstr.FromInt(int(port.Port))
		tgResourceID := t.buildTargetGroupResourceID(k8s.NamespacedName(t.service), svcPort)
		var targetGroups []*elbv2model.TargetGroup
		var targetGroupBindingResources []*elbv2model.TargetGroupBindingResource
		t.stack.ListResources(&targetGroups)
		t.stack.ListResources(&targetGroupBindingResources)
		for _, tg := range targetGroups {
			if tg.ID() == tgResourceID {
				t.stack.RemoveResource(graph.ResourceUID{
					ResID:   tg.ID(),
					ResType: reflect.TypeOf(tg),
				})
			}
		}
		for _, tgBinding := range targetGroupBindingResources {
			if tgBinding.ID() == tgResourceID {
				t.stack.RemoveResource(graph.ResourceUID{
					ResID:   tgBinding.ID(),
					ResType: reflect.TypeOf(tgBinding),
				})
			}
		}
	}
	// Delete the load balancer if there is no listener left.
	var resLSs []*elbv2model.Listener
	t.stack.ListResources(&resLSs)
	if len(resLSs) == 0 {
		t.stack.RemoveResource(graph.ResourceUID{
			ResID:   "LoadBalancer",
			ResType: reflect.TypeOf(&elbv2model.LoadBalancer{}),
		})
		t.loadBalancer = nil
	}
	t.stack.RemoveService(t.service)
}

func (t *defaultModelBuildTask) buildModel(ctx context.Context) error {
	// always lock the stack building models. Since stack can be accessed by multiple goroutines from multiple service reconciliation loops,
	// make sure only one goroutine is building the model at a time to guarantee thread safety.
	t.stack.Lock()
	defer t.stack.Unlock()
	scheme, err := t.buildLoadBalancerScheme(ctx)
	if err != nil {
		return err
	}
	t.ec2Subnets, err = t.buildLoadBalancerSubnets(ctx, scheme)
	if err != nil {
		return err
	}
	err = t.buildLoadBalancer(ctx, scheme)
	if err != nil {
		return err
	}
	err = t.buildListeners(ctx, scheme)
	if err != nil {
		return err
	}
	return nil
}

func (t *defaultModelBuildTask) getDeletionProtectionViaAnnotation(svc corev1.Service) (bool, error) {
	var lbAttributes map[string]string
	_, err := t.annotationParser.ParseStringMapAnnotation(annotations.SvcLBSuffixLoadBalancerAttributes, &lbAttributes, svc.Annotations)
	if err != nil {
		return false, err
	}
	if _, deletionProtectionSpecified := lbAttributes[lbAttrsDeletionProtection]; deletionProtectionSpecified {
		deletionProtectionEnabled, err := strconv.ParseBool(lbAttributes[lbAttrsDeletionProtection])
		if err != nil {
			return false, err
		}
		return deletionProtectionEnabled, nil
	}
	return false, nil
}

func (t *defaultModelBuildTask) stackID() core.StackID {
	stackID := core.StackID(k8s.NamespacedName(t.service))
	if t.service.Annotations[LoadBalancerStackKey] != "" {
		stackID = core.StackID(types.NamespacedName{
			Namespace: "stack",
			Name:      t.service.Annotations[LoadBalancerStackKey],
		})
	}
	return stackID
}
