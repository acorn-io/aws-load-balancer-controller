package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/aws-load-balancer-controller/controllers/service/eventhandlers"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/annotations"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/aws"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/config"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/deploy"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/deploy/elbv2"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/deploy/tracking"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/k8s"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/model/core"
	elbv2model "sigs.k8s.io/aws-load-balancer-controller/pkg/model/elbv2"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/networking"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/runtime"
	"sigs.k8s.io/aws-load-balancer-controller/pkg/service"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	serviceFinalizer        = "service.k8s.aws/resources"
	serviceTagPrefix        = "service.k8s.aws"
	serviceAnnotationPrefix = "service.beta.kubernetes.io"
	controllerName          = "service"

	nlbListenerLimit = 50
)

func NewServiceReconciler(cloud aws.Cloud, k8sClient client.Client, eventRecorder record.EventRecorder,
	finalizerManager k8s.FinalizerManager, networkingSGManager networking.SecurityGroupManager,
	networkingSGReconciler networking.SecurityGroupReconciler, subnetsResolver networking.SubnetsResolver,
	vpcInfoProvider networking.VPCInfoProvider, controllerConfig config.ControllerConfig,
	backendSGProvider networking.BackendSGProvider, sgResolver networking.SecurityGroupResolver, logger logr.Logger) *serviceReconciler {

	annotationParser := annotations.NewSuffixAnnotationParser(serviceAnnotationPrefix)
	trackingProvider := tracking.NewDefaultProvider(serviceTagPrefix, controllerConfig.ClusterName)
	elbv2TaggingManager := elbv2.NewDefaultTaggingManager(cloud.ELBV2(), cloud.VpcID(), controllerConfig.FeatureGates, cloud.RGT(), logger)
	serviceUtils := service.NewServiceUtils(annotationParser, serviceFinalizer, controllerConfig.ServiceConfig.LoadBalancerClass, controllerConfig.FeatureGates)
	modelBuilder := service.NewDefaultModelBuilder(annotationParser, subnetsResolver, vpcInfoProvider, cloud.VpcID(), trackingProvider,
		elbv2TaggingManager, cloud.EC2(), controllerConfig.FeatureGates, controllerConfig.ClusterName, controllerConfig.DefaultTags, controllerConfig.ExternalManagedTags,
		controllerConfig.DefaultSSLPolicy, controllerConfig.DefaultTargetType, controllerConfig.FeatureGates.Enabled(config.EnableIPTargetType), serviceUtils,
		backendSGProvider, sgResolver, controllerConfig.EnableBackendSecurityGroup, controllerConfig.DisableRestrictedSGRules, k8sClient)
	stackMarshaller := deploy.NewDefaultStackMarshaller()
	stackDeployer := deploy.NewDefaultStackDeployer(cloud, k8sClient, networkingSGManager, networkingSGReconciler, controllerConfig, serviceTagPrefix, logger)
	return &serviceReconciler{
		k8sClient:         k8sClient,
		eventRecorder:     eventRecorder,
		finalizerManager:  finalizerManager,
		annotationParser:  annotationParser,
		loadBalancerClass: controllerConfig.ServiceConfig.LoadBalancerClass,
		serviceUtils:      serviceUtils,
		backendSGProvider: backendSGProvider,

		modelBuilder:    modelBuilder,
		stackMarshaller: stackMarshaller,
		stackDeployer:   stackDeployer,
		logger:          logger,

		allocatedServices: map[string]map[int32]struct{}{},
		lock:              sync.Mutex{},

		maxConcurrentReconciles: controllerConfig.ServiceMaxConcurrentReconciles,
	}
}

type serviceReconciler struct {
	k8sClient         client.Client
	eventRecorder     record.EventRecorder
	finalizerManager  k8s.FinalizerManager
	annotationParser  annotations.Parser
	loadBalancerClass string
	serviceUtils      service.ServiceUtils
	backendSGProvider networking.BackendSGProvider

	modelBuilder    service.ModelBuilder
	stackMarshaller deploy.StackMarshaller
	stackDeployer   deploy.StackDeployer
	logger          logr.Logger

	allocatedServices       map[string]map[int32]struct{}
	initialized             bool
	lock                    sync.Mutex
	maxConcurrentReconciles int
}

// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=services/status,verbs=update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *serviceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return runtime.HandleReconcileError(r.reconcile(ctx, req), r.logger)
}

func (r *serviceReconciler) reconcile(ctx context.Context, req ctrl.Request) error {
	svc := &corev1.Service{}
	if err := r.k8sClient.Get(ctx, req.NamespacedName, svc); err != nil {
		return client.IgnoreNotFound(err)
	}
	if svc.Annotations[service.LoadBalancerAllocatingPortKey] == "true" {
		// AllocateService has to be locked to guarantee thread-safe since it read/writes map concurrently
		r.lock.Lock()
		if err := r.allocatedService(ctx, svc); err != nil {
			r.lock.Unlock()
			return err
		}
		r.lock.Unlock()
	}

	stack, lb, backendSGRequired, err := r.buildModel(ctx, svc)
	if err != nil {
		return err
	}
	if lb == nil || !svc.DeletionTimestamp.IsZero() {
		return r.cleanupLoadBalancerResources(ctx, svc, stack, lb == nil)
	}
	return r.reconcileLoadBalancerResources(ctx, svc, stack, lb, backendSGRequired)
}

// AllocateService makes sure that each service is allocated to a virtual stack, and a stack will not have more than 50 service/listener(the limit of listener on NLB).
// It maintains an in-memory cache to be able to track the usage. If no stack is available, it will create a new stack.
func (r *serviceReconciler) allocatedService(ctx context.Context, svc *corev1.Service) error {
	if !r.initialized {
		var serviceList corev1.ServiceList
		if err := r.k8sClient.List(ctx, &serviceList); err != nil {
			return err
		}
		for _, svc := range serviceList.Items {
			if svc.Annotations[service.LoadBalancerStackKey] != "" {
				if r.allocatedServices[svc.Annotations[service.LoadBalancerStackKey]] == nil {
					r.allocatedServices[svc.Annotations[service.LoadBalancerStackKey]] = map[int32]struct{}{}
				}
				for _, port := range svc.Spec.Ports {
					r.allocatedServices[svc.Annotations[service.LoadBalancerStackKey]][port.NodePort] = struct{}{}
				}
			}
		}
		r.initialized = true
	}

	if !svc.DeletionTimestamp.IsZero() {
		if _, ok := r.allocatedServices[svc.Annotations[service.LoadBalancerStackKey]]; !ok {
			return nil
		}

		for _, port := range svc.Spec.Ports {
			delete(r.allocatedServices[svc.Annotations[service.LoadBalancerStackKey]], port.NodePort)
		}

		if len(r.allocatedServices[svc.Annotations[service.LoadBalancerStackKey]]) == 0 {
			delete(r.allocatedServices, svc.Annotations[service.LoadBalancerStackKey])
		}

		return nil
	}

	// If service is not type loadbalancer, or it is not intended to share LB, or it has been allocated, skip the controller
	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer || svc.Annotations[service.LoadBalancerAllocatingPortKey] != "true" || svc.Annotations[service.LoadBalancerStackKey] != "" {
		return nil
	}

	allocated := false
	for stackName := range r.allocatedServices {
		usedPort := r.allocatedServices[stackName]
		if len(usedPort) <= nlbListenerLimit-len(svc.Spec.Ports) {
			svc.Annotations[service.LoadBalancerStackKey] = stackName
			if err := r.k8sClient.Update(ctx, svc); err != nil {
				return err
			}
			for _, port := range svc.Spec.Ports {
				usedPort[port.NodePort] = struct{}{}
			}
			r.allocatedServices[stackName] = usedPort
			allocated = true
			break
		}
	}

	if !allocated {
		stackName := uuid.New().String()
		svc.Annotations[service.LoadBalancerStackKey] = stackName
		if err := r.k8sClient.Update(ctx, svc); err != nil {
			return err
		}
		if r.allocatedServices[stackName] == nil {
			r.allocatedServices[stackName] = map[int32]struct{}{}
		}
		for _, port := range svc.Spec.Ports {
			r.allocatedServices[stackName][port.NodePort] = struct{}{}
		}
	}
	return nil
}

func (r *serviceReconciler) buildModel(ctx context.Context, svc *corev1.Service) (core.Stack, *elbv2model.LoadBalancer, bool, error) {
	stack, lb, backendSGRequired, err := r.modelBuilder.Build(ctx, svc)
	if err != nil {
		r.eventRecorder.Event(svc, corev1.EventTypeWarning, k8s.ServiceEventReasonFailedBuildModel, fmt.Sprintf("Failed build model due to %v", err))
		return nil, nil, false, err
	}
	stackJSON, err := r.stackMarshaller.Marshal(stack)
	if err != nil {
		r.eventRecorder.Event(svc, corev1.EventTypeWarning, k8s.ServiceEventReasonFailedBuildModel, fmt.Sprintf("Failed build model due to %v", err))
		return nil, nil, false, err
	}
	r.logger.Info("successfully built model", "model", stackJSON)
	return stack, lb, backendSGRequired, nil
}

func (r *serviceReconciler) deployModel(ctx context.Context, svc *corev1.Service, stack core.Stack) error {
	if err := r.stackDeployer.Deploy(ctx, stack); err != nil {
		r.eventRecorder.Event(svc, corev1.EventTypeWarning, k8s.ServiceEventReasonFailedDeployModel, fmt.Sprintf("Failed deploy model due to %v", err))
		return err
	}
	r.logger.Info("successfully deployed model", "service", k8s.NamespacedName(svc))

	return nil
}

func (r *serviceReconciler) reconcileLoadBalancerResources(ctx context.Context, svc *corev1.Service, stack core.Stack,
	lb *elbv2model.LoadBalancer, backendSGRequired bool) error {
	if err := r.finalizerManager.AddFinalizers(ctx, svc, serviceFinalizer); err != nil {
		r.eventRecorder.Event(svc, corev1.EventTypeWarning, k8s.ServiceEventReasonFailedAddFinalizer, fmt.Sprintf("Failed add finalizer due to %v", err))
		return err
	}
	// always lock the stack deploying models. Since stack can be accessed by multiple goroutines from multiple service reconciliation loops,
	// make sure only one goroutine is building the model at a time to guarantee thread safety.
	stack.Lock()
	err := r.deployModel(ctx, svc, stack)
	if err != nil {
		stack.Unlock()
		return err
	}
	stack.Unlock()
	lbDNS, err := lb.DNSName().Resolve(ctx)
	if err != nil {
		return err
	}

	if !backendSGRequired {
		if err := r.backendSGProvider.Release(ctx, networking.ResourceTypeService, []types.NamespacedName{k8s.NamespacedName(svc)}); err != nil {
			return err
		}
	}

	if err = r.updateServiceStatus(ctx, lbDNS, svc); err != nil {
		r.eventRecorder.Event(svc, corev1.EventTypeWarning, k8s.ServiceEventReasonFailedUpdateStatus, fmt.Sprintf("Failed update status due to %v", err))
		return err
	}
	r.eventRecorder.Event(svc, corev1.EventTypeNormal, k8s.ServiceEventReasonSuccessfullyReconciled, "Successfully reconciled")
	return nil
}

func (r *serviceReconciler) cleanupLoadBalancerResources(ctx context.Context, svc *corev1.Service, stack core.Stack, cleanlb bool) error {
	if k8s.HasFinalizer(svc, serviceFinalizer) {
		stack.Lock()
		err := r.deployModel(ctx, svc, stack)
		if err != nil {
			stack.Unlock()
			return err
		}
		stack.Unlock()
		if cleanlb {
			nsName := k8s.NamespacedName(svc)
			if svc.Annotations[service.LoadBalancerAllocatingPortKey] == "true" {
				nsName = types.NamespacedName{
					Namespace: "stack",
					Name:      svc.Annotations[service.LoadBalancerStackKey],
				}
			}
			if err := r.backendSGProvider.Release(ctx, networking.ResourceTypeService, []types.NamespacedName{nsName}); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}

		if err = r.cleanupServiceStatus(ctx, svc); err != nil {
			r.eventRecorder.Event(svc, corev1.EventTypeWarning, k8s.ServiceEventReasonFailedCleanupStatus, fmt.Sprintf("Failed update status due to %v", err))
			return err
		}
		if err := r.finalizerManager.RemoveFinalizers(ctx, svc, serviceFinalizer); err != nil {
			r.eventRecorder.Event(svc, corev1.EventTypeWarning, k8s.ServiceEventReasonFailedRemoveFinalizer, fmt.Sprintf("Failed remove finalizer due to %v", err))
			return err
		}
	}
	return nil
}

func (r *serviceReconciler) updateServiceStatus(ctx context.Context, lbDNS string, svc *corev1.Service) error {
	if len(svc.Status.LoadBalancer.Ingress) != 1 ||
		svc.Status.LoadBalancer.Ingress[0].IP != "" ||
		svc.Status.LoadBalancer.Ingress[0].Hostname != lbDNS {
		svcOld := svc.DeepCopy()
		ingress := corev1.LoadBalancerIngress{
			Hostname: lbDNS,
		}
		if svc.Annotations[service.LoadBalancerAllocatingPortKey] == "true" {
			for _, port := range svc.Spec.Ports {
				ingress.Ports = append(ingress.Ports, corev1.PortStatus{
					Port: port.NodePort,
				})
			}
		}
		svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{ingress}
		if err := r.k8sClient.Status().Patch(ctx, svc, client.MergeFrom(svcOld)); err != nil {
			return errors.Wrapf(err, "failed to update service status: %v", k8s.NamespacedName(svc))
		}
	}
	return nil
}

func (r *serviceReconciler) cleanupServiceStatus(ctx context.Context, svc *corev1.Service) error {
	svcOld := svc.DeepCopy()
	svc.Status.LoadBalancer = corev1.LoadBalancerStatus{}
	if err := r.k8sClient.Status().Patch(ctx, svc, client.MergeFrom(svcOld)); err != nil {
		return errors.Wrapf(err, "failed to cleanup service status: %v", k8s.NamespacedName(svc))
	}
	return nil
}

func (r *serviceReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	c, err := controller.New(controllerName, mgr, controller.Options{
		MaxConcurrentReconciles: r.maxConcurrentReconciles,
		Reconciler:              r,
	})
	if err != nil {
		return err
	}
	if err := r.setupWatches(ctx, c); err != nil {
		return err
	}

	return nil
}

func (r *serviceReconciler) setupWatches(_ context.Context, c controller.Controller) error {
	svcEventHandler := eventhandlers.NewEnqueueRequestForServiceEvent(r.eventRecorder,
		r.serviceUtils, r.logger.WithName("eventHandlers").WithName("service"))
	if err := c.Watch(&source.Kind{Type: &corev1.Service{}}, svcEventHandler); err != nil {
		return err
	}
	return nil
}
