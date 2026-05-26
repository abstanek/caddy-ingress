package controller

import (
	"net"
	"sort"
	"strings"

	"github.com/caddyserver/ingress/internal/k8s"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/client-go/kubernetes"
)

// dispatchSync is run every syncInterval duration to sync ingress source address fields.
func (c *CaddyController) dispatchSync() {
	c.syncQueue.Add(SyncStatusAction{})
}

// SyncStatusAction provides an implementation of the action interface.
type SyncStatusAction struct {
}

// handle is run when a syncStatusAction appears in the queue.
func (r SyncStatusAction) handle(c *CaddyController) error {
	return c.syncStatus(c.resourceStore.Ingresses)
}

// syncStatus ensures that the ingress source address points to this ingress controller's IP address.
func (c *CaddyController) syncStatus(ings []*networkingv1.Ingress) error {
	addrs, err := k8s.GetAddresses(c.resourceStore.CurrentPod, c.kubeClient)
	if err != nil {
		return err
	}

	c.logger.Debugf("Syncing %d Ingress resources source addresses", len(ings))
	c.updateIngStatuses(sliceToLoadBalancerIngress(addrs), ings)

	return nil
}

// updateIngStatuses starts a queue and adds all monitored ingresses to update their status source address to the on
// that the ingress controller is running on. This is called by the syncStatus queue.
func (c *CaddyController) updateIngStatuses(controllerAddresses []networkingv1.IngressLoadBalancerIngress, ings []*networkingv1.Ingress) {
	var g errgroup.Group
	g.SetLimit(10)
	sort.SliceStable(controllerAddresses, lessLoadBalancerIngress(controllerAddresses))

	for _, ing := range ings {
		curIPs := ing.Status.LoadBalancer.Ingress
		sort.SliceStable(curIPs, lessLoadBalancerIngress(curIPs))

		// check to see if ingresses source address does not match the ingress controller's.
		if ingressSliceEqual(curIPs, controllerAddresses) {
			c.logger.Debugf("skipping update of Ingress %v/%v (no change)", ing.Namespace, ing.Name)
			continue
		}

		g.Go(runUpdate(c.logger, ing, controllerAddresses, c.kubeClient))
	}

	_ = g.Wait()
}

// runUpdate updates the ingress status field.
func runUpdate(logger *zap.SugaredLogger, ing *networkingv1.Ingress, status []networkingv1.IngressLoadBalancerIngress, client *kubernetes.Clientset) func() error {
	return func() error {
		updated, err := k8s.UpdateIngressStatus(client, ing, status)
		if err != nil {
			logger.Warnf("error updating ingress rule: %v", err)
			return nil
		}
		logger.Debugf(
			"updating Ingress %v/%v status from %v to %v",
			ing.Namespace,
			ing.Name,
			ing.Status.LoadBalancer.Ingress,
			updated.Status.LoadBalancer.Ingress,
		)
		return nil
	}
}

// ingressSliceEqual determines if the ingress source matches the ingress controller's.
func ingressSliceEqual(lhs, rhs []networkingv1.IngressLoadBalancerIngress) bool {
	if len(lhs) != len(rhs) {
		return false
	}

	for i := range lhs {
		if lhs[i].IP != rhs[i].IP {
			return false
		}
		if lhs[i].Hostname != rhs[i].Hostname {
			return false
		}
	}

	return true
}

// lessLoadBalancerIngress is a sorting function for ingress hostnames.
func lessLoadBalancerIngress(addrs []networkingv1.IngressLoadBalancerIngress) func(int, int) bool {
	return func(a, b int) bool {
		switch strings.Compare(addrs[a].Hostname, addrs[b].Hostname) {
		case -1:
			return true
		case 1:
			return false
		}
		return addrs[a].IP < addrs[b].IP
	}
}

// sliceToLoadBalancerIngress converts a slice of IP and/or hostnames to LoadBalancerIngress
func sliceToLoadBalancerIngress(endpoints []string) []networkingv1.IngressLoadBalancerIngress {
	lbi := []networkingv1.IngressLoadBalancerIngress{}
	for _, ep := range endpoints {
		if net.ParseIP(ep) == nil {
			lbi = append(lbi, networkingv1.IngressLoadBalancerIngress{Hostname: ep})
		} else {
			lbi = append(lbi, networkingv1.IngressLoadBalancerIngress{IP: ep})
		}
	}

	sort.SliceStable(lbi, func(a, b int) bool {
		return lbi[a].IP < lbi[b].IP
	})

	return lbi
}
