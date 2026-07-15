package collector

import (
	"context"
	"net/netip"
	"sort"
	"strings"

	"github.com/elohmeier/avi-exporter/avi"
)

type seAddressRecord struct {
	IP          string
	Role        string
	Interface   string
	Network     string
	NetworkUUID string
	VRF         string
	VRFUUID     string
	CIDR        string
	MAC         string
}

func (e *Exporter) refreshClusterInventory(ctx context.Context) error {
	cluster, err := e.client.GetCluster(ctx)
	if err != nil {
		return err
	}

	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	e.clusterNodeInfo.Reset()
	for _, node := range cluster.Nodes {
		e.clusterNodeInfo.WithLabelValues(e.appendLabels(
			node.Name,
			cluster.Name,
			cluster.UUID,
			node.IP.Addr,
			node.IP6.Addr,
			node.PublicIPOrName.Addr,
			node.VMHostname,
			node.VMName,
			node.VMUUID,
		)...).Set(1)
	}
	return nil
}

func (e *Exporter) refreshSEConfig(ctx context.Context) error {
	items, err := e.client.ListSEConfig(ctx)
	if err != nil {
		return err
	}

	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	resetGaugeVecs(e.seInfo, e.seAddressInfo)
	for _, item := range items {
		e.collectSEConfig(item)
	}
	return nil
}

func (e *Exporter) collectSEConfig(item avi.SEConfig) {
	seName := item.Name
	if seName == "" {
		seName = item.UUID
	}
	cloudName, cloudUUID := referenceLabels(item.CloudRef)
	groupName, groupUUID := referenceLabels(item.SeGroupRef)
	tenantName, tenantUUID := referenceLabels(item.TenantRef)

	e.seInfo.WithLabelValues(e.appendLabels(
		seName,
		item.UUID,
		cloudName,
		cloudUUID,
		groupName,
		groupUUID,
		tenantName,
		tenantUUID,
		item.ControllerIP,
		item.AvailabilityZone,
	)...).Set(1)

	for _, address := range serviceEngineAddresses(item) {
		e.seAddressInfo.WithLabelValues(e.appendLabels(
			seName,
			item.UUID,
			address.IP,
			address.Role,
			address.Interface,
			address.Network,
			address.NetworkUUID,
			address.VRF,
			address.VRFUUID,
			address.CIDR,
			address.MAC,
		)...).Set(1)
	}
}

func referenceLabels(ref string) (string, string) {
	return avi.RefName(ref), avi.RefUUID(ref)
}

func serviceEngineAddresses(item avi.SEConfig) []seAddressRecord {
	byIP := make(map[string]seAddressRecord)
	add := func(candidate seAddressRecord) {
		parsed, err := netip.ParseAddr(strings.TrimSpace(candidate.IP))
		if err != nil {
			return
		}
		candidate.IP = parsed.Unmap().String()
		if existing, ok := byIP[candidate.IP]; ok {
			candidate = preferAddress(existing, candidate)
		}
		byIP[candidate.IP] = candidate
	}

	if item.MgmtIPAddress != nil {
		add(seAddressRecord{IP: item.MgmtIPAddress.Addr, Role: "management"})
	}
	if item.MgmtIP6Address != nil {
		add(seAddressRecord{IP: item.MgmtIP6Address.Addr, Role: "management"})
	}
	if item.MgmtVNIC != nil {
		collectVNICAddresses(*item.MgmtVNIC, "management", add)
	}
	for _, vnic := range item.DataVNICs {
		role := "data"
		if vnic.IsMgmt {
			role = "management"
		}
		collectVNICAddresses(vnic, role, add)
	}

	out := make([]seAddressRecord, 0, len(byIP))
	for _, address := range byIP {
		out = append(out, address)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].IP < out[j].IP })
	return out
}

func collectVNICAddresses(vnic avi.VNIC, role string, add func(seAddressRecord)) {
	networkName := vnic.NetworkName
	if networkName == "" {
		networkName = avi.RefName(vnic.NetworkRef)
	}
	networkUUID := avi.RefUUID(vnic.NetworkRef)
	vrfName, vrfUUID := referenceLabels(vnic.VrfRef)
	for _, network := range vnic.VNICNetworks {
		add(addressFromVNICNetwork(network, role, vnic.IfName, networkName, networkUUID, vrfName, vrfUUID, vnic.MacAddress))
	}

	for _, vlan := range vnic.VLANInterfaces {
		vlanRole := role
		if vlan.IsMgmt {
			vlanRole = "management"
		}
		ifName := vlan.IfName
		if ifName == "" {
			ifName = vnic.IfName
		}
		vlanVRFName, vlanVRFUUID := referenceLabels(vlan.VrfRef)
		if vlanVRFName == "" && vlanVRFUUID == "" {
			vlanVRFName, vlanVRFUUID = vrfName, vrfUUID
		}
		for _, network := range vlan.VNICNetworks {
			add(addressFromVNICNetwork(network, vlanRole, ifName, networkName, networkUUID, vlanVRFName, vlanVRFUUID, vnic.MacAddress))
		}
	}
}

func addressFromVNICNetwork(network avi.VNICNetwork, role, ifName, networkName, networkUUID, vrfName, vrfUUID, mac string) seAddressRecord {
	return seAddressRecord{
		IP:          network.IP.IPAddr.Addr,
		Role:        role,
		Interface:   ifName,
		Network:     networkName,
		NetworkUUID: networkUUID,
		VRF:         vrfName,
		VRFUUID:     vrfUUID,
		CIDR:        normalizedCIDR(network.IP.IPAddr.Addr, network.IP.Mask),
		MAC:         mac,
	}
}

func normalizedCIDR(raw string, bits *int) string {
	addr, err := netip.ParseAddr(strings.TrimSpace(raw))
	if err != nil || bits == nil {
		return ""
	}
	addr = addr.Unmap()
	if *bits < 0 || *bits > addr.BitLen() {
		return ""
	}
	return netip.PrefixFrom(addr, *bits).Masked().String()
}

func preferAddress(a, b seAddressRecord) seAddressRecord {
	winner, other := a, b
	aScore, bScore := addressScore(a), addressScore(b)
	if bScore > aScore || (bScore == aScore && addressKey(b) < addressKey(a)) {
		winner, other = b, a
	}
	if winner.Role == other.Role {
		fillAddressGaps(&winner, other)
	}
	return winner
}

func addressScore(address seAddressRecord) int {
	score := 0
	if address.Role == "management" {
		score += 100
	}
	for _, value := range []string{address.Interface, address.Network, address.NetworkUUID, address.VRF, address.VRFUUID, address.CIDR, address.MAC} {
		if value != "" {
			score++
		}
	}
	return score
}

func addressKey(address seAddressRecord) string {
	return strings.Join([]string{address.Role, address.Interface, address.Network, address.NetworkUUID, address.VRF, address.VRFUUID, address.CIDR, address.MAC}, "\x00")
}

func fillAddressGaps(target *seAddressRecord, other seAddressRecord) {
	if target.Interface == "" {
		target.Interface = other.Interface
	}
	if target.Network == "" {
		target.Network = other.Network
	}
	if target.NetworkUUID == "" {
		target.NetworkUUID = other.NetworkUUID
	}
	if target.VRF == "" {
		target.VRF = other.VRF
	}
	if target.VRFUUID == "" {
		target.VRFUUID = other.VRFUUID
	}
	if target.CIDR == "" {
		target.CIDR = other.CIDR
	}
	if target.MAC == "" {
		target.MAC = other.MAC
	}
}
