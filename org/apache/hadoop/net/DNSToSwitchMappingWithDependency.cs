using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>
	/// An interface that must be implemented to allow pluggable
	/// DNS-name/IP-address to RackID resolvers.
	/// </summary>
	public interface DNSToSwitchMappingWithDependency : org.apache.hadoop.net.DNSToSwitchMapping
	{
		/// <summary>Get a list of dependent DNS-names for a given DNS-name/IP-address.</summary>
		/// <remarks>
		/// Get a list of dependent DNS-names for a given DNS-name/IP-address.
		/// Dependent DNS-names fall into the same fault domain which must be
		/// taken into account when placing replicas. This is intended to be used for
		/// cross node group dependencies when node groups are not sufficient to
		/// distinguish data nodes by fault domains. In practice, this is needed when
		/// a compute server runs VMs which use shared storage (as opposite to
		/// directly attached storage). In this case data nodes fall in two different
		/// fault domains. One fault domain is defined by a compute server and
		/// the other is defined by storage. With node groups we can group data nodes
		/// either by server fault domain or by storage fault domain. However one of
		/// the fault domains cannot be handled and there we need to define cross node
		/// group dependencies. These dependencies are applied in block placement
		/// polices which ensure that no two replicas will be on two dependent nodes.
		/// </remarks>
		/// <param name="name">
		/// - host name or IP address of a data node. Input host name
		/// parameter must take a value of dfs.datanode.hostname config value if this
		/// config property is set. Otherwise FQDN of the data node is used.
		/// </param>
		/// <returns>
		/// list of dependent host names. If dfs.datanode.hostname config
		/// property is set, then its value must be returned.
		/// Otherwise, FQDN is returned.
		/// </returns>
		System.Collections.Generic.IList<string> getDependency(string name);
	}
}
