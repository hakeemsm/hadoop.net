using System.Collections.Generic;


namespace Org.Apache.Hadoop.Net
{
	/// <summary>
	/// An interface that must be implemented to allow pluggable
	/// DNS-name/IP-address to RackID resolvers.
	/// </summary>
	public interface DNSToSwitchMapping
	{
		/// <summary>
		/// Resolves a list of DNS-names/IP-addresses and returns back a list of
		/// switch information (network paths).
		/// </summary>
		/// <remarks>
		/// Resolves a list of DNS-names/IP-addresses and returns back a list of
		/// switch information (network paths). One-to-one correspondence must be
		/// maintained between the elements in the lists.
		/// Consider an element in the argument list - x.y.com. The switch information
		/// that is returned must be a network path of the form /foo/rack,
		/// where / is the root, and 'foo' is the switch where 'rack' is connected.
		/// Note the hostname/ip-address is not part of the returned path.
		/// The network topology of the cluster would determine the number of
		/// components in the network path.
		/// <p/>
		/// If a name cannot be resolved to a rack, the implementation
		/// should return
		/// <see cref="NetworkTopology.DefaultRack"/>
		/// . This
		/// is what the bundled implementations do, though it is not a formal requirement
		/// </remarks>
		/// <param name="names">the list of hosts to resolve (can be empty)</param>
		/// <returns>
		/// list of resolved network paths.
		/// If <i>names</i> is empty, the returned list is also empty
		/// </returns>
		IList<string> Resolve(IList<string> names);

		/// <summary>Reload all of the cached mappings.</summary>
		/// <remarks>
		/// Reload all of the cached mappings.
		/// If there is a cache, this method will clear it, so that future accesses
		/// will get a chance to see the new data.
		/// </remarks>
		void ReloadCachedMappings();

		/// <summary>Reload cached mappings on specific nodes.</summary>
		/// <remarks>
		/// Reload cached mappings on specific nodes.
		/// If there is a cache on these nodes, this method will clear it, so that
		/// future accesses will see updated data.
		/// </remarks>
		void ReloadCachedMappings(IList<string> names);
	}
}
