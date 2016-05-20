using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>
	/// A class that provides direct and reverse lookup functionalities, allowing
	/// the querying of specific network interfaces or nameservers.
	/// </summary>
	public class DNS
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.net.DNS)));

		/// <summary>The cached hostname -initially null.</summary>
		private static readonly string cachedHostname = resolveLocalHostname();

		private static readonly string cachedHostAddress = resolveLocalHostIPAddress();

		private const string LOCALHOST = "localhost";

		/// <summary>
		/// Returns the hostname associated with the specified IP address by the
		/// provided nameserver.
		/// </summary>
		/// <remarks>
		/// Returns the hostname associated with the specified IP address by the
		/// provided nameserver.
		/// Loopback addresses
		/// </remarks>
		/// <param name="hostIp">The address to reverse lookup</param>
		/// <param name="ns">The host name of a reachable DNS server</param>
		/// <returns>The host name associated with the provided IP</returns>
		/// <exception cref="javax.naming.NamingException">If a NamingException is encountered
		/// 	</exception>
		public static string reverseDns(java.net.InetAddress hostIp, string ns)
		{
			//
			// Builds the reverse IP lookup form
			// This is formed by reversing the IP numbers and appending in-addr.arpa
			//
			string[] parts = hostIp.getHostAddress().split("\\.");
			string reverseIP = parts[3] + "." + parts[2] + "." + parts[1] + "." + parts[0] + 
				".in-addr.arpa";
			javax.naming.directory.DirContext ictx = new javax.naming.directory.InitialDirContext
				();
			javax.naming.directory.Attributes attribute;
			try
			{
				attribute = ictx.getAttributes("dns://" + ((ns == null) ? string.Empty : ns) + "/"
					 + reverseIP, new string[] { "PTR" });
			}
			finally
			{
				// Use "dns:///" if the default
				// nameserver is to be used
				ictx.close();
			}
			string hostname = attribute.get("PTR").get().ToString();
			int hostnameLength = hostname.Length;
			if (hostname[hostnameLength - 1] == '.')
			{
				hostname = Sharpen.Runtime.substring(hostname, 0, hostnameLength - 1);
			}
			return hostname;
		}

		/// <returns>
		/// NetworkInterface for the given subinterface name (eg eth0:0)
		/// or null if no interface with the given name can be found
		/// </returns>
		/// <exception cref="System.Net.Sockets.SocketException"/>
		private static java.net.NetworkInterface getSubinterface(string strInterface)
		{
			java.util.Enumeration<java.net.NetworkInterface> nifs = java.net.NetworkInterface
				.getNetworkInterfaces();
			while (nifs.MoveNext())
			{
				java.util.Enumeration<java.net.NetworkInterface> subNifs = nifs.Current.getSubInterfaces
					();
				while (subNifs.MoveNext())
				{
					java.net.NetworkInterface nif = subNifs.Current;
					if (nif.getName().Equals(strInterface))
					{
						return nif;
					}
				}
			}
			return null;
		}

		/// <param name="nif">network interface to get addresses for</param>
		/// <returns>
		/// set containing addresses for each subinterface of nif,
		/// see below for the rationale for using an ordered set
		/// </returns>
		private static java.util.LinkedHashSet<java.net.InetAddress> getSubinterfaceInetAddrs
			(java.net.NetworkInterface nif)
		{
			java.util.LinkedHashSet<java.net.InetAddress> addrs = new java.util.LinkedHashSet
				<java.net.InetAddress>();
			java.util.Enumeration<java.net.NetworkInterface> subNifs = nif.getSubInterfaces();
			while (subNifs.MoveNext())
			{
				java.net.NetworkInterface subNif = subNifs.Current;
				Sharpen.Collections.AddAll(addrs, java.util.Collections.list(subNif.getInetAddresses
					()));
			}
			return addrs;
		}

		/// <summary>
		/// Like
		/// <see>
		/// DNS#getIPs(String, boolean), but returns all
		/// IPs associated with the given interface and its subinterfaces.
		/// </see>
		/// </summary>
		/// <exception cref="java.net.UnknownHostException"/>
		public static string[] getIPs(string strInterface)
		{
			return getIPs(strInterface, true);
		}

		/// <summary>
		/// Returns all the IPs associated with the provided interface, if any, in
		/// textual form.
		/// </summary>
		/// <param name="strInterface">
		/// The name of the network interface or sub-interface to query
		/// (eg eth0 or eth0:0) or the string "default"
		/// </param>
		/// <param name="returnSubinterfaces">
		/// Whether to return IPs associated with subinterfaces of
		/// the given interface
		/// </param>
		/// <returns>
		/// A string vector of all the IPs associated with the provided
		/// interface. The local host IP is returned if the interface
		/// name "default" is specified or there is an I/O error looking
		/// for the given interface.
		/// </returns>
		/// <exception cref="java.net.UnknownHostException">If the given interface is invalid
		/// 	</exception>
		public static string[] getIPs(string strInterface, bool returnSubinterfaces)
		{
			if ("default".Equals(strInterface))
			{
				return new string[] { cachedHostAddress };
			}
			java.net.NetworkInterface netIf;
			try
			{
				netIf = java.net.NetworkInterface.getByName(strInterface);
				if (netIf == null)
				{
					netIf = getSubinterface(strInterface);
				}
			}
			catch (System.Net.Sockets.SocketException e)
			{
				LOG.warn("I/O error finding interface " + strInterface + ": " + e.Message);
				return new string[] { cachedHostAddress };
			}
			if (netIf == null)
			{
				throw new java.net.UnknownHostException("No such interface " + strInterface);
			}
			// NB: Using a LinkedHashSet to preserve the order for callers
			// that depend on a particular element being 1st in the array.
			// For example, getDefaultIP always returns the first element.
			java.util.LinkedHashSet<java.net.InetAddress> allAddrs = new java.util.LinkedHashSet
				<java.net.InetAddress>();
			Sharpen.Collections.AddAll(allAddrs, java.util.Collections.list(netIf.getInetAddresses
				()));
			if (!returnSubinterfaces)
			{
				allAddrs.removeAll(getSubinterfaceInetAddrs(netIf));
			}
			string[] ips = new string[allAddrs.Count];
			int i = 0;
			foreach (java.net.InetAddress addr in allAddrs)
			{
				ips[i++] = addr.getHostAddress();
			}
			return ips;
		}

		/// <summary>
		/// Returns the first available IP address associated with the provided
		/// network interface or the local host IP if "default" is given.
		/// </summary>
		/// <param name="strInterface">
		/// The name of the network interface or subinterface to query
		/// (e.g. eth0 or eth0:0) or the string "default"
		/// </param>
		/// <returns>
		/// The IP address in text form, the local host IP is returned
		/// if the interface name "default" is specified
		/// </returns>
		/// <exception cref="java.net.UnknownHostException">If the given interface is invalid
		/// 	</exception>
		public static string getDefaultIP(string strInterface)
		{
			string[] ips = getIPs(strInterface);
			return ips[0];
		}

		/// <summary>
		/// Returns all the host names associated by the provided nameserver with the
		/// address bound to the specified network interface
		/// </summary>
		/// <param name="strInterface">
		/// The name of the network interface or subinterface to query
		/// (e.g. eth0 or eth0:0)
		/// </param>
		/// <param name="nameserver">The DNS host name</param>
		/// <returns>
		/// A string vector of all host names associated with the IPs tied to
		/// the specified interface
		/// </returns>
		/// <exception cref="java.net.UnknownHostException">if the given interface is invalid
		/// 	</exception>
		public static string[] getHosts(string strInterface, string nameserver)
		{
			string[] ips = getIPs(strInterface);
			java.util.Vector<string> hosts = new java.util.Vector<string>();
			for (int ctr = 0; ctr < ips.Length; ctr++)
			{
				try
				{
					hosts.add(reverseDns(java.net.InetAddress.getByName(ips[ctr]), nameserver));
				}
				catch (java.net.UnknownHostException)
				{
				}
				catch (javax.naming.NamingException)
				{
				}
			}
			if (hosts.isEmpty())
			{
				LOG.warn("Unable to determine hostname for interface " + strInterface);
				return new string[] { cachedHostname };
			}
			else
			{
				return Sharpen.Collections.ToArray(hosts, new string[hosts.Count]);
			}
		}

		/// <summary>
		/// Determine the local hostname; retrieving it from cache if it is known
		/// If we cannot determine our host name, return "localhost"
		/// </summary>
		/// <returns>the local hostname or "localhost"</returns>
		private static string resolveLocalHostname()
		{
			string localhost;
			try
			{
				localhost = java.net.InetAddress.getLocalHost().getCanonicalHostName();
			}
			catch (java.net.UnknownHostException e)
			{
				LOG.warn("Unable to determine local hostname " + "-falling back to \"" + LOCALHOST
					 + "\"", e);
				localhost = LOCALHOST;
			}
			return localhost;
		}

		/// <summary>Get the IPAddress of the local host as a string.</summary>
		/// <remarks>
		/// Get the IPAddress of the local host as a string.
		/// This will be a loop back value if the local host address cannot be
		/// determined.
		/// If the loopback address of "localhost" does not resolve, then the system's
		/// network is in such a state that nothing is going to work. A message is
		/// logged at the error level and a null pointer returned, a pointer
		/// which will trigger failures later on the application
		/// </remarks>
		/// <returns>the IPAddress of the local host or null for a serious problem.</returns>
		private static string resolveLocalHostIPAddress()
		{
			string address;
			try
			{
				address = java.net.InetAddress.getLocalHost().getHostAddress();
			}
			catch (java.net.UnknownHostException e)
			{
				LOG.warn("Unable to determine address of the host" + "-falling back to \"" + LOCALHOST
					 + "\" address", e);
				try
				{
					address = java.net.InetAddress.getByName(LOCALHOST).getHostAddress();
				}
				catch (java.net.UnknownHostException)
				{
					//at this point, deep trouble
					LOG.error("Unable to determine local loopback address " + "of \"" + LOCALHOST + "\" "
						 + "-this system's network configuration is unsupported", e);
					address = null;
				}
			}
			return address;
		}

		/// <summary>
		/// Returns all the host names associated by the default nameserver with the
		/// address bound to the specified network interface
		/// </summary>
		/// <param name="strInterface">The name of the network interface to query (e.g. eth0)
		/// 	</param>
		/// <returns>
		/// The list of host names associated with IPs bound to the network
		/// interface
		/// </returns>
		/// <exception cref="java.net.UnknownHostException">If one is encountered while querying the default interface
		/// 	</exception>
		public static string[] getHosts(string strInterface)
		{
			return getHosts(strInterface, null);
		}

		/// <summary>
		/// Returns the default (first) host name associated by the provided
		/// nameserver with the address bound to the specified network interface
		/// </summary>
		/// <param name="strInterface">The name of the network interface to query (e.g. eth0)
		/// 	</param>
		/// <param name="nameserver">The DNS host name</param>
		/// <returns>
		/// The default host names associated with IPs bound to the network
		/// interface
		/// </returns>
		/// <exception cref="java.net.UnknownHostException">If one is encountered while querying the default interface
		/// 	</exception>
		public static string getDefaultHost(string strInterface, string nameserver)
		{
			if ("default".Equals(strInterface))
			{
				return cachedHostname;
			}
			if ("default".Equals(nameserver))
			{
				return getDefaultHost(strInterface);
			}
			string[] hosts = getHosts(strInterface, nameserver);
			return hosts[0];
		}

		/// <summary>
		/// Returns the default (first) host name associated by the default
		/// nameserver with the address bound to the specified network interface
		/// </summary>
		/// <param name="strInterface">
		/// The name of the network interface to query (e.g. eth0).
		/// Must not be null.
		/// </param>
		/// <returns>
		/// The default host name associated with IPs bound to the network
		/// interface
		/// </returns>
		/// <exception cref="java.net.UnknownHostException">If one is encountered while querying the default interface
		/// 	</exception>
		public static string getDefaultHost(string strInterface)
		{
			return getDefaultHost(strInterface, null);
		}
	}
}
