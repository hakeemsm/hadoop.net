using System.Net;
using System.Net.Sockets;
using Javax.Naming;
using Javax.Naming.Directory;
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.Net
{
	/// <summary>
	/// A class that provides direct and reverse lookup functionalities, allowing
	/// the querying of specific network interfaces or nameservers.
	/// </summary>
	public class DNS
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(DNS));

		/// <summary>The cached hostname -initially null.</summary>
		private static readonly string cachedHostname = ResolveLocalHostname();

		private static readonly string cachedHostAddress = ResolveLocalHostIPAddress();

		private const string Localhost = "localhost";

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
		/// <exception cref="Javax.Naming.NamingException">If a NamingException is encountered
		/// 	</exception>
		public static string ReverseDns(IPAddress hostIp, string ns)
		{
			//
			// Builds the reverse IP lookup form
			// This is formed by reversing the IP numbers and appending in-addr.arpa
			//
			string[] parts = hostIp.GetHostAddress().Split("\\.");
			string reverseIP = parts[3] + "." + parts[2] + "." + parts[1] + "." + parts[0] + 
				".in-addr.arpa";
			DirContext ictx = new InitialDirContext();
			Attributes attribute;
			try
			{
				attribute = ictx.GetAttributes("dns://" + ((ns == null) ? string.Empty : ns) + "/"
					 + reverseIP, new string[] { "PTR" });
			}
			finally
			{
				// Use "dns:///" if the default
				// nameserver is to be used
				ictx.Close();
			}
			string hostname = attribute.Get("PTR").Get().ToString();
			int hostnameLength = hostname.Length;
			if (hostname[hostnameLength - 1] == '.')
			{
				hostname = Runtime.Substring(hostname, 0, hostnameLength - 1);
			}
			return hostname;
		}

		/// <returns>
		/// NetworkInterface for the given subinterface name (eg eth0:0)
		/// or null if no interface with the given name can be found
		/// </returns>
		/// <exception cref="System.Net.Sockets.SocketException"/>
		private static NetworkInterface GetSubinterface(string strInterface)
		{
			Enumeration<NetworkInterface> nifs = NetworkInterface.GetNetworkInterfaces();
			while (nifs.MoveNext())
			{
				Enumeration<NetworkInterface> subNifs = nifs.Current.GetSubInterfaces();
				while (subNifs.MoveNext())
				{
					NetworkInterface nif = subNifs.Current;
					if (nif.GetName().Equals(strInterface))
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
		private static LinkedHashSet<IPAddress> GetSubinterfaceInetAddrs(NetworkInterface
			 nif)
		{
			LinkedHashSet<IPAddress> addrs = new LinkedHashSet<IPAddress>();
			Enumeration<NetworkInterface> subNifs = nif.GetSubInterfaces();
			while (subNifs.MoveNext())
			{
				NetworkInterface subNif = subNifs.Current;
				Collections.AddAll(addrs, Collections.List(subNif.GetInetAddresses()));
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
		/// <exception cref="UnknownHostException"/>
		public static string[] GetIPs(string strInterface)
		{
			return GetIPs(strInterface, true);
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
		/// <exception cref="UnknownHostException">If the given interface is invalid</exception>
		public static string[] GetIPs(string strInterface, bool returnSubinterfaces)
		{
			if ("default".Equals(strInterface))
			{
				return new string[] { cachedHostAddress };
			}
			NetworkInterface netIf;
			try
			{
				netIf = NetworkInterface.GetByName(strInterface);
				if (netIf == null)
				{
					netIf = GetSubinterface(strInterface);
				}
			}
			catch (SocketException e)
			{
				Log.Warn("I/O error finding interface " + strInterface + ": " + e.Message);
				return new string[] { cachedHostAddress };
			}
			if (netIf == null)
			{
				throw new UnknownHostException("No such interface " + strInterface);
			}
			// NB: Using a LinkedHashSet to preserve the order for callers
			// that depend on a particular element being 1st in the array.
			// For example, getDefaultIP always returns the first element.
			LinkedHashSet<IPAddress> allAddrs = new LinkedHashSet<IPAddress>();
			Collections.AddAll(allAddrs, Collections.List(netIf.GetInetAddresses()));
			if (!returnSubinterfaces)
			{
				allAddrs.RemoveAll(GetSubinterfaceInetAddrs(netIf));
			}
			string[] ips = new string[allAddrs.Count];
			int i = 0;
			foreach (IPAddress addr in allAddrs)
			{
				ips[i++] = addr.GetHostAddress();
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
		/// <exception cref="UnknownHostException">If the given interface is invalid</exception>
		public static string GetDefaultIP(string strInterface)
		{
			string[] ips = GetIPs(strInterface);
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
		/// <exception cref="UnknownHostException">if the given interface is invalid</exception>
		public static string[] GetHosts(string strInterface, string nameserver)
		{
			string[] ips = GetIPs(strInterface);
			Vector<string> hosts = new Vector<string>();
			for (int ctr = 0; ctr < ips.Length; ctr++)
			{
				try
				{
					hosts.AddItem(ReverseDns(Extensions.GetAddressByName(ips[ctr]), nameserver
						));
				}
				catch (UnknownHostException)
				{
				}
				catch (NamingException)
				{
				}
			}
			if (hosts.IsEmpty())
			{
				Log.Warn("Unable to determine hostname for interface " + strInterface);
				return new string[] { cachedHostname };
			}
			else
			{
				return Collections.ToArray(hosts, new string[hosts.Count]);
			}
		}

		/// <summary>
		/// Determine the local hostname; retrieving it from cache if it is known
		/// If we cannot determine our host name, return "localhost"
		/// </summary>
		/// <returns>the local hostname or "localhost"</returns>
		private static string ResolveLocalHostname()
		{
			string localhost;
			try
			{
				localhost = Runtime.GetLocalHost().ToString();
			}
			catch (UnknownHostException e)
			{
				Log.Warn("Unable to determine local hostname " + "-falling back to \"" + Localhost
					 + "\"", e);
				localhost = Localhost;
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
		private static string ResolveLocalHostIPAddress()
		{
			string address;
			try
			{
				address = Runtime.GetLocalHost().GetHostAddress();
			}
			catch (UnknownHostException e)
			{
				Log.Warn("Unable to determine address of the host" + "-falling back to \"" + Localhost
					 + "\" address", e);
				try
				{
					address = Extensions.GetAddressByName(Localhost).GetHostAddress();
				}
				catch (UnknownHostException)
				{
					//at this point, deep trouble
					Log.Error("Unable to determine local loopback address " + "of \"" + Localhost + "\" "
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
		/// <exception cref="UnknownHostException">If one is encountered while querying the default interface
		/// 	</exception>
		public static string[] GetHosts(string strInterface)
		{
			return GetHosts(strInterface, null);
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
		/// <exception cref="UnknownHostException">If one is encountered while querying the default interface
		/// 	</exception>
		public static string GetDefaultHost(string strInterface, string nameserver)
		{
			if ("default".Equals(strInterface))
			{
				return cachedHostname;
			}
			if ("default".Equals(nameserver))
			{
				return GetDefaultHost(strInterface);
			}
			string[] hosts = GetHosts(strInterface, nameserver);
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
		/// <exception cref="UnknownHostException">If one is encountered while querying the default interface
		/// 	</exception>
		public static string GetDefaultHost(string strInterface)
		{
			return GetDefaultHost(strInterface, null);
		}
	}
}
