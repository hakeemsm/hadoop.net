using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>
	/// Container class which holds a list of ip/host addresses and
	/// answers membership queries.
	/// </summary>
	/// <remarks>
	/// Container class which holds a list of ip/host addresses and
	/// answers membership queries.
	/// Accepts list of ip addresses, ip addreses in CIDR format and/or
	/// host addresses.
	/// </remarks>
	public class MachineList
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.MachineList
			)));

		public const string WILDCARD_VALUE = "*";

		/// <summary>InetAddressFactory is used to obtain InetAddress from host.</summary>
		/// <remarks>
		/// InetAddressFactory is used to obtain InetAddress from host.
		/// This class makes it easy to simulate host to ip mappings during testing.
		/// </remarks>
		public class InetAddressFactory
		{
			internal static readonly org.apache.hadoop.util.MachineList.InetAddressFactory S_INSTANCE
				 = new org.apache.hadoop.util.MachineList.InetAddressFactory();

			/// <exception cref="java.net.UnknownHostException"/>
			public virtual java.net.InetAddress getByName(string host)
			{
				return java.net.InetAddress.getByName(host);
			}
		}

		private readonly bool all;

		private readonly System.Collections.Generic.ICollection<string> ipAddresses;

		private readonly System.Collections.Generic.IList<org.apache.commons.net.util.SubnetUtils.SubnetInfo
			> cidrAddresses;

		private readonly System.Collections.Generic.ICollection<string> hostNames;

		private readonly org.apache.hadoop.util.MachineList.InetAddressFactory addressFactory;

		/// <param name="hostEntries">comma separated ip/cidr/host addresses</param>
		public MachineList(string hostEntries)
			: this(org.apache.hadoop.util.StringUtils.getTrimmedStringCollection(hostEntries)
				)
		{
		}

		/// <param name="hostEntries">collection of separated ip/cidr/host addresses</param>
		public MachineList(System.Collections.Generic.ICollection<string> hostEntries)
			: this(hostEntries, org.apache.hadoop.util.MachineList.InetAddressFactory.S_INSTANCE
				)
		{
		}

		/// <summary>Accepts a collection of ip/cidr/host addresses</summary>
		/// <param name="hostEntries"/>
		/// <param name="addressFactory">addressFactory to convert host to InetAddress</param>
		public MachineList(System.Collections.Generic.ICollection<string> hostEntries, org.apache.hadoop.util.MachineList.InetAddressFactory
			 addressFactory)
		{
			this.addressFactory = addressFactory;
			if (hostEntries != null)
			{
				if ((hostEntries.Count == 1) && (hostEntries.contains(WILDCARD_VALUE)))
				{
					all = true;
					ipAddresses = null;
					hostNames = null;
					cidrAddresses = null;
				}
				else
				{
					all = false;
					System.Collections.Generic.ICollection<string> ips = new java.util.HashSet<string
						>();
					System.Collections.Generic.IList<org.apache.commons.net.util.SubnetUtils.SubnetInfo
						> cidrs = new System.Collections.Generic.LinkedList<org.apache.commons.net.util.SubnetUtils.SubnetInfo
						>();
					System.Collections.Generic.ICollection<string> hosts = new java.util.HashSet<string
						>();
					foreach (string hostEntry in hostEntries)
					{
						//ip address range
						if (hostEntry.IndexOf("/") > -1)
						{
							try
							{
								org.apache.commons.net.util.SubnetUtils subnet = new org.apache.commons.net.util.SubnetUtils
									(hostEntry);
								subnet.setInclusiveHostCount(true);
								cidrs.add(subnet.getInfo());
							}
							catch (System.ArgumentException e)
							{
								LOG.warn("Invalid CIDR syntax : " + hostEntry);
								throw;
							}
						}
						else
						{
							if (com.google.common.net.InetAddresses.isInetAddress(hostEntry))
							{
								//ip address
								ips.add(hostEntry);
							}
							else
							{
								//hostname
								hosts.add(hostEntry);
							}
						}
					}
					ipAddresses = (ips.Count > 0) ? ips : null;
					cidrAddresses = (cidrs.Count > 0) ? cidrs : null;
					hostNames = (hosts.Count > 0) ? hosts : null;
				}
			}
			else
			{
				all = false;
				ipAddresses = null;
				hostNames = null;
				cidrAddresses = null;
			}
		}

		/// <summary>Accepts an ip address and return true if ipAddress is in the list</summary>
		/// <param name="ipAddress"/>
		/// <returns>true if ipAddress is part of the list</returns>
		public virtual bool includes(string ipAddress)
		{
			if (all)
			{
				return true;
			}
			//check in the set of ipAddresses
			if ((ipAddresses != null) && ipAddresses.contains(ipAddress))
			{
				return true;
			}
			//iterate through the ip ranges for inclusion
			if (cidrAddresses != null)
			{
				foreach (org.apache.commons.net.util.SubnetUtils.SubnetInfo cidrAddress in cidrAddresses)
				{
					if (cidrAddress.isInRange(ipAddress))
					{
						return true;
					}
				}
			}
			//check if the ipAddress matches one of hostnames
			if (hostNames != null)
			{
				//convert given ipAddress to hostname and look for a match
				java.net.InetAddress hostAddr;
				try
				{
					hostAddr = addressFactory.getByName(ipAddress);
					if ((hostAddr != null) && hostNames.contains(hostAddr.getCanonicalHostName()))
					{
						return true;
					}
				}
				catch (java.net.UnknownHostException)
				{
				}
				//ignore the exception and proceed to resolve the list of hosts
				//loop through host addresses and convert them to ip and look for a match
				foreach (string host in hostNames)
				{
					try
					{
						hostAddr = addressFactory.getByName(host);
					}
					catch (java.net.UnknownHostException)
					{
						continue;
					}
					if (hostAddr.getHostAddress().Equals(ipAddress))
					{
						return true;
					}
				}
			}
			return false;
		}

		/// <summary>
		/// returns the contents of the MachineList as a Collection<String>
		/// This can be used for testing
		/// </summary>
		/// <returns>contents of the MachineList</returns>
		[com.google.common.annotations.VisibleForTesting]
		public virtual System.Collections.Generic.ICollection<string> getCollection()
		{
			System.Collections.Generic.ICollection<string> list = new System.Collections.Generic.List
				<string>();
			if (all)
			{
				list.add("*");
			}
			else
			{
				if (ipAddresses != null)
				{
					Sharpen.Collections.AddAll(list, ipAddresses);
				}
				if (hostNames != null)
				{
					Sharpen.Collections.AddAll(list, hostNames);
				}
				if (cidrAddresses != null)
				{
					foreach (org.apache.commons.net.util.SubnetUtils.SubnetInfo cidrAddress in cidrAddresses)
					{
						list.add(cidrAddress.getCidrSignature());
					}
				}
			}
			return list;
		}
	}
}
