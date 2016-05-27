using System;
using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Net.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Util
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
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Util.MachineList
			));

		public const string WildcardValue = "*";

		/// <summary>InetAddressFactory is used to obtain InetAddress from host.</summary>
		/// <remarks>
		/// InetAddressFactory is used to obtain InetAddress from host.
		/// This class makes it easy to simulate host to ip mappings during testing.
		/// </remarks>
		public class InetAddressFactory
		{
			internal static readonly MachineList.InetAddressFactory SInstance = new MachineList.InetAddressFactory
				();

			/// <exception cref="Sharpen.UnknownHostException"/>
			public virtual IPAddress GetByName(string host)
			{
				return Sharpen.Extensions.GetAddressByName(host);
			}
		}

		private readonly bool all;

		private readonly ICollection<string> ipAddresses;

		private readonly IList<SubnetUtils.SubnetInfo> cidrAddresses;

		private readonly ICollection<string> hostNames;

		private readonly MachineList.InetAddressFactory addressFactory;

		/// <param name="hostEntries">comma separated ip/cidr/host addresses</param>
		public MachineList(string hostEntries)
			: this(StringUtils.GetTrimmedStringCollection(hostEntries))
		{
		}

		/// <param name="hostEntries">collection of separated ip/cidr/host addresses</param>
		public MachineList(ICollection<string> hostEntries)
			: this(hostEntries, MachineList.InetAddressFactory.SInstance)
		{
		}

		/// <summary>Accepts a collection of ip/cidr/host addresses</summary>
		/// <param name="hostEntries"/>
		/// <param name="addressFactory">addressFactory to convert host to InetAddress</param>
		public MachineList(ICollection<string> hostEntries, MachineList.InetAddressFactory
			 addressFactory)
		{
			this.addressFactory = addressFactory;
			if (hostEntries != null)
			{
				if ((hostEntries.Count == 1) && (hostEntries.Contains(WildcardValue)))
				{
					all = true;
					ipAddresses = null;
					hostNames = null;
					cidrAddresses = null;
				}
				else
				{
					all = false;
					ICollection<string> ips = new HashSet<string>();
					IList<SubnetUtils.SubnetInfo> cidrs = new List<SubnetUtils.SubnetInfo>();
					ICollection<string> hosts = new HashSet<string>();
					foreach (string hostEntry in hostEntries)
					{
						//ip address range
						if (hostEntry.IndexOf("/") > -1)
						{
							try
							{
								SubnetUtils subnet = new SubnetUtils(hostEntry);
								subnet.SetInclusiveHostCount(true);
								cidrs.AddItem(subnet.GetInfo());
							}
							catch (ArgumentException e)
							{
								Log.Warn("Invalid CIDR syntax : " + hostEntry);
								throw;
							}
						}
						else
						{
							if (InetAddresses.IsInetAddress(hostEntry))
							{
								//ip address
								ips.AddItem(hostEntry);
							}
							else
							{
								//hostname
								hosts.AddItem(hostEntry);
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
		public virtual bool Includes(string ipAddress)
		{
			if (all)
			{
				return true;
			}
			//check in the set of ipAddresses
			if ((ipAddresses != null) && ipAddresses.Contains(ipAddress))
			{
				return true;
			}
			//iterate through the ip ranges for inclusion
			if (cidrAddresses != null)
			{
				foreach (SubnetUtils.SubnetInfo cidrAddress in cidrAddresses)
				{
					if (cidrAddress.IsInRange(ipAddress))
					{
						return true;
					}
				}
			}
			//check if the ipAddress matches one of hostnames
			if (hostNames != null)
			{
				//convert given ipAddress to hostname and look for a match
				IPAddress hostAddr;
				try
				{
					hostAddr = addressFactory.GetByName(ipAddress);
					if ((hostAddr != null) && hostNames.Contains(hostAddr.ToString()))
					{
						return true;
					}
				}
				catch (UnknownHostException)
				{
				}
				//ignore the exception and proceed to resolve the list of hosts
				//loop through host addresses and convert them to ip and look for a match
				foreach (string host in hostNames)
				{
					try
					{
						hostAddr = addressFactory.GetByName(host);
					}
					catch (UnknownHostException)
					{
						continue;
					}
					if (hostAddr.GetHostAddress().Equals(ipAddress))
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
		[VisibleForTesting]
		public virtual ICollection<string> GetCollection()
		{
			ICollection<string> list = new AList<string>();
			if (all)
			{
				list.AddItem("*");
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
					foreach (SubnetUtils.SubnetInfo cidrAddress in cidrAddresses)
					{
						list.AddItem(cidrAddress.GetCidrSignature());
					}
				}
			}
			return list;
		}
	}
}
