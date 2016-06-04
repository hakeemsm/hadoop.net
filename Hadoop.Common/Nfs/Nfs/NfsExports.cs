using System;
using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Base;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Net.Util;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs
{
	/// <summary>
	/// This class provides functionality for loading and checking the mapping
	/// between client hosts and their access privileges.
	/// </summary>
	public class NfsExports
	{
		private static Org.Apache.Hadoop.Nfs.NfsExports exports = null;

		public static Org.Apache.Hadoop.Nfs.NfsExports GetInstance(Configuration conf)
		{
			lock (typeof(NfsExports))
			{
				if (exports == null)
				{
					string matchHosts = conf.Get(CommonConfigurationKeys.NfsExportsAllowedHostsKey, CommonConfigurationKeys
						.NfsExportsAllowedHostsKeyDefault);
					int cacheSize = conf.GetInt(Nfs3Constant.NfsExportsCacheSizeKey, Nfs3Constant.NfsExportsCacheSizeDefault
						);
					long expirationPeriodNano = conf.GetLong(Nfs3Constant.NfsExportsCacheExpirytimeMillisKey
						, Nfs3Constant.NfsExportsCacheExpirytimeMillisDefault) * 1000 * 1000;
					try
					{
						exports = new Org.Apache.Hadoop.Nfs.NfsExports(cacheSize, expirationPeriodNano, matchHosts
							);
					}
					catch (ArgumentException e)
					{
						Log.Error("Invalid NFS Exports provided: ", e);
						return exports;
					}
				}
				return exports;
			}
		}

		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Nfs.NfsExports
			));

		private const string IpAddress = "(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})";

		private const string SlashFormatShort = IpAddress + "/(\\d{1,3})";

		private const string SlashFormatLong = IpAddress + "/" + IpAddress;

		private static readonly Sharpen.Pattern CidrFormatShort = Sharpen.Pattern.Compile
			(SlashFormatShort);

		private static readonly Sharpen.Pattern CidrFormatLong = Sharpen.Pattern.Compile(
			SlashFormatLong);

		private const string LabelFormat = "[a-zA-Z0-9]([a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])?";

		private static readonly Sharpen.Pattern HostnameFormat = Sharpen.Pattern.Compile(
			"^(" + LabelFormat + "\\.)*" + LabelFormat + "$");

		internal class AccessCacheEntry : LightWeightCache.Entry
		{
			private readonly string hostAddr;

			private AccessPrivilege access;

			private readonly long expirationTime;

			private LightWeightGSet.LinkedElement next;

			internal AccessCacheEntry(string hostAddr, AccessPrivilege access, long expirationTime
				)
			{
				// only support IPv4 now
				// Hostnames are composed of series of 'labels' concatenated with dots.
				// Labels can be between 1-63 characters long, and can only take
				// letters, digits & hyphens. They cannot start and end with hyphens. For
				// more details, refer RFC-1123 & http://en.wikipedia.org/wiki/Hostname
				Preconditions.CheckArgument(hostAddr != null);
				this.hostAddr = hostAddr;
				this.access = access;
				this.expirationTime = expirationTime;
			}

			public override int GetHashCode()
			{
				return hostAddr.GetHashCode();
			}

			public override bool Equals(object obj)
			{
				if (this == obj)
				{
					return true;
				}
				if (obj is NfsExports.AccessCacheEntry)
				{
					NfsExports.AccessCacheEntry entry = (NfsExports.AccessCacheEntry)obj;
					return this.hostAddr.Equals(entry.hostAddr);
				}
				return false;
			}

			public virtual void SetNext(LightWeightGSet.LinkedElement next)
			{
				this.next = next;
			}

			public virtual LightWeightGSet.LinkedElement GetNext()
			{
				return this.next;
			}

			public virtual void SetExpirationTime(long timeNano)
			{
			}

			// we set expiration time in the constructor, and the expiration time 
			// does not change
			public virtual long GetExpirationTime()
			{
				return this.expirationTime;
			}
		}

		private readonly IList<NfsExports.Match> mMatches;

		private readonly LightWeightCache<NfsExports.AccessCacheEntry, NfsExports.AccessCacheEntry
			> accessCache;

		private readonly long cacheExpirationPeriod;

		/// <summary>Constructor.</summary>
		/// <param name="cacheSize">The size of the access privilege cache.</param>
		/// <param name="expirationPeriodNano">The period</param>
		/// <param name="matchingHosts">A string specifying one or multiple matchers.</param>
		internal NfsExports(int cacheSize, long expirationPeriodNano, string matchHosts)
		{
			this.cacheExpirationPeriod = expirationPeriodNano;
			accessCache = new LightWeightCache<NfsExports.AccessCacheEntry, NfsExports.AccessCacheEntry
				>(cacheSize, cacheSize, expirationPeriodNano, 0);
			string[] matchStrings = matchHosts.Split(CommonConfigurationKeys.NfsExportsAllowedHostsSeparator
				);
			mMatches = new AList<NfsExports.Match>(matchStrings.Length);
			foreach (string mStr in matchStrings)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Processing match string '" + mStr + "'");
				}
				mStr = mStr.Trim();
				if (!mStr.IsEmpty())
				{
					mMatches.AddItem(GetMatch(mStr));
				}
			}
		}

		/// <summary>Return the configured group list</summary>
		public virtual string[] GetHostGroupList()
		{
			int listSize = mMatches.Count;
			string[] hostGroups = new string[listSize];
			for (int i = 0; i < mMatches.Count; i++)
			{
				hostGroups[i] = mMatches[i].GetHostGroup();
			}
			return hostGroups;
		}

		public virtual AccessPrivilege GetAccessPrivilege(IPAddress addr)
		{
			return GetAccessPrivilege(addr.GetHostAddress(), addr.ToString());
		}

		internal virtual AccessPrivilege GetAccessPrivilege(string address, string hostname
			)
		{
			long now = Runtime.NanoTime();
			NfsExports.AccessCacheEntry newEntry = new NfsExports.AccessCacheEntry(address, AccessPrivilege
				.None, now + this.cacheExpirationPeriod);
			// check if there is a cache entry for the given address
			NfsExports.AccessCacheEntry cachedEntry = accessCache.Get(newEntry);
			if (cachedEntry != null && now < cachedEntry.expirationTime)
			{
				// get a non-expired cache entry, use it
				return cachedEntry.access;
			}
			else
			{
				foreach (NfsExports.Match match in mMatches)
				{
					if (match.IsIncluded(address, hostname))
					{
						if (match.accessPrivilege == AccessPrivilege.ReadOnly)
						{
							newEntry.access = AccessPrivilege.ReadOnly;
							break;
						}
						else
						{
							if (match.accessPrivilege == AccessPrivilege.ReadWrite)
							{
								newEntry.access = AccessPrivilege.ReadWrite;
							}
						}
					}
				}
				accessCache.Put(newEntry);
				return newEntry.access;
			}
		}

		private abstract class Match
		{
			private readonly AccessPrivilege accessPrivilege;

			private Match(AccessPrivilege accessPrivilege)
			{
				this.accessPrivilege = accessPrivilege;
			}

			public abstract bool IsIncluded(string address, string hostname);

			public abstract string GetHostGroup();
		}

		/// <summary>Matcher covering all client hosts (specified by "*")</summary>
		private class AnonymousMatch : NfsExports.Match
		{
			private AnonymousMatch(AccessPrivilege accessPrivilege)
				: base(accessPrivilege)
			{
			}

			public override bool IsIncluded(string address, string hostname)
			{
				return true;
			}

			public override string GetHostGroup()
			{
				return "*";
			}
		}

		/// <summary>Matcher using CIDR for client host matching</summary>
		private class CIDRMatch : NfsExports.Match
		{
			private readonly SubnetUtils.SubnetInfo subnetInfo;

			private CIDRMatch(AccessPrivilege accessPrivilege, SubnetUtils.SubnetInfo subnetInfo
				)
				: base(accessPrivilege)
			{
				this.subnetInfo = subnetInfo;
			}

			public override bool IsIncluded(string address, string hostname)
			{
				if (subnetInfo.IsInRange(address))
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("CIDRNMatcher low = " + subnetInfo.GetLowAddress() + ", high = " + subnetInfo
							.GetHighAddress() + ", allowing client '" + address + "', '" + hostname + "'");
					}
					return true;
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("CIDRNMatcher low = " + subnetInfo.GetLowAddress() + ", high = " + subnetInfo
						.GetHighAddress() + ", denying client '" + address + "', '" + hostname + "'");
				}
				return false;
			}

			public override string GetHostGroup()
			{
				return subnetInfo.GetAddress() + "/" + subnetInfo.GetNetmask();
			}
		}

		/// <summary>Matcher requiring exact string match for client host</summary>
		private class ExactMatch : NfsExports.Match
		{
			private readonly string ipOrHost;

			private ExactMatch(AccessPrivilege accessPrivilege, string ipOrHost)
				: base(accessPrivilege)
			{
				this.ipOrHost = ipOrHost;
			}

			public override bool IsIncluded(string address, string hostname)
			{
				if (Sharpen.Runtime.EqualsIgnoreCase(ipOrHost, address) || Sharpen.Runtime.EqualsIgnoreCase
					(ipOrHost, hostname))
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("ExactMatcher '" + ipOrHost + "', allowing client " + "'" + address + "', '"
							 + hostname + "'");
					}
					return true;
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("ExactMatcher '" + ipOrHost + "', denying client " + "'" + address + "', '"
						 + hostname + "'");
				}
				return false;
			}

			public override string GetHostGroup()
			{
				return ipOrHost;
			}
		}

		/// <summary>Matcher where client hosts are specified by regular expression</summary>
		private class RegexMatch : NfsExports.Match
		{
			private readonly Sharpen.Pattern pattern;

			private RegexMatch(AccessPrivilege accessPrivilege, string wildcard)
				: base(accessPrivilege)
			{
				this.pattern = Sharpen.Pattern.Compile(wildcard, Sharpen.Pattern.CaseInsensitive);
			}

			public override bool IsIncluded(string address, string hostname)
			{
				if (pattern.Matcher(address).Matches() || pattern.Matcher(hostname).Matches())
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("RegexMatcher '" + pattern.Pattern() + "', allowing client '" + address
							 + "', '" + hostname + "'");
					}
					return true;
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("RegexMatcher '" + pattern.Pattern() + "', denying client '" + address 
						+ "', '" + hostname + "'");
				}
				return false;
			}

			public override string GetHostGroup()
			{
				return pattern.ToString();
			}
		}

		/// <summary>Loading a matcher from a string.</summary>
		/// <remarks>
		/// Loading a matcher from a string. The default access privilege is read-only.
		/// The string contains 1 or 2 parts, separated by whitespace characters, where
		/// the first part specifies the client hosts, and the second part (if
		/// existent) specifies the access privilege of the client hosts. I.e.,
		/// "client-hosts [access-privilege]"
		/// </remarks>
		private static NfsExports.Match GetMatch(string line)
		{
			string[] parts = line.Split("\\s+");
			string host;
			AccessPrivilege privilege = AccessPrivilege.ReadOnly;
			switch (parts.Length)
			{
				case 1:
				{
					host = StringUtils.ToLowerCase(parts[0]).Trim();
					break;
				}

				case 2:
				{
					host = StringUtils.ToLowerCase(parts[0]).Trim();
					string option = parts[1].Trim();
					if (Sharpen.Runtime.EqualsIgnoreCase("rw", option))
					{
						privilege = AccessPrivilege.ReadWrite;
					}
					break;
				}

				default:
				{
					throw new ArgumentException("Incorrectly formatted line '" + line + "'");
				}
			}
			if (host.Equals("*"))
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Using match all for '" + host + "' and " + privilege);
				}
				return new NfsExports.AnonymousMatch(privilege);
			}
			else
			{
				if (CidrFormatShort.Matcher(host).Matches())
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Using CIDR match for '" + host + "' and " + privilege);
					}
					return new NfsExports.CIDRMatch(privilege, new SubnetUtils(host).GetInfo());
				}
				else
				{
					if (CidrFormatLong.Matcher(host).Matches())
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Using CIDR match for '" + host + "' and " + privilege);
						}
						string[] pair = host.Split("/");
						return new NfsExports.CIDRMatch(privilege, new SubnetUtils(pair[0], pair[1]).GetInfo
							());
					}
					else
					{
						if (host.Contains("*") || host.Contains("?") || host.Contains("[") || host.Contains
							("]") || host.Contains("(") || host.Contains(")"))
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("Using Regex match for '" + host + "' and " + privilege);
							}
							return new NfsExports.RegexMatch(privilege, host);
						}
						else
						{
							if (HostnameFormat.Matcher(host).Matches())
							{
								if (Log.IsDebugEnabled())
								{
									Log.Debug("Using exact match for '" + host + "' and " + privilege);
								}
								return new NfsExports.ExactMatch(privilege, host);
							}
							else
							{
								throw new ArgumentException("Invalid hostname provided '" + host + "'");
							}
						}
					}
				}
			}
		}
	}
}
