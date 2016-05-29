using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class RackResolver
	{
		private static DNSToSwitchMapping dnsToSwitchMapping;

		private static bool initCalled = false;

		private static readonly Log Log = LogFactory.GetLog(typeof(RackResolver));

		public static void Init(Configuration conf)
		{
			lock (typeof(RackResolver))
			{
				if (initCalled)
				{
					return;
				}
				else
				{
					initCalled = true;
				}
				Type dnsToSwitchMappingClass = conf.GetClass<DNSToSwitchMapping>(CommonConfigurationKeysPublic
					.NetTopologyNodeSwitchMappingImplKey, typeof(ScriptBasedMapping));
				try
				{
					DNSToSwitchMapping newInstance = ReflectionUtils.NewInstance(dnsToSwitchMappingClass
						, conf);
					// Wrap around the configured class with the Cached implementation so as
					// to save on repetitive lookups.
					// Check if the impl is already caching, to avoid double caching.
					dnsToSwitchMapping = ((newInstance is CachedDNSToSwitchMapping) ? newInstance : new 
						CachedDNSToSwitchMapping(newInstance));
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			}
		}

		/// <summary>
		/// Utility method for getting a hostname resolved to a node in the
		/// network topology.
		/// </summary>
		/// <remarks>
		/// Utility method for getting a hostname resolved to a node in the
		/// network topology. This method initializes the class with the
		/// right resolver implementation.
		/// </remarks>
		/// <param name="conf"/>
		/// <param name="hostName"/>
		/// <returns>
		/// node
		/// <see cref="Org.Apache.Hadoop.Net.Node"/>
		/// after resolving the hostname
		/// </returns>
		public static Node Resolve(Configuration conf, string hostName)
		{
			Init(conf);
			return CoreResolve(hostName);
		}

		/// <summary>
		/// Utility method for getting a hostname resolved to a node in the
		/// network topology.
		/// </summary>
		/// <remarks>
		/// Utility method for getting a hostname resolved to a node in the
		/// network topology. This method doesn't initialize the class.
		/// Call
		/// <see cref="Init(Org.Apache.Hadoop.Conf.Configuration)"/>
		/// explicitly.
		/// </remarks>
		/// <param name="hostName"/>
		/// <returns>
		/// node
		/// <see cref="Org.Apache.Hadoop.Net.Node"/>
		/// after resolving the hostname
		/// </returns>
		public static Node Resolve(string hostName)
		{
			if (!initCalled)
			{
				throw new InvalidOperationException("RackResolver class not yet initialized");
			}
			return CoreResolve(hostName);
		}

		private static Node CoreResolve(string hostName)
		{
			IList<string> tmpList = new AList<string>(1);
			tmpList.AddItem(hostName);
			IList<string> rNameList = dnsToSwitchMapping.Resolve(tmpList);
			string rName = null;
			if (rNameList == null || rNameList[0] == null)
			{
				rName = NetworkTopology.DefaultRack;
				Log.Info("Couldn't resolve " + hostName + ". Falling back to " + NetworkTopology.
					DefaultRack);
			}
			else
			{
				rName = rNameList[0];
				Log.Info("Resolved " + hostName + " to " + rName);
			}
			return new NodeBase(hostName, rName);
		}

		/// <summary>Only used by tests</summary>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal static DNSToSwitchMapping GetDnsToSwitchMapping()
		{
			return dnsToSwitchMapping;
		}
	}
}
