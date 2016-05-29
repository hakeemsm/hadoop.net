using System.Collections.Generic;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class TestRackResolver
	{
		private static Log Log = LogFactory.GetLog(typeof(TestRackResolver));

		private const string invalidHost = "invalidHost";

		public sealed class MyResolver : DNSToSwitchMapping
		{
			internal int numHost1 = 0;

			public static string resolvedHost1 = "host1";

			public IList<string> Resolve(IList<string> hostList)
			{
				// Only one host at a time
				NUnit.Framework.Assert.IsTrue("hostList size is " + hostList.Count, hostList.Count
					 <= 1);
				IList<string> returnList = new AList<string>();
				if (hostList.IsEmpty())
				{
					return returnList;
				}
				if (hostList[0].Equals(invalidHost))
				{
					// Simulate condition where resolving host returns null
					return null;
				}
				Log.Info("Received resolve request for " + hostList[0]);
				if (hostList[0].Equals("host1") || hostList[0].Equals(resolvedHost1))
				{
					numHost1++;
					returnList.AddItem("/rack1");
				}
				// I should not be reached again as RackResolver is supposed to do
				// caching.
				NUnit.Framework.Assert.IsTrue(numHost1 <= 1);
				return returnList;
			}

			public void ReloadCachedMappings()
			{
			}

			// nothing to do here, since RawScriptBasedMapping has no cache.
			public void ReloadCachedMappings(IList<string> names)
			{
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestCaching()
		{
			Configuration conf = new Configuration();
			conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
				typeof(TestRackResolver.MyResolver), typeof(DNSToSwitchMapping));
			RackResolver.Init(conf);
			try
			{
				IPAddress iaddr = Sharpen.Extensions.GetAddressByName("host1");
				TestRackResolver.MyResolver.resolvedHost1 = iaddr.GetHostAddress();
			}
			catch (UnknownHostException)
			{
			}
			// Ignore if not found
			Node node = RackResolver.Resolve("host1");
			NUnit.Framework.Assert.AreEqual("/rack1", node.GetNetworkLocation());
			node = RackResolver.Resolve("host1");
			NUnit.Framework.Assert.AreEqual("/rack1", node.GetNetworkLocation());
			node = RackResolver.Resolve(invalidHost);
			NUnit.Framework.Assert.AreEqual(NetworkTopology.DefaultRack, node.GetNetworkLocation
				());
		}
	}
}
