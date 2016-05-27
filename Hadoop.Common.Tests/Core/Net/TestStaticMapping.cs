using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	/// <summary>Test the static mapping class.</summary>
	/// <remarks>
	/// Test the static mapping class.
	/// Because the map is actually static, this map needs to be reset for every test
	/// </remarks>
	public class TestStaticMapping : Assert
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestStaticMapping));

		/// <summary>
		/// Reset the map then create a new instance of the
		/// <see cref="StaticMapping"/>
		/// class with a null configuration
		/// </summary>
		/// <returns>a new instance</returns>
		private StaticMapping NewInstance()
		{
			StaticMapping.ResetMap();
			return new StaticMapping();
		}

		/// <summary>
		/// Reset the map then create a new instance of the
		/// <see cref="StaticMapping"/>
		/// class with the topology script in the configuration set to
		/// the parameter
		/// </summary>
		/// <param name="script">a (never executed) script, can be null</param>
		/// <returns>a new instance</returns>
		private StaticMapping NewInstance(string script)
		{
			StaticMapping mapping = NewInstance();
			mapping.SetConf(CreateConf(script));
			return mapping;
		}

		/// <summary>Create a configuration with a specific topology script</summary>
		/// <param name="script">a (never executed) script, can be null</param>
		/// <returns>a configuration</returns>
		private Configuration CreateConf(string script)
		{
			Configuration conf = new Configuration();
			if (script != null)
			{
				conf.Set(CommonConfigurationKeys.NetTopologyScriptFileNameKey, script);
			}
			else
			{
				conf.Unset(CommonConfigurationKeys.NetTopologyScriptFileNameKey);
			}
			return conf;
		}

		private void AssertSingleSwitch(DNSToSwitchMapping mapping)
		{
			NUnit.Framework.Assert.AreEqual("Expected a single switch mapping " + mapping, true
				, AbstractDNSToSwitchMapping.IsMappingSingleSwitch(mapping));
		}

		private void AssertMultiSwitch(DNSToSwitchMapping mapping)
		{
			NUnit.Framework.Assert.AreEqual("Expected a multi switch mapping " + mapping, false
				, AbstractDNSToSwitchMapping.IsMappingSingleSwitch(mapping));
		}

		protected internal virtual void AssertMapSize(AbstractDNSToSwitchMapping switchMapping
			, int expectedSize)
		{
			NUnit.Framework.Assert.AreEqual("Expected two entries in the map " + switchMapping
				.DumpTopology(), expectedSize, switchMapping.GetSwitchMap().Count);
		}

		private IList<string> CreateQueryList()
		{
			IList<string> l1 = new AList<string>(2);
			l1.AddItem("n1");
			l1.AddItem("unknown");
			return l1;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStaticIsSingleSwitchOnNullScript()
		{
			StaticMapping mapping = NewInstance(null);
			mapping.SetConf(CreateConf(null));
			AssertSingleSwitch(mapping);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStaticIsMultiSwitchOnScript()
		{
			StaticMapping mapping = NewInstance("ls");
			AssertMultiSwitch(mapping);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddResolveNodes()
		{
			StaticMapping mapping = NewInstance();
			StaticMapping.AddNodeToRack("n1", "/r1");
			IList<string> queryList = CreateQueryList();
			IList<string> resolved = mapping.Resolve(queryList);
			NUnit.Framework.Assert.AreEqual(2, resolved.Count);
			NUnit.Framework.Assert.AreEqual("/r1", resolved[0]);
			NUnit.Framework.Assert.AreEqual(NetworkTopology.DefaultRack, resolved[1]);
			// get the switch map and examine it
			IDictionary<string, string> switchMap = mapping.GetSwitchMap();
			string topology = mapping.DumpTopology();
			Log.Info(topology);
			NUnit.Framework.Assert.AreEqual(topology, 1, switchMap.Count);
			NUnit.Framework.Assert.AreEqual(topology, "/r1", switchMap["n1"]);
		}

		/// <summary>Verify that a configuration string builds a topology</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReadNodesFromConfig()
		{
			StaticMapping mapping = NewInstance();
			Configuration conf = new Configuration();
			conf.Set(StaticMapping.KeyHadoopConfiguredNodeMapping, "n1=/r1,n2=/r2");
			mapping.SetConf(conf);
			//even though we have inserted elements into the list, because 
			//it is driven by the script key in the configuration, it still
			//thinks that it is single rack
			AssertSingleSwitch(mapping);
			IList<string> l1 = new AList<string>(3);
			l1.AddItem("n1");
			l1.AddItem("unknown");
			l1.AddItem("n2");
			IList<string> resolved = mapping.Resolve(l1);
			NUnit.Framework.Assert.AreEqual(3, resolved.Count);
			NUnit.Framework.Assert.AreEqual("/r1", resolved[0]);
			NUnit.Framework.Assert.AreEqual(NetworkTopology.DefaultRack, resolved[1]);
			NUnit.Framework.Assert.AreEqual("/r2", resolved[2]);
			IDictionary<string, string> switchMap = mapping.GetSwitchMap();
			string topology = mapping.DumpTopology();
			Log.Info(topology);
			NUnit.Framework.Assert.AreEqual(topology, 2, switchMap.Count);
			NUnit.Framework.Assert.AreEqual(topology, "/r1", switchMap["n1"]);
			NUnit.Framework.Assert.IsNull(topology, switchMap["unknown"]);
		}

		/// <summary>Verify that if the inner mapping is single-switch, so is the cached one</summary>
		/// <exception cref="System.Exception">on any problem</exception>
		[NUnit.Framework.Test]
		public virtual void TestCachingRelaysSingleSwitchQueries()
		{
			//create a single switch map
			StaticMapping staticMapping = NewInstance(null);
			AssertSingleSwitch(staticMapping);
			CachedDNSToSwitchMapping cachedMap = new CachedDNSToSwitchMapping(staticMapping);
			Log.Info("Mapping: " + cachedMap + "\n" + cachedMap.DumpTopology());
			AssertSingleSwitch(cachedMap);
		}

		/// <summary>Verify that if the inner mapping is multi-switch, so is the cached one</summary>
		/// <exception cref="System.Exception">on any problem</exception>
		[NUnit.Framework.Test]
		public virtual void TestCachingRelaysMultiSwitchQueries()
		{
			StaticMapping staticMapping = NewInstance("top");
			AssertMultiSwitch(staticMapping);
			CachedDNSToSwitchMapping cachedMap = new CachedDNSToSwitchMapping(staticMapping);
			Log.Info("Mapping: " + cachedMap + "\n" + cachedMap.DumpTopology());
			AssertMultiSwitch(cachedMap);
		}

		/// <summary>This test verifies that resultion queries get relayed to the inner rack</summary>
		/// <exception cref="System.Exception">on any problem</exception>
		[NUnit.Framework.Test]
		public virtual void TestCachingRelaysResolveQueries()
		{
			StaticMapping mapping = NewInstance();
			mapping.SetConf(CreateConf("top"));
			StaticMapping staticMapping = mapping;
			CachedDNSToSwitchMapping cachedMap = new CachedDNSToSwitchMapping(staticMapping);
			AssertMapSize(cachedMap, 0);
			//add a node to the static map
			StaticMapping.AddNodeToRack("n1", "/r1");
			//verify it is there
			AssertMapSize(staticMapping, 1);
			//verify that the cache hasn't picked it up yet
			AssertMapSize(cachedMap, 0);
			//now relay the query
			cachedMap.Resolve(CreateQueryList());
			//and verify the cache is no longer empty
			AssertMapSize(cachedMap, 2);
		}

		/// <summary>This test verifies that resultion queries get relayed to the inner rack</summary>
		/// <exception cref="System.Exception">on any problem</exception>
		[NUnit.Framework.Test]
		public virtual void TestCachingCachesNegativeEntries()
		{
			StaticMapping staticMapping = NewInstance();
			CachedDNSToSwitchMapping cachedMap = new CachedDNSToSwitchMapping(staticMapping);
			AssertMapSize(cachedMap, 0);
			AssertMapSize(staticMapping, 0);
			IList<string> resolved = cachedMap.Resolve(CreateQueryList());
			//and verify the cache is no longer empty while the static map is
			AssertMapSize(staticMapping, 0);
			AssertMapSize(cachedMap, 2);
		}
	}
}
