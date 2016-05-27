using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	/// <summary>Test some other details of the switch mapping</summary>
	public class TestSwitchMapping : Assert
	{
		/// <summary>
		/// Verify the switch mapping query handles arbitrary DNSToSwitchMapping
		/// implementations
		/// </summary>
		/// <exception cref="System.Exception">on any problem</exception>
		[NUnit.Framework.Test]
		public virtual void TestStandaloneClassesAssumedMultiswitch()
		{
			DNSToSwitchMapping mapping = new TestSwitchMapping.StandaloneSwitchMapping();
			NUnit.Framework.Assert.IsFalse("Expected to be multi switch " + mapping, AbstractDNSToSwitchMapping
				.IsMappingSingleSwitch(mapping));
		}

		/// <summary>
		/// Verify the cached mapper delegates the switch mapping query to the inner
		/// mapping, which again handles arbitrary DNSToSwitchMapping implementations
		/// </summary>
		/// <exception cref="System.Exception">on any problem</exception>
		[NUnit.Framework.Test]
		public virtual void TestCachingRelays()
		{
			CachedDNSToSwitchMapping mapping = new CachedDNSToSwitchMapping(new TestSwitchMapping.StandaloneSwitchMapping
				());
			NUnit.Framework.Assert.IsFalse("Expected to be multi switch " + mapping, mapping.
				IsSingleSwitch());
		}

		/// <summary>
		/// Verify the cached mapper delegates the switch mapping query to the inner
		/// mapping, which again handles arbitrary DNSToSwitchMapping implementations
		/// </summary>
		/// <exception cref="System.Exception">on any problem</exception>
		[NUnit.Framework.Test]
		public virtual void TestCachingRelaysStringOperations()
		{
			Configuration conf = new Configuration();
			string scriptname = "mappingscript.sh";
			conf.Set(CommonConfigurationKeys.NetTopologyScriptFileNameKey, scriptname);
			ScriptBasedMapping scriptMapping = new ScriptBasedMapping(conf);
			NUnit.Framework.Assert.IsTrue("Did not find " + scriptname + " in " + scriptMapping
				, scriptMapping.ToString().Contains(scriptname));
			CachedDNSToSwitchMapping mapping = new CachedDNSToSwitchMapping(scriptMapping);
			NUnit.Framework.Assert.IsTrue("Did not find " + scriptname + " in " + mapping, mapping
				.ToString().Contains(scriptname));
		}

		/// <summary>
		/// Verify the cached mapper delegates the switch mapping query to the inner
		/// mapping, which again handles arbitrary DNSToSwitchMapping implementations
		/// </summary>
		/// <exception cref="System.Exception">on any problem</exception>
		[NUnit.Framework.Test]
		public virtual void TestCachingRelaysStringOperationsToNullScript()
		{
			Configuration conf = new Configuration();
			ScriptBasedMapping scriptMapping = new ScriptBasedMapping(conf);
			NUnit.Framework.Assert.IsTrue("Did not find " + ScriptBasedMapping.NoScript + " in "
				 + scriptMapping, scriptMapping.ToString().Contains(ScriptBasedMapping.NoScript)
				);
			CachedDNSToSwitchMapping mapping = new CachedDNSToSwitchMapping(scriptMapping);
			NUnit.Framework.Assert.IsTrue("Did not find " + ScriptBasedMapping.NoScript + " in "
				 + mapping, mapping.ToString().Contains(ScriptBasedMapping.NoScript));
		}

		[NUnit.Framework.Test]
		public virtual void TestNullMapping()
		{
			NUnit.Framework.Assert.IsFalse(AbstractDNSToSwitchMapping.IsMappingSingleSwitch(null
				));
		}

		/// <summary>
		/// This class does not extend the abstract switch mapping, and verifies that
		/// the switch mapping logic assumes that this is multi switch
		/// </summary>
		private class StandaloneSwitchMapping : DNSToSwitchMapping
		{
			public virtual IList<string> Resolve(IList<string> names)
			{
				return names;
			}

			public virtual void ReloadCachedMappings()
			{
			}

			public virtual void ReloadCachedMappings(IList<string> names)
			{
			}
		}
	}
}
