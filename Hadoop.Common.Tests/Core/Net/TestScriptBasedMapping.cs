using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	public class TestScriptBasedMapping : TestCase
	{
		public TestScriptBasedMapping()
		{
		}

		[NUnit.Framework.Test]
		public virtual void TestNoArgsMeansNoResult()
		{
			Configuration conf = new Configuration();
			conf.SetInt(ScriptBasedMapping.ScriptArgCountKey, ScriptBasedMapping.MinAllowableArgs
				 - 1);
			conf.Set(ScriptBasedMapping.ScriptFilenameKey, "any-filename");
			conf.Set(ScriptBasedMapping.ScriptFilenameKey, "any-filename");
			ScriptBasedMapping mapping = CreateMapping(conf);
			IList<string> names = new AList<string>();
			names.AddItem("some.machine.name");
			names.AddItem("other.machine.name");
			IList<string> result = mapping.Resolve(names);
			NUnit.Framework.Assert.IsNull("Expected an empty list", result);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoFilenameMeansSingleSwitch()
		{
			Configuration conf = new Configuration();
			ScriptBasedMapping mapping = CreateMapping(conf);
			NUnit.Framework.Assert.IsTrue("Expected to be single switch", mapping.IsSingleSwitch
				());
			NUnit.Framework.Assert.IsTrue("Expected to be single switch", AbstractDNSToSwitchMapping
				.IsMappingSingleSwitch(mapping));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFilenameMeansMultiSwitch()
		{
			Configuration conf = new Configuration();
			conf.Set(ScriptBasedMapping.ScriptFilenameKey, "any-filename");
			ScriptBasedMapping mapping = CreateMapping(conf);
			NUnit.Framework.Assert.IsFalse("Expected to be multi switch", mapping.IsSingleSwitch
				());
			mapping.SetConf(new Configuration());
			NUnit.Framework.Assert.IsTrue("Expected to be single switch", mapping.IsSingleSwitch
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNullConfig()
		{
			ScriptBasedMapping mapping = CreateMapping(null);
			NUnit.Framework.Assert.IsTrue("Expected to be single switch", mapping.IsSingleSwitch
				());
		}

		private ScriptBasedMapping CreateMapping(Configuration conf)
		{
			ScriptBasedMapping mapping = new ScriptBasedMapping();
			mapping.SetConf(conf);
			return mapping;
		}
	}
}
