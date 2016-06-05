using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class TestRackResolverScriptBasedMapping
	{
		[NUnit.Framework.Test]
		public virtual void TestScriptName()
		{
			Configuration conf = new Configuration();
			conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
				typeof(ScriptBasedMapping), typeof(DNSToSwitchMapping));
			conf.Set(CommonConfigurationKeysPublic.NetTopologyScriptFileNameKey, "testScript"
				);
			RackResolver.Init(conf);
			NUnit.Framework.Assert.AreEqual(RackResolver.GetDnsToSwitchMapping().ToString(), 
				"script-based mapping with script testScript");
		}
	}
}
