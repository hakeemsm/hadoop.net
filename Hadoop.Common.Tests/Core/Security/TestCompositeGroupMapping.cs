using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	public class TestCompositeGroupMapping
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestCompositeGroupMapping
			));

		private static Configuration conf = new Configuration();

		private class TestUser
		{
			internal string name;

			internal string group;

			internal string group2;

			public TestUser(string name, string group)
			{
				this.name = name;
				this.group = group;
			}

			public TestUser(string name, string group, string group2)
				: this(name, group)
			{
				this.group2 = group2;
			}
		}

		private static TestCompositeGroupMapping.TestUser john = new TestCompositeGroupMapping.TestUser
			("John", "user-group");

		private static TestCompositeGroupMapping.TestUser hdfs = new TestCompositeGroupMapping.TestUser
			("hdfs", "supergroup");

		private static TestCompositeGroupMapping.TestUser jack = new TestCompositeGroupMapping.TestUser
			("Jack", "user-group", "dev-group-1");

		private const string ProviderSpecificConf = ".test.prop";

		private const string ProviderSpecificConfKey = GroupMappingServiceProvider.GroupMappingConfigPrefix
			 + ProviderSpecificConf;

		private const string ProviderSpecificConfValueForUser = "value-for-user";

		private const string ProviderSpecificConfValueForCluster = "value-for-cluster";

		private abstract class GroupMappingProviderBase : GroupMappingServiceProvider, Configurable
		{
			private Configuration conf;

			public virtual void SetConf(Configuration conf)
			{
				this.conf = conf;
			}

			public virtual Configuration GetConf()
			{
				return this.conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void CacheGroupsRefresh()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void CacheGroupsAdd(IList<string> groups)
			{
			}

			protected internal virtual IList<string> ToList(string group)
			{
				if (group != null)
				{
					return Arrays.AsList(new string[] { group });
				}
				return new AList<string>();
			}

			protected internal virtual void CheckTestConf(string expectedValue)
			{
				string configValue = GetConf().Get(ProviderSpecificConfKey);
				if (configValue == null || !configValue.Equals(expectedValue))
				{
					throw new RuntimeException("Failed to find mandatory configuration of " + ProviderSpecificConfKey
						);
				}
			}

			public abstract IList<string> GetGroups(string arg1);
		}

		private class UserProvider : TestCompositeGroupMapping.GroupMappingProviderBase
		{
			/// <exception cref="System.IO.IOException"/>
			public override IList<string> GetGroups(string user)
			{
				CheckTestConf(ProviderSpecificConfValueForUser);
				string group = null;
				if (user.Equals(john.name))
				{
					group = john.group;
				}
				else
				{
					if (user.Equals(jack.name))
					{
						group = jack.group;
					}
				}
				return ToList(group);
			}
		}

		private class ClusterProvider : TestCompositeGroupMapping.GroupMappingProviderBase
		{
			/// <exception cref="System.IO.IOException"/>
			public override IList<string> GetGroups(string user)
			{
				CheckTestConf(ProviderSpecificConfValueForCluster);
				string group = null;
				if (user.Equals(hdfs.name))
				{
					group = hdfs.group;
				}
				else
				{
					if (user.Equals(jack.name))
					{
						// jack has another group from clusterProvider
						group = jack.group2;
					}
				}
				return ToList(group);
			}
		}

		static TestCompositeGroupMapping()
		{
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(CompositeGroupsMapping
				), typeof(GroupMappingServiceProvider));
			conf.Set(CompositeGroupsMapping.MappingProvidersConfigKey, "userProvider,clusterProvider"
				);
			conf.SetClass(CompositeGroupsMapping.MappingProviderConfigPrefix + ".userProvider"
				, typeof(TestCompositeGroupMapping.UserProvider), typeof(GroupMappingServiceProvider
				));
			conf.SetClass(CompositeGroupsMapping.MappingProviderConfigPrefix + ".clusterProvider"
				, typeof(TestCompositeGroupMapping.ClusterProvider), typeof(GroupMappingServiceProvider
				));
			conf.Set(CompositeGroupsMapping.MappingProviderConfigPrefix + ".clusterProvider" 
				+ ProviderSpecificConf, ProviderSpecificConfValueForCluster);
			conf.Set(CompositeGroupsMapping.MappingProviderConfigPrefix + ".userProvider" + ProviderSpecificConf
				, ProviderSpecificConfValueForUser);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleGroupsMapping()
		{
			Groups groups = new Groups(conf);
			NUnit.Framework.Assert.IsTrue(groups.GetGroups(john.name)[0].Equals(john.group));
			NUnit.Framework.Assert.IsTrue(groups.GetGroups(hdfs.name)[0].Equals(hdfs.group));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleGroupsMappingWithCombined()
		{
			conf.Set(CompositeGroupsMapping.MappingProvidersCombinedConfigKey, "true");
			Groups groups = new Groups(conf);
			NUnit.Framework.Assert.IsTrue(groups.GetGroups(jack.name).Count == 2);
			// the configured providers list in order is "userProvider,clusterProvider"
			// group -> userProvider, group2 -> clusterProvider
			NUnit.Framework.Assert.IsTrue(groups.GetGroups(jack.name).Contains(jack.group));
			NUnit.Framework.Assert.IsTrue(groups.GetGroups(jack.name).Contains(jack.group2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleGroupsMappingWithoutCombined()
		{
			conf.Set(CompositeGroupsMapping.MappingProvidersCombinedConfigKey, "false");
			Groups groups = new Groups(conf);
			// the configured providers list in order is "userProvider,clusterProvider"
			// group -> userProvider, group2 -> clusterProvider
			NUnit.Framework.Assert.IsTrue(groups.GetGroups(jack.name).Count == 1);
			NUnit.Framework.Assert.IsTrue(groups.GetGroups(jack.name)[0].Equals(jack.group));
		}
	}
}
