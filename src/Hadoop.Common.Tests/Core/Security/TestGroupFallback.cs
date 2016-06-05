using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Log4j;


namespace Org.Apache.Hadoop.Security
{
	public class TestGroupFallback
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestGroupFallback));

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGroupShell()
		{
			Logger.GetRootLogger().SetLevel(Level.Debug);
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.HadoopSecurityGroupMapping, "org.apache.hadoop.security.ShellBasedUnixGroupsMapping"
				);
			Groups groups = new Groups(conf);
			string username = Runtime.GetProperty("user.name");
			IList<string> groupList = groups.GetGroups(username);
			Log.Info(username + " has GROUPS: " + groupList.ToString());
			Assert.True(groupList.Count > 0);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNetgroupShell()
		{
			Logger.GetRootLogger().SetLevel(Level.Debug);
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.HadoopSecurityGroupMapping, "org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMapping"
				);
			Groups groups = new Groups(conf);
			string username = Runtime.GetProperty("user.name");
			IList<string> groupList = groups.GetGroups(username);
			Log.Info(username + " has GROUPS: " + groupList.ToString());
			Assert.True(groupList.Count > 0);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGroupWithFallback()
		{
			Log.Info("running 'mvn -Pnative -DTestGroupFallback clear test' will " + "test the normal path and 'mvn -DTestGroupFallback clear test' will"
				 + " test the fall back functionality");
			Logger.GetRootLogger().SetLevel(Level.Debug);
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.HadoopSecurityGroupMapping, "org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback"
				);
			Groups groups = new Groups(conf);
			string username = Runtime.GetProperty("user.name");
			IList<string> groupList = groups.GetGroups(username);
			Log.Info(username + " has GROUPS: " + groupList.ToString());
			Assert.True(groupList.Count > 0);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNetgroupWithFallback()
		{
			Log.Info("running 'mvn -Pnative -DTestGroupFallback clear test' will " + "test the normal path and 'mvn -DTestGroupFallback clear test' will"
				 + " test the fall back functionality");
			Logger.GetRootLogger().SetLevel(Level.Debug);
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.HadoopSecurityGroupMapping, "org.apache.hadoop.security.JniBasedUnixGroupsNetgroupMappingWithFallback"
				);
			Groups groups = new Groups(conf);
			string username = Runtime.GetProperty("user.name");
			IList<string> groupList = groups.GetGroups(username);
			Log.Info(username + " has GROUPS: " + groupList.ToString());
			Assert.True(groupList.Count > 0);
		}
	}
}
