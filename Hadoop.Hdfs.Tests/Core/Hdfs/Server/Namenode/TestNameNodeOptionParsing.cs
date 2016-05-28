using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestNameNodeOptionParsing
	{
		public virtual void TestUpgrade()
		{
			HdfsServerConstants.StartupOption opt = null;
			// UPGRADE is set, but nothing else
			opt = NameNode.ParseArguments(new string[] { "-upgrade" });
			NUnit.Framework.Assert.AreEqual(opt, HdfsServerConstants.StartupOption.Upgrade);
			NUnit.Framework.Assert.IsNull(opt.GetClusterId());
			NUnit.Framework.Assert.IsTrue(FSImageFormat.renameReservedMap.IsEmpty());
			// cluster ID is set
			opt = NameNode.ParseArguments(new string[] { "-upgrade", "-clusterid", "mycid" });
			NUnit.Framework.Assert.AreEqual(HdfsServerConstants.StartupOption.Upgrade, opt);
			NUnit.Framework.Assert.AreEqual("mycid", opt.GetClusterId());
			NUnit.Framework.Assert.IsTrue(FSImageFormat.renameReservedMap.IsEmpty());
			// Everything is set
			opt = NameNode.ParseArguments(new string[] { "-upgrade", "-clusterid", "mycid", "-renameReserved"
				, ".snapshot=.my-snapshot,.reserved=.my-reserved" });
			NUnit.Framework.Assert.AreEqual(HdfsServerConstants.StartupOption.Upgrade, opt);
			NUnit.Framework.Assert.AreEqual("mycid", opt.GetClusterId());
			NUnit.Framework.Assert.AreEqual(".my-snapshot", FSImageFormat.renameReservedMap[".snapshot"
				]);
			NUnit.Framework.Assert.AreEqual(".my-reserved", FSImageFormat.renameReservedMap[".reserved"
				]);
			// Reset the map
			FSImageFormat.renameReservedMap.Clear();
			// Everything is set, but in a different order
			opt = NameNode.ParseArguments(new string[] { "-upgrade", "-renameReserved", ".reserved=.my-reserved,.snapshot=.my-snapshot"
				, "-clusterid", "mycid" });
			NUnit.Framework.Assert.AreEqual(HdfsServerConstants.StartupOption.Upgrade, opt);
			NUnit.Framework.Assert.AreEqual("mycid", opt.GetClusterId());
			NUnit.Framework.Assert.AreEqual(".my-snapshot", FSImageFormat.renameReservedMap[".snapshot"
				]);
			NUnit.Framework.Assert.AreEqual(".my-reserved", FSImageFormat.renameReservedMap[".reserved"
				]);
			// Try the default renameReserved
			opt = NameNode.ParseArguments(new string[] { "-upgrade", "-renameReserved" });
			NUnit.Framework.Assert.AreEqual(HdfsServerConstants.StartupOption.Upgrade, opt);
			NUnit.Framework.Assert.AreEqual(".snapshot." + HdfsConstants.NamenodeLayoutVersion
				 + ".UPGRADE_RENAMED", FSImageFormat.renameReservedMap[".snapshot"]);
			NUnit.Framework.Assert.AreEqual(".reserved." + HdfsConstants.NamenodeLayoutVersion
				 + ".UPGRADE_RENAMED", FSImageFormat.renameReservedMap[".reserved"]);
			// Try some error conditions
			try
			{
				opt = NameNode.ParseArguments(new string[] { "-upgrade", "-renameReserved", ".reserved=.my-reserved,.not-reserved=.my-not-reserved"
					 });
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("Unknown reserved path", e);
			}
			try
			{
				opt = NameNode.ParseArguments(new string[] { "-upgrade", "-renameReserved", ".reserved=.my-reserved,.snapshot=.snapshot"
					 });
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("Invalid rename path", e);
			}
			try
			{
				opt = NameNode.ParseArguments(new string[] { "-upgrade", "-renameReserved", ".snapshot=.reserved"
					 });
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("Invalid rename path", e);
			}
			opt = NameNode.ParseArguments(new string[] { "-upgrade", "-cid" });
			NUnit.Framework.Assert.IsNull(opt);
		}

		public virtual void TestRollingUpgrade()
		{
			{
				string[] args = new string[] { "-rollingUpgrade" };
				HdfsServerConstants.StartupOption opt = NameNode.ParseArguments(args);
				NUnit.Framework.Assert.IsNull(opt);
			}
			{
				string[] args = new string[] { "-rollingUpgrade", "started" };
				HdfsServerConstants.StartupOption opt = NameNode.ParseArguments(args);
				NUnit.Framework.Assert.AreEqual(HdfsServerConstants.StartupOption.Rollingupgrade, 
					opt);
				NUnit.Framework.Assert.AreEqual(HdfsServerConstants.RollingUpgradeStartupOption.Started
					, opt.GetRollingUpgradeStartupOption());
				NUnit.Framework.Assert.IsTrue(HdfsServerConstants.RollingUpgradeStartupOption.Started
					.Matches(opt));
			}
			{
				string[] args = new string[] { "-rollingUpgrade", "downgrade" };
				HdfsServerConstants.StartupOption opt = NameNode.ParseArguments(args);
				NUnit.Framework.Assert.AreEqual(HdfsServerConstants.StartupOption.Rollingupgrade, 
					opt);
				NUnit.Framework.Assert.AreEqual(HdfsServerConstants.RollingUpgradeStartupOption.Downgrade
					, opt.GetRollingUpgradeStartupOption());
				NUnit.Framework.Assert.IsTrue(HdfsServerConstants.RollingUpgradeStartupOption.Downgrade
					.Matches(opt));
			}
			{
				string[] args = new string[] { "-rollingUpgrade", "rollback" };
				HdfsServerConstants.StartupOption opt = NameNode.ParseArguments(args);
				NUnit.Framework.Assert.AreEqual(HdfsServerConstants.StartupOption.Rollingupgrade, 
					opt);
				NUnit.Framework.Assert.AreEqual(HdfsServerConstants.RollingUpgradeStartupOption.Rollback
					, opt.GetRollingUpgradeStartupOption());
				NUnit.Framework.Assert.IsTrue(HdfsServerConstants.RollingUpgradeStartupOption.Rollback
					.Matches(opt));
			}
			{
				string[] args = new string[] { "-rollingUpgrade", "foo" };
				try
				{
					NameNode.ParseArguments(args);
					NUnit.Framework.Assert.Fail();
				}
				catch (ArgumentException)
				{
				}
			}
		}
		// the exception is expected.
	}
}
