using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>
	/// Tests for HAAdmin command with
	/// <see cref="Org.Apache.Hadoop.Hdfs.MiniDFSCluster"/>
	/// set up in HA mode.
	/// </summary>
	public class TestDFSHAAdminMiniCluster
	{
		static TestDFSHAAdminMiniCluster()
		{
			((Log4JLogger)LogFactory.GetLog(typeof(HAAdmin))).GetLogger().SetLevel(Level.All);
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(TestDFSHAAdminMiniCluster
			));

		private MiniDFSCluster cluster;

		private Configuration conf;

		private DFSHAAdmin tool;

		private readonly ByteArrayOutputStream errOutBytes = new ByteArrayOutputStream();

		private string errOutput;

		private int nn1Port;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(0).Build();
			tool = new DFSHAAdmin();
			tool.SetConf(conf);
			tool.SetErrOut(new TextWriter(errOutBytes));
			cluster.WaitActive();
			nn1Port = cluster.GetNameNodePort(0);
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Shutdown()
		{
			cluster.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetServiceState()
		{
			NUnit.Framework.Assert.AreEqual(0, RunTool("-getServiceState", "nn1"));
			NUnit.Framework.Assert.AreEqual(0, RunTool("-getServiceState", "nn2"));
			cluster.TransitionToActive(0);
			NUnit.Framework.Assert.AreEqual(0, RunTool("-getServiceState", "nn1"));
			NameNodeAdapter.EnterSafeMode(cluster.GetNameNode(0), false);
			NUnit.Framework.Assert.AreEqual(0, RunTool("-getServiceState", "nn1"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStateTransition()
		{
			NameNode nnode1 = cluster.GetNameNode(0);
			NUnit.Framework.Assert.IsTrue(nnode1.IsStandbyState());
			NUnit.Framework.Assert.AreEqual(0, RunTool("-transitionToActive", "nn1"));
			NUnit.Framework.Assert.IsFalse(nnode1.IsStandbyState());
			NUnit.Framework.Assert.AreEqual(0, RunTool("-transitionToStandby", "nn1"));
			NUnit.Framework.Assert.IsTrue(nnode1.IsStandbyState());
			NameNode nnode2 = cluster.GetNameNode(1);
			NUnit.Framework.Assert.IsTrue(nnode2.IsStandbyState());
			NUnit.Framework.Assert.AreEqual(0, RunTool("-transitionToActive", "nn2"));
			NUnit.Framework.Assert.IsFalse(nnode2.IsStandbyState());
			NUnit.Framework.Assert.AreEqual(0, RunTool("-transitionToStandby", "nn2"));
			NUnit.Framework.Assert.IsTrue(nnode2.IsStandbyState());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTryFailoverToSafeMode()
		{
			conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, TestDFSHAAdmin.GetFencerTrueCommand(
				));
			tool.SetConf(conf);
			NameNodeAdapter.EnterSafeMode(cluster.GetNameNode(0), false);
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-failover", "nn2", "nn1"));
			NUnit.Framework.Assert.IsTrue("Bad output: " + errOutput, errOutput.Contains("is not ready to become active: "
				 + "The NameNode is in safemode"));
		}

		/// <summary>Test failover with various options</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFencer()
		{
			// Test failover with no fencer
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-failover", "nn1", "nn2"));
			// Set up fencer to write info about the fencing target into a
			// tmp file, so we can verify that the args were substituted right
			FilePath tmpFile = FilePath.CreateTempFile("testFencer", ".txt");
			tmpFile.DeleteOnExit();
			if (Shell.Windows)
			{
				conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, "shell(echo %target_nameserviceid%.%target_namenodeid% "
					 + "%target_port% %dfs_ha_namenode_id% > " + tmpFile.GetAbsolutePath() + ")");
			}
			else
			{
				conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, "shell(echo -n $target_nameserviceid.$target_namenodeid "
					 + "$target_port $dfs_ha_namenode_id > " + tmpFile.GetAbsolutePath() + ")");
			}
			// Test failover with fencer
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(0, RunTool("-transitionToActive", "nn1"));
			NUnit.Framework.Assert.AreEqual(0, RunTool("-failover", "nn1", "nn2"));
			// Test failover with fencer and nameservice
			NUnit.Framework.Assert.AreEqual(0, RunTool("-ns", "minidfs-ns", "-failover", "nn2"
				, "nn1"));
			// Fencer has not run yet, since none of the above required fencing 
			NUnit.Framework.Assert.AreEqual(string.Empty, Files.ToString(tmpFile, Charsets.Utf8
				));
			// Test failover with fencer and forcefence option
			NUnit.Framework.Assert.AreEqual(0, RunTool("-failover", "nn1", "nn2", "--forcefence"
				));
			// The fence script should run with the configuration from the target
			// node, rather than the configuration from the fencing node. Strip
			// out any trailing spaces and CR/LFs which may be present on Windows.
			string fenceCommandOutput = Files.ToString(tmpFile, Charsets.Utf8).ReplaceAll(" *[\r\n]+"
				, string.Empty);
			NUnit.Framework.Assert.AreEqual("minidfs-ns.nn1 " + nn1Port + " nn1", fenceCommandOutput
				);
			tmpFile.Delete();
			// Test failover with forceactive option
			NUnit.Framework.Assert.AreEqual(0, RunTool("-failover", "nn2", "nn1", "--forceactive"
				));
			// Fencing should not occur, since it was graceful
			NUnit.Framework.Assert.IsFalse(tmpFile.Exists());
			// Test failover with not fencer and forcefence option
			conf.Unset(DFSConfigKeys.DfsHaFenceMethodsKey);
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-failover", "nn1", "nn2", "--forcefence"
				));
			NUnit.Framework.Assert.IsFalse(tmpFile.Exists());
			// Test failover with bad fencer and forcefence option
			conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, "foobar!");
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-failover", "nn1", "nn2", "--forcefence"
				));
			NUnit.Framework.Assert.IsFalse(tmpFile.Exists());
			// Test failover with force fence listed before the other arguments
			conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, TestDFSHAAdmin.GetFencerTrueCommand(
				));
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(0, RunTool("-failover", "--forcefence", "nn1", "nn2"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckHealth()
		{
			NUnit.Framework.Assert.AreEqual(0, RunTool("-checkHealth", "nn1"));
			NUnit.Framework.Assert.AreEqual(0, RunTool("-checkHealth", "nn2"));
		}

		/// <summary>Test case to check whether both the name node is active or not</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTransitionToActiveWhenOtherNamenodeisActive()
		{
			NameNode nn1 = cluster.GetNameNode(0);
			NameNode nn2 = cluster.GetNameNode(1);
			if (nn1.GetState() != null && !nn1.GetState().Equals(HAServiceProtocol.HAServiceState
				.Standby.ToString()))
			{
				cluster.TransitionToStandby(0);
			}
			if (nn2.GetState() != null && !nn2.GetState().Equals(HAServiceProtocol.HAServiceState
				.Standby.ToString()))
			{
				cluster.TransitionToStandby(1);
			}
			//Making sure both the namenode are in standby state
			NUnit.Framework.Assert.IsTrue(nn1.IsStandbyState());
			NUnit.Framework.Assert.IsTrue(nn2.IsStandbyState());
			// Triggering the transition for both namenode to Active
			RunTool("-transitionToActive", "nn1");
			RunTool("-transitionToActive", "nn2");
			NUnit.Framework.Assert.IsFalse("Both namenodes cannot be active", nn1.IsActiveState
				() && nn2.IsActiveState());
			/*  In this test case, we have deliberately shut down nn1 and this will
			cause HAAAdmin#isOtherTargetNodeActive to throw an Exception
			and transitionToActive for nn2 with  forceActive switch will succeed
			even with Exception  */
			cluster.ShutdownNameNode(0);
			if (nn2.GetState() != null && !nn2.GetState().Equals(HAServiceProtocol.HAServiceState
				.Standby.ToString()))
			{
				cluster.TransitionToStandby(1);
			}
			//Making sure both the namenode (nn2) is in standby state
			NUnit.Framework.Assert.IsTrue(nn2.IsStandbyState());
			NUnit.Framework.Assert.IsFalse(cluster.IsNameNodeUp(0));
			RunTool("-transitionToActive", "nn2", "--forceactive");
			NUnit.Framework.Assert.IsTrue("Namenode nn2 should be active", nn2.IsActiveState(
				));
		}

		/// <exception cref="System.Exception"/>
		private int RunTool(params string[] args)
		{
			errOutBytes.Reset();
			Log.Info("Running: DFSHAAdmin " + Joiner.On(" ").Join(args));
			int ret = tool.Run(args);
			errOutput = new string(errOutBytes.ToByteArray(), Charsets.Utf8);
			Log.Info("Output:\n" + errOutput);
			return ret;
		}
	}
}
