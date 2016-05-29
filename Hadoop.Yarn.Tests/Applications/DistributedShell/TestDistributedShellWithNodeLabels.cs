using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Applications.Distributedshell
{
	public class TestDistributedShellWithNodeLabels
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDistributedShellWithNodeLabels
			));

		internal const int NumNms = 2;

		internal TestDistributedShell distShellTest;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			distShellTest = new TestDistributedShell();
			distShellTest.SetupInternal(NumNms);
		}

		/// <exception cref="System.IO.IOException"/>
		private void InitializeNodeLabels()
		{
			RMContext rmContext = distShellTest.yarnCluster.GetResourceManager(0).GetRMContext
				();
			// Setup node labels
			RMNodeLabelsManager labelsMgr = rmContext.GetNodeLabelManager();
			ICollection<string> labels = new HashSet<string>();
			labels.AddItem("x");
			labelsMgr.AddToCluserNodeLabels(labels);
			// Setup queue access to node labels
			distShellTest.conf.Set("yarn.scheduler.capacity.root.accessible-node-labels", "x"
				);
			distShellTest.conf.Set("yarn.scheduler.capacity.root.accessible-node-labels.x.capacity"
				, "100");
			distShellTest.conf.Set("yarn.scheduler.capacity.root.default.accessible-node-labels"
				, "x");
			distShellTest.conf.Set("yarn.scheduler.capacity.root.default.accessible-node-labels.x.capacity"
				, "100");
			rmContext.GetScheduler().Reinitialize(distShellTest.conf, rmContext);
			// Fetch node-ids from yarn cluster
			NodeId[] nodeIds = new NodeId[NumNms];
			for (int i = 0; i < NumNms; i++)
			{
				NodeManager mgr = distShellTest.yarnCluster.GetNodeManager(i);
				nodeIds[i] = mgr.GetNMContext().GetNodeId();
			}
			// Set label x to NM[1]
			labelsMgr.AddLabelsToNode(ImmutableMap.Of(nodeIds[1], labels));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDSShellWithNodeLabelExpression()
		{
			InitializeNodeLabels();
			// Start NMContainerMonitor
			TestDistributedShellWithNodeLabels.NMContainerMonitor mon = new TestDistributedShellWithNodeLabels.NMContainerMonitor
				(this);
			Sharpen.Thread t = new Sharpen.Thread(mon);
			t.Start();
			// Submit a job which will sleep for 60 sec
			string[] args = new string[] { "--jar", TestDistributedShell.AppmasterJar, "--num_containers"
				, "4", "--shell_command", "sleep", "--shell_args", "15", "--master_memory", "512"
				, "--master_vcores", "2", "--container_memory", "128", "--container_vcores", "1"
				, "--node_label_expression", "x" };
			Log.Info("Initializing DS Client");
			Client client = new Client(new Configuration(distShellTest.yarnCluster.GetConfig(
				)));
			bool initSuccess = client.Init(args);
			NUnit.Framework.Assert.IsTrue(initSuccess);
			Log.Info("Running DS Client");
			bool result = client.Run();
			Log.Info("Client run completed. Result=" + result);
			t.Interrupt();
			// Check maximum number of containers on each NMs
			int[] maxRunningContainersOnNMs = mon.GetMaxRunningContainersReport();
			// Check no container allocated on NM[0]
			NUnit.Framework.Assert.AreEqual(0, maxRunningContainersOnNMs[0]);
			// Check there're some containers allocated on NM[1]
			NUnit.Framework.Assert.IsTrue(maxRunningContainersOnNMs[1] > 0);
		}

		/// <summary>Monitor containers running on NMs</summary>
		internal class NMContainerMonitor : Runnable
		{
			internal const int SamplingIntervalMs = 500;

			internal int[] maxRunningContainersOnNMs = new int[TestDistributedShellWithNodeLabels
				.NumNms];

			// The interval of milliseconds of sampling (500ms)
			// The maximum number of containers running on each NMs
			public virtual void Run()
			{
				while (true)
				{
					for (int i = 0; i < TestDistributedShellWithNodeLabels.NumNms; i++)
					{
						int nContainers = this._enclosing.distShellTest.yarnCluster.GetNodeManager(i).GetNMContext
							().GetContainers().Count;
						if (nContainers > this.maxRunningContainersOnNMs[i])
						{
							this.maxRunningContainersOnNMs[i] = nContainers;
						}
					}
					try
					{
						Sharpen.Thread.Sleep(TestDistributedShellWithNodeLabels.NMContainerMonitor.SamplingIntervalMs
							);
					}
					catch (Exception e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
						break;
					}
				}
			}

			public virtual int[] GetMaxRunningContainersReport()
			{
				return this.maxRunningContainersOnNMs;
			}

			internal NMContainerMonitor(TestDistributedShellWithNodeLabels _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDistributedShellWithNodeLabels _enclosing;
		}
	}
}
