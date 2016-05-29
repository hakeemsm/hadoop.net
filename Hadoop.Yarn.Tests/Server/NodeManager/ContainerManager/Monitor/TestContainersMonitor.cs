using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor
{
	public class TestContainersMonitor : BaseContainerManagerTest
	{
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		public TestContainersMonitor()
			: base()
		{
		}

		static TestContainersMonitor()
		{
			Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor.TestContainersMonitor
				));
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public override void Setup()
		{
			conf.SetClass(YarnConfiguration.NmContainerMonResourceCalculator, typeof(LinuxResourceCalculatorPlugin
				), typeof(ResourceCalculatorPlugin));
			conf.SetBoolean(YarnConfiguration.NmVmemCheckEnabled, true);
			base.Setup();
		}

		/// <summary>Test to verify the check for whether a process tree is over limit or not.
		/// 	</summary>
		/// <exception cref="System.IO.IOException">
		/// if there was a problem setting up the fake procfs directories or
		/// files.
		/// </exception>
		[NUnit.Framework.Test]
		public virtual void TestProcessTreeLimits()
		{
			// set up a dummy proc file system
			FilePath procfsRootDir = new FilePath(localDir, "proc");
			string[] pids = new string[] { "100", "200", "300", "400", "500", "600", "700" };
			try
			{
				TestProcfsBasedProcessTree.SetupProcfsRootDir(procfsRootDir);
				// create pid dirs.
				TestProcfsBasedProcessTree.SetupPidDirs(procfsRootDir, pids);
				// create process infos.
				TestProcfsBasedProcessTree.ProcessStatInfo[] procs = new TestProcfsBasedProcessTree.ProcessStatInfo
					[7];
				// assume pids 100, 500 are in 1 tree
				// 200,300,400 are in another
				// 600,700 are in a third
				procs[0] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "100", "proc1"
					, "1", "100", "100", "100000" });
				procs[1] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "200", "proc2"
					, "1", "200", "200", "200000" });
				procs[2] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "300", "proc3"
					, "200", "200", "200", "300000" });
				procs[3] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "400", "proc4"
					, "200", "200", "200", "400000" });
				procs[4] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "500", "proc5"
					, "100", "100", "100", "1500000" });
				procs[5] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "600", "proc6"
					, "1", "600", "600", "100000" });
				procs[6] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "700", "proc7"
					, "600", "600", "600", "100000" });
				// write stat files.
				TestProcfsBasedProcessTree.WriteStatFiles(procfsRootDir, pids, procs, null);
				// vmem limit
				long limit = 700000;
				ContainersMonitorImpl test = new ContainersMonitorImpl(null, null, null);
				// create process trees
				// tree rooted at 100 is over limit immediately, as it is
				// twice over the mem limit.
				ProcfsBasedProcessTree pTree = new ProcfsBasedProcessTree("100", procfsRootDir.GetAbsolutePath
					());
				pTree.UpdateProcessTree();
				NUnit.Framework.Assert.IsTrue("tree rooted at 100 should be over limit " + "after first iteration."
					, test.IsProcessTreeOverLimit(pTree, "dummyId", limit));
				// the tree rooted at 200 is initially below limit.
				pTree = new ProcfsBasedProcessTree("200", procfsRootDir.GetAbsolutePath());
				pTree.UpdateProcessTree();
				NUnit.Framework.Assert.IsFalse("tree rooted at 200 shouldn't be over limit " + "after one iteration."
					, test.IsProcessTreeOverLimit(pTree, "dummyId", limit));
				// second iteration - now the tree has been over limit twice,
				// hence it should be declared over limit.
				pTree.UpdateProcessTree();
				NUnit.Framework.Assert.IsTrue("tree rooted at 200 should be over limit after 2 iterations"
					, test.IsProcessTreeOverLimit(pTree, "dummyId", limit));
				// the tree rooted at 600 is never over limit.
				pTree = new ProcfsBasedProcessTree("600", procfsRootDir.GetAbsolutePath());
				pTree.UpdateProcessTree();
				NUnit.Framework.Assert.IsFalse("tree rooted at 600 should never be over limit.", 
					test.IsProcessTreeOverLimit(pTree, "dummyId", limit));
				// another iteration does not make any difference.
				pTree.UpdateProcessTree();
				NUnit.Framework.Assert.IsFalse("tree rooted at 600 should never be over limit.", 
					test.IsProcessTreeOverLimit(pTree, "dummyId", limit));
			}
			finally
			{
				FileUtil.FullyDelete(procfsRootDir);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerKillOnMemoryOverflow()
		{
			if (!ProcfsBasedProcessTree.IsAvailable())
			{
				return;
			}
			containerManager.Start();
			FilePath scriptFile = new FilePath(tmpDir, "scriptFile.sh");
			PrintWriter fileWriter = new PrintWriter(scriptFile);
			FilePath processStartFile = new FilePath(tmpDir, "start_file.txt").GetAbsoluteFile
				();
			fileWriter.Write("\numask 0");
			// So that start file is readable by the
			// test.
			fileWriter.Write("\necho Hello World! > " + processStartFile);
			fileWriter.Write("\necho $$ >> " + processStartFile);
			fileWriter.Write("\nsleep 15");
			fileWriter.Close();
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			// ////// Construct the Container-id
			ApplicationId appId = ApplicationId.NewInstance(0, 0);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId cId = ContainerId.NewContainerId(appAttemptId, 0);
			int port = 12345;
			URL resource_alpha = ConverterUtils.GetYarnUrlFromPath(localFS.MakeQualified(new 
				Path(scriptFile.GetAbsolutePath())));
			LocalResource rsrc_alpha = recordFactory.NewRecordInstance<LocalResource>();
			rsrc_alpha.SetResource(resource_alpha);
			rsrc_alpha.SetSize(-1);
			rsrc_alpha.SetVisibility(LocalResourceVisibility.Application);
			rsrc_alpha.SetType(LocalResourceType.File);
			rsrc_alpha.SetTimestamp(scriptFile.LastModified());
			string destinationFile = "dest_file";
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			localResources[destinationFile] = rsrc_alpha;
			containerLaunchContext.SetLocalResources(localResources);
			IList<string> commands = new AList<string>();
			commands.AddItem("/bin/bash");
			commands.AddItem(scriptFile.GetAbsolutePath());
			containerLaunchContext.SetCommands(commands);
			Resource r = BuilderUtils.NewResource(8 * 1024 * 1024, 1);
			ContainerTokenIdentifier containerIdentifier = new ContainerTokenIdentifier(cId, 
				context.GetNodeId().ToString(), user, r, Runtime.CurrentTimeMillis() + 120000, 123
				, DummyRmIdentifier, Priority.NewInstance(0), 0);
			Token containerToken = BuilderUtils.NewContainerToken(context.GetNodeId(), containerManager
				.GetContext().GetContainerTokenSecretManager().CreatePassword(containerIdentifier
				), containerIdentifier);
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
				, containerToken);
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(scRequest);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			containerManager.StartContainers(allRequests);
			int timeoutSecs = 0;
			while (!processStartFile.Exists() && timeoutSecs++ < 20)
			{
				Sharpen.Thread.Sleep(1000);
				Log.Info("Waiting for process start-file to be created");
			}
			NUnit.Framework.Assert.IsTrue("ProcessStartFile doesn't exist!", processStartFile
				.Exists());
			// Now verify the contents of the file
			BufferedReader reader = new BufferedReader(new FileReader(processStartFile));
			NUnit.Framework.Assert.AreEqual("Hello World!", reader.ReadLine());
			// Get the pid of the process
			string pid = reader.ReadLine().Trim();
			// No more lines
			NUnit.Framework.Assert.AreEqual(null, reader.ReadLine());
			BaseContainerManagerTest.WaitForContainerState(containerManager, cId, ContainerState
				.Complete, 60);
			IList<ContainerId> containerIds = new AList<ContainerId>();
			containerIds.AddItem(cId);
			GetContainerStatusesRequest gcsRequest = GetContainerStatusesRequest.NewInstance(
				containerIds);
			ContainerStatus containerStatus = containerManager.GetContainerStatuses(gcsRequest
				).GetContainerStatuses()[0];
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.KilledExceededVmem, containerStatus
				.GetExitStatus());
			string expectedMsgPattern = "Container \\[pid=" + pid + ",containerID=" + cId + "\\] is running beyond virtual memory limits. Current usage: "
				 + "[0-9.]+ ?[KMGTPE]?B of [0-9.]+ ?[KMGTPE]?B physical memory used; " + "[0-9.]+ ?[KMGTPE]?B of [0-9.]+ ?[KMGTPE]?B virtual memory used. "
				 + "Killing container.\nDump of the process-tree for " + cId + " :\n";
			Sharpen.Pattern pat = Sharpen.Pattern.Compile(expectedMsgPattern);
			NUnit.Framework.Assert.AreEqual("Expected message pattern is: " + expectedMsgPattern
				 + "\n\nObserved message is: " + containerStatus.GetDiagnostics(), true, pat.Matcher
				(containerStatus.GetDiagnostics()).Find());
			// Assert that the process is not alive anymore
			NUnit.Framework.Assert.IsFalse("Process is still alive!", exec.SignalContainer(user
				, pid, ContainerExecutor.Signal.Null));
		}

		public virtual void TestContainerMonitorMemFlags()
		{
			ContainersMonitor cm = null;
			long expPmem = 8192 * 1024 * 1024l;
			long expVmem = (long)(expPmem * 2.1f);
			cm = new ContainersMonitorImpl(Org.Mockito.Mockito.Mock<ContainerExecutor>(), Org.Mockito.Mockito.Mock
				<AsyncDispatcher>(), Org.Mockito.Mockito.Mock<Context>());
			cm.Init(GetConfForCM(false, false, 8192, 2.1f));
			NUnit.Framework.Assert.AreEqual(expPmem, cm.GetPmemAllocatedForContainers());
			NUnit.Framework.Assert.AreEqual(expVmem, cm.GetVmemAllocatedForContainers());
			NUnit.Framework.Assert.AreEqual(false, cm.IsPmemCheckEnabled());
			NUnit.Framework.Assert.AreEqual(false, cm.IsVmemCheckEnabled());
			cm = new ContainersMonitorImpl(Org.Mockito.Mockito.Mock<ContainerExecutor>(), Org.Mockito.Mockito.Mock
				<AsyncDispatcher>(), Org.Mockito.Mockito.Mock<Context>());
			cm.Init(GetConfForCM(true, false, 8192, 2.1f));
			NUnit.Framework.Assert.AreEqual(expPmem, cm.GetPmemAllocatedForContainers());
			NUnit.Framework.Assert.AreEqual(expVmem, cm.GetVmemAllocatedForContainers());
			NUnit.Framework.Assert.AreEqual(true, cm.IsPmemCheckEnabled());
			NUnit.Framework.Assert.AreEqual(false, cm.IsVmemCheckEnabled());
			cm = new ContainersMonitorImpl(Org.Mockito.Mockito.Mock<ContainerExecutor>(), Org.Mockito.Mockito.Mock
				<AsyncDispatcher>(), Org.Mockito.Mockito.Mock<Context>());
			cm.Init(GetConfForCM(true, true, 8192, 2.1f));
			NUnit.Framework.Assert.AreEqual(expPmem, cm.GetPmemAllocatedForContainers());
			NUnit.Framework.Assert.AreEqual(expVmem, cm.GetVmemAllocatedForContainers());
			NUnit.Framework.Assert.AreEqual(true, cm.IsPmemCheckEnabled());
			NUnit.Framework.Assert.AreEqual(true, cm.IsVmemCheckEnabled());
			cm = new ContainersMonitorImpl(Org.Mockito.Mockito.Mock<ContainerExecutor>(), Org.Mockito.Mockito.Mock
				<AsyncDispatcher>(), Org.Mockito.Mockito.Mock<Context>());
			cm.Init(GetConfForCM(false, true, 8192, 2.1f));
			NUnit.Framework.Assert.AreEqual(expPmem, cm.GetPmemAllocatedForContainers());
			NUnit.Framework.Assert.AreEqual(expVmem, cm.GetVmemAllocatedForContainers());
			NUnit.Framework.Assert.AreEqual(false, cm.IsPmemCheckEnabled());
			NUnit.Framework.Assert.AreEqual(true, cm.IsVmemCheckEnabled());
		}

		private YarnConfiguration GetConfForCM(bool pMemEnabled, bool vMemEnabled, int nmPmem
			, float vMemToPMemRatio)
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.NmPmemMb, nmPmem);
			conf.SetBoolean(YarnConfiguration.NmPmemCheckEnabled, pMemEnabled);
			conf.SetBoolean(YarnConfiguration.NmVmemCheckEnabled, vMemEnabled);
			conf.SetFloat(YarnConfiguration.NmVmemPmemRatio, vMemToPMemRatio);
			return conf;
		}
	}
}
