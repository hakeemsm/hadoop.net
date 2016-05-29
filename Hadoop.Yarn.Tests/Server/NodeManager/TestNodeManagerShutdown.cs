using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestNodeManagerShutdown
	{
		internal static readonly FilePath basedir = new FilePath("target", typeof(TestNodeManagerShutdown
			).FullName);

		internal static readonly FilePath tmpDir = new FilePath(basedir, "tmpDir");

		internal static readonly FilePath logsDir = new FilePath(basedir, "logs");

		internal static readonly FilePath remoteLogsDir = new FilePath(basedir, "remotelogs"
			);

		internal static readonly FilePath nmLocalDir = new FilePath(basedir, "nm0");

		internal static readonly FilePath processStartFile = new FilePath(tmpDir, "start_file.txt"
			).GetAbsoluteFile();

		internal static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		internal const string user = "nobody";

		private FileContext localFS;

		private ContainerId cId;

		private NodeManager nm;

		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		[SetUp]
		public virtual void Setup()
		{
			localFS = FileContext.GetLocalFSFileContext();
			tmpDir.Mkdirs();
			logsDir.Mkdirs();
			remoteLogsDir.Mkdirs();
			nmLocalDir.Mkdirs();
			// Construct the Container-id
			cId = CreateContainerId();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (nm != null)
			{
				nm.Stop();
			}
			localFS.Delete(new Path(basedir.GetPath()), true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStateStoreRemovalOnDecommission()
		{
			FilePath recoveryDir = new FilePath(basedir, "nm-recovery");
			nm = new TestNodeManagerShutdown.TestNodeManager(this);
			YarnConfiguration conf = CreateNMConfig();
			conf.SetBoolean(YarnConfiguration.NmRecoveryEnabled, true);
			conf.Set(YarnConfiguration.NmRecoveryDir, recoveryDir.GetAbsolutePath());
			// verify state store is not removed on normal shutdown
			nm.Init(conf);
			nm.Start();
			NUnit.Framework.Assert.IsTrue(recoveryDir.Exists());
			NUnit.Framework.Assert.IsTrue(recoveryDir.IsDirectory());
			nm.Stop();
			nm = null;
			NUnit.Framework.Assert.IsTrue(recoveryDir.Exists());
			NUnit.Framework.Assert.IsTrue(recoveryDir.IsDirectory());
			// verify state store is removed on decommissioned shutdown
			nm = new TestNodeManagerShutdown.TestNodeManager(this);
			nm.Init(conf);
			nm.Start();
			NUnit.Framework.Assert.IsTrue(recoveryDir.Exists());
			NUnit.Framework.Assert.IsTrue(recoveryDir.IsDirectory());
			nm.GetNMContext().SetDecommissioned(true);
			nm.Stop();
			nm = null;
			NUnit.Framework.Assert.IsFalse(recoveryDir.Exists());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestKillContainersOnShutdown()
		{
			nm = new TestNodeManagerShutdown.TestNodeManager(this);
			nm.Init(CreateNMConfig());
			nm.Start();
			StartContainer(nm, cId, localFS, tmpDir, processStartFile);
			int MaxTries = 20;
			int numTries = 0;
			while (!processStartFile.Exists() && numTries < MaxTries)
			{
				try
				{
					Sharpen.Thread.Sleep(500);
				}
				catch (Exception ex)
				{
					Sharpen.Runtime.PrintStackTrace(ex);
				}
				numTries++;
			}
			nm.Stop();
			// Now verify the contents of the file.  Script generates a message when it
			// receives a sigterm so we look for that.  We cannot perform this check on
			// Windows, because the process is not notified when killed by winutils.
			// There is no way for the process to trap and respond.  Instead, we can
			// verify that the job object with ID matching container ID no longer exists.
			if (Shell.Windows)
			{
				NUnit.Framework.Assert.IsFalse("Process is still alive!", DefaultContainerExecutor
					.ContainerIsAlive(cId.ToString()));
			}
			else
			{
				BufferedReader reader = new BufferedReader(new FileReader(processStartFile));
				bool foundSigTermMessage = false;
				while (true)
				{
					string line = reader.ReadLine();
					if (line == null)
					{
						break;
					}
					if (line.Contains("SIGTERM"))
					{
						foundSigTermMessage = true;
						break;
					}
				}
				NUnit.Framework.Assert.IsTrue("Did not find sigterm message", foundSigTermMessage
					);
				reader.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public static void StartContainer(NodeManager nm, ContainerId cId, FileContext localFS
			, FilePath scriptFileDir, FilePath processStartFile)
		{
			FilePath scriptFile = CreateUnhaltingScriptFile(cId, scriptFileDir, processStartFile
				);
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			NodeId nodeId = BuilderUtils.NewNodeId(Sharpen.Extensions.GetAddressByName("localhost"
				).ToString(), 12345);
			URL localResourceUri = ConverterUtils.GetYarnUrlFromPath(localFS.MakeQualified(new 
				Path(scriptFile.GetAbsolutePath())));
			LocalResource localResource = recordFactory.NewRecordInstance<LocalResource>();
			localResource.SetResource(localResourceUri);
			localResource.SetSize(-1);
			localResource.SetVisibility(LocalResourceVisibility.Application);
			localResource.SetType(LocalResourceType.File);
			localResource.SetTimestamp(scriptFile.LastModified());
			string destinationFile = "dest_file";
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			localResources[destinationFile] = localResource;
			containerLaunchContext.SetLocalResources(localResources);
			IList<string> commands = Arrays.AsList(Shell.GetRunScriptCommand(scriptFile));
			containerLaunchContext.SetCommands(commands);
			IPEndPoint containerManagerBindAddress = NetUtils.CreateSocketAddrForHost("127.0.0.1"
				, 12345);
			UserGroupInformation currentUser = UserGroupInformation.CreateRemoteUser(cId.ToString
				());
			Org.Apache.Hadoop.Security.Token.Token<NMTokenIdentifier> nmToken = ConverterUtils
				.ConvertFromYarn(nm.GetNMContext().GetNMTokenSecretManager().CreateNMToken(cId.GetApplicationAttemptId
				(), nodeId, user), containerManagerBindAddress);
			currentUser.AddToken(nmToken);
			ContainerManagementProtocol containerManager = currentUser.DoAs(new _PrivilegedAction_229
				());
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
				, TestContainerManager.CreateContainerToken(cId, 0, nodeId, user, nm.GetNMContext
				().GetContainerTokenSecretManager()));
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(scRequest);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			containerManager.StartContainers(allRequests);
			IList<ContainerId> containerIds = new AList<ContainerId>();
			containerIds.AddItem(cId);
			GetContainerStatusesRequest request = GetContainerStatusesRequest.NewInstance(containerIds
				);
			ContainerStatus containerStatus = containerManager.GetContainerStatuses(request).
				GetContainerStatuses()[0];
			NUnit.Framework.Assert.AreEqual(ContainerState.Running, containerStatus.GetState(
				));
		}

		private sealed class _PrivilegedAction_229 : PrivilegedAction<ContainerManagementProtocol
			>
		{
			public _PrivilegedAction_229()
			{
			}

			public ContainerManagementProtocol Run()
			{
				Configuration conf = new Configuration();
				YarnRPC rpc = YarnRPC.Create(conf);
				IPEndPoint containerManagerBindAddress = NetUtils.CreateSocketAddrForHost("127.0.0.1"
					, 12345);
				return (ContainerManagementProtocol)rpc.GetProxy(typeof(ContainerManagementProtocol
					), containerManagerBindAddress, conf);
			}
		}

		public static ContainerId CreateContainerId()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 0);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 0);
			return containerId;
		}

		private YarnConfiguration CreateNMConfig()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.NmPmemMb, 5 * 1024);
			// 5GB
			conf.Set(YarnConfiguration.NmAddress, "127.0.0.1:12345");
			conf.Set(YarnConfiguration.NmLocalizerAddress, "127.0.0.1:12346");
			conf.Set(YarnConfiguration.NmLogDirs, logsDir.GetAbsolutePath());
			conf.Set(YarnConfiguration.NmRemoteAppLogDir, remoteLogsDir.GetAbsolutePath());
			conf.Set(YarnConfiguration.NmLocalDirs, nmLocalDir.GetAbsolutePath());
			conf.SetLong(YarnConfiguration.NmLogRetainSeconds, 1);
			return conf;
		}

		/// <summary>
		/// Creates a script to run a container that will run forever unless
		/// stopped by external means.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static FilePath CreateUnhaltingScriptFile(ContainerId cId, FilePath scriptFileDir
			, FilePath processStartFile)
		{
			FilePath scriptFile = Shell.AppendScriptExtension(scriptFileDir, "scriptFile");
			PrintWriter fileWriter = new PrintWriter(scriptFile);
			if (Shell.Windows)
			{
				fileWriter.WriteLine("@echo \"Running testscript for delayed kill\"");
				fileWriter.WriteLine("@echo \"Writing pid to start file\"");
				fileWriter.WriteLine("@echo " + cId + ">> " + processStartFile);
				fileWriter.WriteLine("@pause");
			}
			else
			{
				fileWriter.Write("#!/bin/bash\n\n");
				fileWriter.Write("echo \"Running testscript for delayed kill\"\n");
				fileWriter.Write("hello=\"Got SIGTERM\"\n");
				fileWriter.Write("umask 0\n");
				fileWriter.Write("trap \"echo $hello >> " + processStartFile + "\" SIGTERM\n");
				fileWriter.Write("echo \"Writing pid to start file\"\n");
				fileWriter.Write("echo $$ >> " + processStartFile + "\n");
				fileWriter.Write("while true; do\ndate >> /dev/null;\n done\n");
			}
			fileWriter.Close();
			return scriptFile;
		}

		internal class TestNodeManager : NodeManager
		{
			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				MockNodeStatusUpdater myNodeStatusUpdater = new MockNodeStatusUpdater(context, dispatcher
					, healthChecker, this.metrics);
				return myNodeStatusUpdater;
			}

			public virtual void SetMasterKey(MasterKey masterKey)
			{
				this.GetNMContext().GetContainerTokenSecretManager().SetMasterKey(masterKey);
			}

			internal TestNodeManager(TestNodeManagerShutdown _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestNodeManagerShutdown _enclosing;
		}
	}
}
