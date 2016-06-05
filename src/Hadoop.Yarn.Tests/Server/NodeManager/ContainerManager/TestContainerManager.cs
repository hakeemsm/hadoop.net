using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager
{
	public class TestContainerManager : BaseContainerManagerTest
	{
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		public TestContainerManager()
			: base()
		{
		}

		static TestContainerManager()
		{
			Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.TestContainerManager
				));
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public override void Setup()
		{
			base.Setup();
		}

		private ContainerId CreateContainerId(int id)
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 0);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, id);
			return containerId;
		}

		protected internal override ContainerManagerImpl CreateContainerManager(DeletionService
			 delSrvc)
		{
			return new _ContainerManagerImpl_115(this, context, exec, delSrvc, nodeStatusUpdater
				, metrics, new ApplicationACLsManager(conf), dirsHandler);
		}

		private sealed class _ContainerManagerImpl_115 : ContainerManagerImpl
		{
			public _ContainerManagerImpl_115(TestContainerManager _enclosing, Context baseArg1
				, ContainerExecutor baseArg2, DeletionService baseArg3, NodeStatusUpdater baseArg4
				, NodeManagerMetrics baseArg5, ApplicationACLsManager baseArg6, LocalDirsHandlerService
				 baseArg7)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7)
			{
				this._enclosing = _enclosing;
			}

			public override void SetBlockNewContainerRequests(bool blockNewContainerRequests)
			{
			}

			// do nothing
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			protected internal override UserGroupInformation GetRemoteUgi()
			{
				ApplicationId appId = ApplicationId.NewInstance(0, 0);
				ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
				UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(appAttemptId.ToString
					());
				ugi.AddTokenIdentifier(new NMTokenIdentifier(appAttemptId, this.context.GetNodeId
					(), this._enclosing.user, this.context.GetNMTokenSecretManager().GetCurrentKey()
					.GetKeyId()));
				return ugi;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			protected internal override void AuthorizeGetAndStopContainerRequest(ContainerId 
				containerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				 container, bool stopRequest, NMTokenIdentifier identifier)
			{
				if (container == null || container.GetUser().Equals("Fail"))
				{
					throw new YarnException("Reject this container");
				}
			}

			private readonly TestContainerManager _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerManagerInitialization()
		{
			containerManager.Start();
			IPAddress localAddr = Sharpen.Runtime.GetLocalHost();
			string fqdn = localAddr.ToString();
			if (!localAddr.GetHostAddress().Equals(fqdn))
			{
				// only check if fqdn is not same as ip
				// api returns ip in case of resolution failure
				NUnit.Framework.Assert.AreEqual(fqdn, context.GetNodeId().GetHost());
			}
			// Just do a query for a non-existing container.
			bool throwsException = false;
			try
			{
				IList<ContainerId> containerIds = new AList<ContainerId>();
				ContainerId id = CreateContainerId(0);
				containerIds.AddItem(id);
				GetContainerStatusesRequest request = GetContainerStatusesRequest.NewInstance(containerIds
					);
				GetContainerStatusesResponse response = containerManager.GetContainerStatuses(request
					);
				if (response.GetFailedRequests().Contains(id))
				{
					throw response.GetFailedRequests()[id].DeSerialize();
				}
			}
			catch
			{
				throwsException = true;
			}
			NUnit.Framework.Assert.IsTrue(throwsException);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerSetup()
		{
			containerManager.Start();
			// ////// Create the resources for the container
			FilePath dir = new FilePath(tmpDir, "dir");
			dir.Mkdirs();
			FilePath file = new FilePath(dir, "file");
			PrintWriter fileWriter = new PrintWriter(file);
			fileWriter.Write("Hello World!");
			fileWriter.Close();
			// ////// Construct the Container-id
			ContainerId cId = CreateContainerId(0);
			// ////// Construct the container-spec.
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			URL resource_alpha = ConverterUtils.GetYarnUrlFromPath(localFS.MakeQualified(new 
				Path(file.GetAbsolutePath())));
			LocalResource rsrc_alpha = recordFactory.NewRecordInstance<LocalResource>();
			rsrc_alpha.SetResource(resource_alpha);
			rsrc_alpha.SetSize(-1);
			rsrc_alpha.SetVisibility(LocalResourceVisibility.Application);
			rsrc_alpha.SetType(LocalResourceType.File);
			rsrc_alpha.SetTimestamp(file.LastModified());
			string destinationFile = "dest_file";
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			localResources[destinationFile] = rsrc_alpha;
			containerLaunchContext.SetLocalResources(localResources);
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
				, CreateContainerToken(cId, DummyRmIdentifier, context.GetNodeId(), user, context
				.GetContainerTokenSecretManager()));
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(scRequest);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			containerManager.StartContainers(allRequests);
			BaseContainerManagerTest.WaitForContainerState(containerManager, cId, ContainerState
				.Complete);
			// Now ascertain that the resources are localised correctly.
			ApplicationId appId = cId.GetApplicationAttemptId().GetApplicationId();
			string appIDStr = ConverterUtils.ToString(appId);
			string containerIDStr = ConverterUtils.ToString(cId);
			FilePath userCacheDir = new FilePath(localDir, ContainerLocalizer.Usercache);
			FilePath userDir = new FilePath(userCacheDir, user);
			FilePath appCache = new FilePath(userDir, ContainerLocalizer.Appcache);
			FilePath appDir = new FilePath(appCache, appIDStr);
			FilePath containerDir = new FilePath(appDir, containerIDStr);
			FilePath targetFile = new FilePath(containerDir, destinationFile);
			FilePath sysDir = new FilePath(localDir, ResourceLocalizationService.NmPrivateDir
				);
			FilePath appSysDir = new FilePath(sysDir, appIDStr);
			FilePath containerSysDir = new FilePath(appSysDir, containerIDStr);
			foreach (FilePath f in new FilePath[] { localDir, sysDir, userCacheDir, appDir, appSysDir
				, containerDir, containerSysDir })
			{
				NUnit.Framework.Assert.IsTrue(f.GetAbsolutePath() + " doesn't exist!!", f.Exists(
					));
				NUnit.Framework.Assert.IsTrue(f.GetAbsolutePath() + " is not a directory!!", f.IsDirectory
					());
			}
			NUnit.Framework.Assert.IsTrue(targetFile.GetAbsolutePath() + " doesn't exist!!", 
				targetFile.Exists());
			// Now verify the contents of the file
			BufferedReader reader = new BufferedReader(new FileReader(targetFile));
			NUnit.Framework.Assert.AreEqual("Hello World!", reader.ReadLine());
			NUnit.Framework.Assert.AreEqual(null, reader.ReadLine());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLaunchAndStop()
		{
			containerManager.Start();
			FilePath scriptFile = Shell.AppendScriptExtension(tmpDir, "scriptFile");
			PrintWriter fileWriter = new PrintWriter(scriptFile);
			FilePath processStartFile = new FilePath(tmpDir, "start_file.txt").GetAbsoluteFile
				();
			// ////// Construct the Container-id
			ContainerId cId = CreateContainerId(0);
			if (Shell.Windows)
			{
				fileWriter.WriteLine("@echo Hello World!> " + processStartFile);
				fileWriter.WriteLine("@echo " + cId + ">> " + processStartFile);
				fileWriter.WriteLine("@ping -n 100 127.0.0.1 >nul");
			}
			else
			{
				fileWriter.Write("\numask 0");
				// So that start file is readable by the test
				fileWriter.Write("\necho Hello World! > " + processStartFile);
				fileWriter.Write("\necho $$ >> " + processStartFile);
				fileWriter.Write("\nexec sleep 100");
			}
			fileWriter.Close();
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
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
			IList<string> commands = Arrays.AsList(Shell.GetRunScriptCommand(scriptFile));
			containerLaunchContext.SetCommands(commands);
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
				, CreateContainerToken(cId, DummyRmIdentifier, context.GetNodeId(), user, context
				.GetContainerTokenSecretManager()));
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
			// Now test the stop functionality.
			// Assert that the process is alive
			NUnit.Framework.Assert.IsTrue("Process is not alive!", DefaultContainerExecutor.ContainerIsAlive
				(pid));
			// Once more
			NUnit.Framework.Assert.IsTrue("Process is not alive!", DefaultContainerExecutor.ContainerIsAlive
				(pid));
			IList<ContainerId> containerIds = new AList<ContainerId>();
			containerIds.AddItem(cId);
			StopContainersRequest stopRequest = StopContainersRequest.NewInstance(containerIds
				);
			containerManager.StopContainers(stopRequest);
			BaseContainerManagerTest.WaitForContainerState(containerManager, cId, ContainerState
				.Complete);
			GetContainerStatusesRequest gcsRequest = GetContainerStatusesRequest.NewInstance(
				containerIds);
			ContainerStatus containerStatus = containerManager.GetContainerStatuses(gcsRequest
				).GetContainerStatuses()[0];
			int expectedExitCode = ContainerExitStatus.KilledByAppmaster;
			NUnit.Framework.Assert.AreEqual(expectedExitCode, containerStatus.GetExitStatus()
				);
			// Assert that the process is not alive anymore
			NUnit.Framework.Assert.IsFalse("Process is still alive!", DefaultContainerExecutor
				.ContainerIsAlive(pid));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void TestContainerLaunchAndExit(int exitCode)
		{
			FilePath scriptFile = Shell.AppendScriptExtension(tmpDir, "scriptFile");
			PrintWriter fileWriter = new PrintWriter(scriptFile);
			FilePath processStartFile = new FilePath(tmpDir, "start_file.txt").GetAbsoluteFile
				();
			// ////// Construct the Container-id
			ContainerId cId = CreateContainerId(0);
			if (Shell.Windows)
			{
				fileWriter.WriteLine("@echo Hello World!> " + processStartFile);
				fileWriter.WriteLine("@echo " + cId + ">> " + processStartFile);
				if (exitCode != 0)
				{
					fileWriter.WriteLine("@exit " + exitCode);
				}
			}
			else
			{
				fileWriter.Write("\numask 0");
				// So that start file is readable by the test
				fileWriter.Write("\necho Hello World! > " + processStartFile);
				fileWriter.Write("\necho $$ >> " + processStartFile);
				// Have script throw an exit code at the end
				if (exitCode != 0)
				{
					fileWriter.Write("\nexit " + exitCode);
				}
			}
			fileWriter.Close();
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
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
			IList<string> commands = Arrays.AsList(Shell.GetRunScriptCommand(scriptFile));
			containerLaunchContext.SetCommands(commands);
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
				, CreateContainerToken(cId, DummyRmIdentifier, context.GetNodeId(), user, context
				.GetContainerTokenSecretManager()));
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(scRequest);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			containerManager.StartContainers(allRequests);
			BaseContainerManagerTest.WaitForContainerState(containerManager, cId, ContainerState
				.Complete);
			IList<ContainerId> containerIds = new AList<ContainerId>();
			containerIds.AddItem(cId);
			GetContainerStatusesRequest gcsRequest = GetContainerStatusesRequest.NewInstance(
				containerIds);
			ContainerStatus containerStatus = containerManager.GetContainerStatuses(gcsRequest
				).GetContainerStatuses()[0];
			// Verify exit status matches exit state of script
			NUnit.Framework.Assert.AreEqual(exitCode, containerStatus.GetExitStatus());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLaunchAndExitSuccess()
		{
			containerManager.Start();
			int exitCode = 0;
			// launch context for a command that will return exit code 0 
			// and verify exit code returned 
			TestContainerLaunchAndExit(exitCode);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLaunchAndExitFailure()
		{
			containerManager.Start();
			int exitCode = 50;
			// launch context for a command that will return exit code 0 
			// and verify exit code returned 
			TestContainerLaunchAndExit(exitCode);
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalFilesCleanup()
		{
			// Real del service
			delSrvc = new DeletionService(exec);
			delSrvc.Init(conf);
			containerManager = CreateContainerManager(delSrvc);
			containerManager.Init(conf);
			containerManager.Start();
			// ////// Create the resources for the container
			FilePath dir = new FilePath(tmpDir, "dir");
			dir.Mkdirs();
			FilePath file = new FilePath(dir, "file");
			PrintWriter fileWriter = new PrintWriter(file);
			fileWriter.Write("Hello World!");
			fileWriter.Close();
			// ////// Construct the Container-id
			ContainerId cId = CreateContainerId(0);
			ApplicationId appId = cId.GetApplicationAttemptId().GetApplicationId();
			// ////// Construct the container-spec.
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			//    containerLaunchContext.resources =
			//        new HashMap<CharSequence, LocalResource>();
			URL resource_alpha = ConverterUtils.GetYarnUrlFromPath(FileContext.GetLocalFSFileContext
				().MakeQualified(new Path(file.GetAbsolutePath())));
			LocalResource rsrc_alpha = recordFactory.NewRecordInstance<LocalResource>();
			rsrc_alpha.SetResource(resource_alpha);
			rsrc_alpha.SetSize(-1);
			rsrc_alpha.SetVisibility(LocalResourceVisibility.Application);
			rsrc_alpha.SetType(LocalResourceType.File);
			rsrc_alpha.SetTimestamp(file.LastModified());
			string destinationFile = "dest_file";
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			localResources[destinationFile] = rsrc_alpha;
			containerLaunchContext.SetLocalResources(localResources);
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
				, CreateContainerToken(cId, DummyRmIdentifier, context.GetNodeId(), user, context
				.GetContainerTokenSecretManager()));
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(scRequest);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			containerManager.StartContainers(allRequests);
			BaseContainerManagerTest.WaitForContainerState(containerManager, cId, ContainerState
				.Complete);
			BaseContainerManagerTest.WaitForApplicationState(containerManager, cId.GetApplicationAttemptId
				().GetApplicationId(), ApplicationState.Running);
			// Now ascertain that the resources are localised correctly.
			string appIDStr = ConverterUtils.ToString(appId);
			string containerIDStr = ConverterUtils.ToString(cId);
			FilePath userCacheDir = new FilePath(localDir, ContainerLocalizer.Usercache);
			FilePath userDir = new FilePath(userCacheDir, user);
			FilePath appCache = new FilePath(userDir, ContainerLocalizer.Appcache);
			FilePath appDir = new FilePath(appCache, appIDStr);
			FilePath containerDir = new FilePath(appDir, containerIDStr);
			FilePath targetFile = new FilePath(containerDir, destinationFile);
			FilePath sysDir = new FilePath(localDir, ResourceLocalizationService.NmPrivateDir
				);
			FilePath appSysDir = new FilePath(sysDir, appIDStr);
			FilePath containerSysDir = new FilePath(appSysDir, containerIDStr);
			// AppDir should still exist
			NUnit.Framework.Assert.IsTrue("AppDir " + appDir.GetAbsolutePath() + " doesn't exist!!"
				, appDir.Exists());
			NUnit.Framework.Assert.IsTrue("AppSysDir " + appSysDir.GetAbsolutePath() + " doesn't exist!!"
				, appSysDir.Exists());
			foreach (FilePath f in new FilePath[] { containerDir, containerSysDir })
			{
				NUnit.Framework.Assert.IsFalse(f.GetAbsolutePath() + " exists!!", f.Exists());
			}
			NUnit.Framework.Assert.IsFalse(targetFile.GetAbsolutePath() + " exists!!", targetFile
				.Exists());
			// Simulate RM sending an AppFinish event.
			containerManager.Handle(new CMgrCompletedAppsEvent(Arrays.AsList(new ApplicationId
				[] { appId }), CMgrCompletedAppsEvent.Reason.OnShutdown));
			BaseContainerManagerTest.WaitForApplicationState(containerManager, cId.GetApplicationAttemptId
				().GetApplicationId(), ApplicationState.Finished);
			// Now ascertain that the resources are localised correctly.
			foreach (FilePath f_1 in new FilePath[] { appDir, containerDir, appSysDir, containerSysDir
				 })
			{
				// Wait for deletion. Deletion can happen long after AppFinish because of
				// the async DeletionService
				int timeout = 0;
				while (f_1.Exists() && timeout++ < 15)
				{
					Sharpen.Thread.Sleep(1000);
				}
				NUnit.Framework.Assert.IsFalse(f_1.GetAbsolutePath() + " exists!!", f_1.Exists());
			}
			// Wait for deletion
			int timeout_1 = 0;
			while (targetFile.Exists() && timeout_1++ < 15)
			{
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.IsFalse(targetFile.GetAbsolutePath() + " exists!!", targetFile
				.Exists());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLaunchFromPreviousRM()
		{
			containerManager.Start();
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			ContainerId cId1 = CreateContainerId(0);
			ContainerId cId2 = CreateContainerId(0);
			containerLaunchContext.SetLocalResources(new Dictionary<string, LocalResource>());
			// Construct the Container with Invalid RMIdentifier
			StartContainerRequest startRequest1 = StartContainerRequest.NewInstance(containerLaunchContext
				, CreateContainerToken(cId1, ResourceManagerConstants.RmInvalidIdentifier, context
				.GetNodeId(), user, context.GetContainerTokenSecretManager()));
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(startRequest1);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			containerManager.StartContainers(allRequests);
			bool catchException = false;
			try
			{
				StartContainersResponse response = containerManager.StartContainers(allRequests);
				if (response.GetFailedRequests().Contains(cId1))
				{
					throw response.GetFailedRequests()[cId1].DeSerialize();
				}
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				catchException = true;
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("Container " + cId1 + " rejected as it is allocated by a previous RM"
					));
				NUnit.Framework.Assert.IsTrue(Sharpen.Runtime.EqualsIgnoreCase(e.GetType().FullName
					, typeof(InvalidContainerException).FullName));
			}
			// Verify that startContainer fail because of invalid container request
			NUnit.Framework.Assert.IsTrue(catchException);
			// Construct the Container with a RMIdentifier within current RM
			StartContainerRequest startRequest2 = StartContainerRequest.NewInstance(containerLaunchContext
				, CreateContainerToken(cId2, DummyRmIdentifier, context.GetNodeId(), user, context
				.GetContainerTokenSecretManager()));
			IList<StartContainerRequest> list2 = new AList<StartContainerRequest>();
			list.AddItem(startRequest2);
			StartContainersRequest allRequests2 = StartContainersRequest.NewInstance(list2);
			containerManager.StartContainers(allRequests2);
			bool noException = true;
			try
			{
				containerManager.StartContainers(allRequests2);
			}
			catch (YarnException)
			{
				noException = false;
			}
			// Verify that startContainer get no YarnException
			NUnit.Framework.Assert.IsTrue(noException);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleContainersLaunch()
		{
			containerManager.Start();
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			for (int i = 0; i < 10; i++)
			{
				ContainerId cId = CreateContainerId(i);
				long identifier = 0;
				if ((i & 1) == 0)
				{
					// container with even id fail
					identifier = ResourceManagerConstants.RmInvalidIdentifier;
				}
				else
				{
					identifier = DummyRmIdentifier;
				}
				Token containerToken = CreateContainerToken(cId, identifier, context.GetNodeId(), 
					user, context.GetContainerTokenSecretManager());
				StartContainerRequest request = StartContainerRequest.NewInstance(containerLaunchContext
					, containerToken);
				list.AddItem(request);
			}
			StartContainersRequest requestList = StartContainersRequest.NewInstance(list);
			StartContainersResponse response = containerManager.StartContainers(requestList);
			NUnit.Framework.Assert.AreEqual(5, response.GetSuccessfullyStartedContainers().Count
				);
			foreach (ContainerId id in response.GetSuccessfullyStartedContainers())
			{
				// Containers with odd id should succeed.
				NUnit.Framework.Assert.AreEqual(1, id.GetContainerId() & 1);
			}
			NUnit.Framework.Assert.AreEqual(5, response.GetFailedRequests().Count);
			foreach (KeyValuePair<ContainerId, SerializedException> entry in response.GetFailedRequests
				())
			{
				// Containers with even id should fail.
				NUnit.Framework.Assert.AreEqual(0, entry.Key.GetContainerId() & 1);
				NUnit.Framework.Assert.IsTrue(entry.Value.GetMessage().Contains("Container " + entry
					.Key + " rejected as it is allocated by a previous RM"));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleContainersStopAndGetStatus()
		{
			containerManager.Start();
			IList<StartContainerRequest> startRequest = new AList<StartContainerRequest>();
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			IList<ContainerId> containerIds = new AList<ContainerId>();
			for (int i = 0; i < 10; i++)
			{
				ContainerId cId = CreateContainerId(i);
				string user = null;
				if ((i & 1) == 0)
				{
					// container with even id fail
					user = "Fail";
				}
				else
				{
					user = "Pass";
				}
				Token containerToken = CreateContainerToken(cId, DummyRmIdentifier, context.GetNodeId
					(), user, context.GetContainerTokenSecretManager());
				StartContainerRequest request = StartContainerRequest.NewInstance(containerLaunchContext
					, containerToken);
				startRequest.AddItem(request);
				containerIds.AddItem(cId);
			}
			// start containers
			StartContainersRequest requestList = StartContainersRequest.NewInstance(startRequest
				);
			containerManager.StartContainers(requestList);
			// Get container statuses
			GetContainerStatusesRequest statusRequest = GetContainerStatusesRequest.NewInstance
				(containerIds);
			GetContainerStatusesResponse statusResponse = containerManager.GetContainerStatuses
				(statusRequest);
			NUnit.Framework.Assert.AreEqual(5, statusResponse.GetContainerStatuses().Count);
			foreach (ContainerStatus status in statusResponse.GetContainerStatuses())
			{
				// Containers with odd id should succeed
				NUnit.Framework.Assert.AreEqual(1, status.GetContainerId().GetContainerId() & 1);
			}
			NUnit.Framework.Assert.AreEqual(5, statusResponse.GetFailedRequests().Count);
			foreach (KeyValuePair<ContainerId, SerializedException> entry in statusResponse.GetFailedRequests
				())
			{
				// Containers with even id should fail.
				NUnit.Framework.Assert.AreEqual(0, entry.Key.GetContainerId() & 1);
				NUnit.Framework.Assert.IsTrue(entry.Value.GetMessage().Contains("Reject this container"
					));
			}
			// stop containers
			StopContainersRequest stopRequest = StopContainersRequest.NewInstance(containerIds
				);
			StopContainersResponse stopResponse = containerManager.StopContainers(stopRequest
				);
			NUnit.Framework.Assert.AreEqual(5, stopResponse.GetSuccessfullyStoppedContainers(
				).Count);
			foreach (ContainerId id in stopResponse.GetSuccessfullyStoppedContainers())
			{
				// Containers with odd id should succeed.
				NUnit.Framework.Assert.AreEqual(1, id.GetContainerId() & 1);
			}
			NUnit.Framework.Assert.AreEqual(5, stopResponse.GetFailedRequests().Count);
			foreach (KeyValuePair<ContainerId, SerializedException> entry_1 in stopResponse.GetFailedRequests
				())
			{
				// Containers with even id should fail.
				NUnit.Framework.Assert.AreEqual(0, entry_1.Key.GetContainerId() & 1);
				NUnit.Framework.Assert.IsTrue(entry_1.Value.GetMessage().Contains("Reject this container"
					));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartContainerFailureWithUnknownAuxService()
		{
			conf.SetStrings(YarnConfiguration.NmAuxServices, new string[] { "existService" });
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, "existService"), typeof(
				TestAuxServices.ServiceA), typeof(Org.Apache.Hadoop.Service.Service));
			containerManager.Start();
			IList<StartContainerRequest> startRequest = new AList<StartContainerRequest>();
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			IDictionary<string, ByteBuffer> serviceData = new Dictionary<string, ByteBuffer>(
				);
			string serviceName = "non_exist_auxService";
			serviceData[serviceName] = ByteBuffer.Wrap(Sharpen.Runtime.GetBytesForString(serviceName
				));
			containerLaunchContext.SetServiceData(serviceData);
			ContainerId cId = CreateContainerId(0);
			string user = "start_container_fail";
			Token containerToken = CreateContainerToken(cId, DummyRmIdentifier, context.GetNodeId
				(), user, context.GetContainerTokenSecretManager());
			StartContainerRequest request = StartContainerRequest.NewInstance(containerLaunchContext
				, containerToken);
			// start containers
			startRequest.AddItem(request);
			StartContainersRequest requestList = StartContainersRequest.NewInstance(startRequest
				);
			StartContainersResponse response = containerManager.StartContainers(requestList);
			NUnit.Framework.Assert.IsTrue(response.GetFailedRequests().Count == 1);
			NUnit.Framework.Assert.IsTrue(response.GetSuccessfullyStartedContainers().Count ==
				 0);
			NUnit.Framework.Assert.IsTrue(response.GetFailedRequests().Contains(cId));
			NUnit.Framework.Assert.IsTrue(response.GetFailedRequests()[cId].GetMessage().Contains
				("The auxService:" + serviceName + " does not exist"));
		}

		/// <exception cref="System.IO.IOException"/>
		public static Token CreateContainerToken(ContainerId cId, long rmIdentifier, NodeId
			 nodeId, string user, NMContainerTokenSecretManager containerTokenSecretManager)
		{
			return CreateContainerToken(cId, rmIdentifier, nodeId, user, containerTokenSecretManager
				, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public static Token CreateContainerToken(ContainerId cId, long rmIdentifier, NodeId
			 nodeId, string user, NMContainerTokenSecretManager containerTokenSecretManager, 
			LogAggregationContext logAggregationContext)
		{
			Resource r = BuilderUtils.NewResource(1024, 1);
			ContainerTokenIdentifier containerTokenIdentifier = new ContainerTokenIdentifier(
				cId, nodeId.ToString(), user, r, Runtime.CurrentTimeMillis() + 100000L, 123, rmIdentifier
				, Priority.NewInstance(0), 0, logAggregationContext);
			Token containerToken = BuilderUtils.NewContainerToken(nodeId, containerTokenSecretManager
				.RetrievePassword(containerTokenIdentifier), containerTokenIdentifier);
			return containerToken;
		}
	}
}
