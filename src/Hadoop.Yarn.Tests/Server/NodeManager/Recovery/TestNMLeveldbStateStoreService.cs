using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery
{
	public class TestNMLeveldbStateStoreService
	{
		private static readonly FilePath TmpDir = new FilePath(Runtime.GetProperty("test.build.data"
			, Runtime.GetProperty("java.io.tmpdir")), typeof(TestNMLeveldbStateStoreService)
			.FullName);

		internal YarnConfiguration conf;

		internal NMLeveldbStateStoreService stateStore;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			FileUtil.FullyDelete(TmpDir);
			conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.NmRecoveryEnabled, true);
			conf.Set(YarnConfiguration.NmRecoveryDir, TmpDir.ToString());
			RestartStateStore();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Cleanup()
		{
			if (stateStore != null)
			{
				stateStore.Close();
			}
			FileUtil.FullyDelete(TmpDir);
		}

		/// <exception cref="System.IO.IOException"/>
		private void RestartStateStore()
		{
			// need to close so leveldb releases database lock
			if (stateStore != null)
			{
				stateStore.Close();
			}
			stateStore = new NMLeveldbStateStoreService();
			stateStore.Init(conf);
			stateStore.Start();
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyEmptyState()
		{
			NMStateStoreService.RecoveredLocalizationState state = stateStore.LoadLocalizationState
				();
			NUnit.Framework.Assert.IsNotNull(state);
			NMStateStoreService.LocalResourceTrackerState pubts = state.GetPublicTrackerState
				();
			NUnit.Framework.Assert.IsNotNull(pubts);
			NUnit.Framework.Assert.IsTrue(pubts.GetLocalizedResources().IsEmpty());
			NUnit.Framework.Assert.IsTrue(pubts.GetInProgressResources().IsEmpty());
			NUnit.Framework.Assert.IsTrue(state.GetUserResources().IsEmpty());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestIsNewlyCreated()
		{
			NUnit.Framework.Assert.IsTrue(stateStore.IsNewlyCreated());
			RestartStateStore();
			NUnit.Framework.Assert.IsFalse(stateStore.IsNewlyCreated());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEmptyState()
		{
			NUnit.Framework.Assert.IsTrue(stateStore.CanRecover());
			VerifyEmptyState();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckVersion()
		{
			// default version
			Version defaultVersion = stateStore.GetCurrentVersion();
			NUnit.Framework.Assert.AreEqual(defaultVersion, stateStore.LoadVersion());
			// compatible version
			Version compatibleVersion = Version.NewInstance(defaultVersion.GetMajorVersion(), 
				defaultVersion.GetMinorVersion() + 2);
			stateStore.StoreVersion(compatibleVersion);
			NUnit.Framework.Assert.AreEqual(compatibleVersion, stateStore.LoadVersion());
			RestartStateStore();
			// overwrite the compatible version
			NUnit.Framework.Assert.AreEqual(defaultVersion, stateStore.LoadVersion());
			// incompatible version
			Version incompatibleVersion = Version.NewInstance(defaultVersion.GetMajorVersion(
				) + 1, defaultVersion.GetMinorVersion());
			stateStore.StoreVersion(incompatibleVersion);
			try
			{
				RestartStateStore();
				NUnit.Framework.Assert.Fail("Incompatible version, should expect fail here.");
			}
			catch (ServiceStateException e)
			{
				NUnit.Framework.Assert.IsTrue("Exception message mismatch", e.Message.Contains("Incompatible version for NM state:"
					));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationStorage()
		{
			// test empty when no state
			NMStateStoreService.RecoveredApplicationsState state = stateStore.LoadApplicationsState
				();
			NUnit.Framework.Assert.IsTrue(state.GetApplications().IsEmpty());
			NUnit.Framework.Assert.IsTrue(state.GetFinishedApplications().IsEmpty());
			// store an application and verify recovered
			ApplicationId appId1 = ApplicationId.NewInstance(1234, 1);
			YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto.Builder builder
				 = YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto.NewBuilder
				();
			builder.SetId(((ApplicationIdPBImpl)appId1).GetProto());
			builder.SetUser("user1");
			YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto appProto1 = 
				((YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto)builder.Build
				());
			stateStore.StoreApplication(appId1, appProto1);
			RestartStateStore();
			state = stateStore.LoadApplicationsState();
			NUnit.Framework.Assert.AreEqual(1, state.GetApplications().Count);
			NUnit.Framework.Assert.AreEqual(appProto1, state.GetApplications()[0]);
			NUnit.Framework.Assert.IsTrue(state.GetFinishedApplications().IsEmpty());
			// finish an application and add a new one
			stateStore.StoreFinishedApplication(appId1);
			ApplicationId appId2 = ApplicationId.NewInstance(1234, 2);
			builder = YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto.NewBuilder
				();
			builder.SetId(((ApplicationIdPBImpl)appId2).GetProto());
			builder.SetUser("user2");
			YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto appProto2 = 
				((YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto)builder.Build
				());
			stateStore.StoreApplication(appId2, appProto2);
			RestartStateStore();
			state = stateStore.LoadApplicationsState();
			NUnit.Framework.Assert.AreEqual(2, state.GetApplications().Count);
			NUnit.Framework.Assert.IsTrue(state.GetApplications().Contains(appProto1));
			NUnit.Framework.Assert.IsTrue(state.GetApplications().Contains(appProto2));
			NUnit.Framework.Assert.AreEqual(1, state.GetFinishedApplications().Count);
			NUnit.Framework.Assert.AreEqual(appId1, state.GetFinishedApplications()[0]);
			// test removing an application
			stateStore.StoreFinishedApplication(appId2);
			stateStore.RemoveApplication(appId2);
			RestartStateStore();
			state = stateStore.LoadApplicationsState();
			NUnit.Framework.Assert.AreEqual(1, state.GetApplications().Count);
			NUnit.Framework.Assert.AreEqual(appProto1, state.GetApplications()[0]);
			NUnit.Framework.Assert.AreEqual(1, state.GetFinishedApplications().Count);
			NUnit.Framework.Assert.AreEqual(appId1, state.GetFinishedApplications()[0]);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerStorage()
		{
			// test empty when no state
			IList<NMStateStoreService.RecoveredContainerState> recoveredContainers = stateStore
				.LoadContainersState();
			NUnit.Framework.Assert.IsTrue(recoveredContainers.IsEmpty());
			// create a container request
			ApplicationId appId = ApplicationId.NewInstance(1234, 3);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 4);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 5);
			LocalResource lrsrc = LocalResource.NewInstance(URL.NewInstance("hdfs", "somehost"
				, 12345, "/some/path/to/rsrc"), LocalResourceType.File, LocalResourceVisibility.
				Application, 123L, 1234567890L);
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			localResources["rsrc"] = lrsrc;
			IDictionary<string, string> env = new Dictionary<string, string>();
			env["somevar"] = "someval";
			IList<string> containerCmds = new AList<string>();
			containerCmds.AddItem("somecmd");
			containerCmds.AddItem("somearg");
			IDictionary<string, ByteBuffer> serviceData = new Dictionary<string, ByteBuffer>(
				);
			serviceData["someservice"] = ByteBuffer.Wrap(new byte[] { unchecked((int)(0x1)), 
				unchecked((int)(0x2)), unchecked((int)(0x3)) });
			ByteBuffer containerTokens = ByteBuffer.Wrap(new byte[] { unchecked((int)(0x7)), 
				unchecked((int)(0x8)), unchecked((int)(0x9)), unchecked((int)(0xa)) });
			IDictionary<ApplicationAccessType, string> acls = new Dictionary<ApplicationAccessType
				, string>();
			acls[ApplicationAccessType.ViewApp] = "viewuser";
			acls[ApplicationAccessType.ModifyApp] = "moduser";
			ContainerLaunchContext clc = ContainerLaunchContext.NewInstance(localResources, env
				, containerCmds, serviceData, containerTokens, acls);
			Resource containerRsrc = Resource.NewInstance(1357, 3);
			ContainerTokenIdentifier containerTokenId = new ContainerTokenIdentifier(containerId
				, "host", "user", containerRsrc, 9876543210L, 42, 2468, Priority.NewInstance(7), 
				13579);
			Token containerToken = Token.NewInstance(containerTokenId.GetBytes(), ContainerTokenIdentifier
				.Kind.ToString(), Sharpen.Runtime.GetBytesForString("password"), "tokenservice");
			StartContainerRequest containerReq = StartContainerRequest.NewInstance(clc, containerToken
				);
			// store a container and verify recovered
			stateStore.StoreContainer(containerId, containerReq);
			RestartStateStore();
			recoveredContainers = stateStore.LoadContainersState();
			NUnit.Framework.Assert.AreEqual(1, recoveredContainers.Count);
			NMStateStoreService.RecoveredContainerState rcs = recoveredContainers[0];
			NUnit.Framework.Assert.AreEqual(NMStateStoreService.RecoveredContainerStatus.Requested
				, rcs.GetStatus());
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.Invalid, rcs.GetExitCode());
			NUnit.Framework.Assert.AreEqual(false, rcs.GetKilled());
			NUnit.Framework.Assert.AreEqual(containerReq, rcs.GetStartRequest());
			NUnit.Framework.Assert.IsTrue(rcs.GetDiagnostics().IsEmpty());
			// store a new container record without StartContainerRequest
			ContainerId containerId1 = ContainerId.NewContainerId(appAttemptId, 6);
			stateStore.StoreContainerLaunched(containerId1);
			recoveredContainers = stateStore.LoadContainersState();
			// check whether the new container record is discarded
			NUnit.Framework.Assert.AreEqual(1, recoveredContainers.Count);
			// launch the container, add some diagnostics, and verify recovered
			StringBuilder diags = new StringBuilder();
			stateStore.StoreContainerLaunched(containerId);
			diags.Append("some diags for container");
			stateStore.StoreContainerDiagnostics(containerId, diags);
			RestartStateStore();
			recoveredContainers = stateStore.LoadContainersState();
			NUnit.Framework.Assert.AreEqual(1, recoveredContainers.Count);
			rcs = recoveredContainers[0];
			NUnit.Framework.Assert.AreEqual(NMStateStoreService.RecoveredContainerStatus.Launched
				, rcs.GetStatus());
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.Invalid, rcs.GetExitCode());
			NUnit.Framework.Assert.AreEqual(false, rcs.GetKilled());
			NUnit.Framework.Assert.AreEqual(containerReq, rcs.GetStartRequest());
			NUnit.Framework.Assert.AreEqual(diags.ToString(), rcs.GetDiagnostics());
			// mark the container killed, add some more diags, and verify recovered
			diags.Append("some more diags for container");
			stateStore.StoreContainerDiagnostics(containerId, diags);
			stateStore.StoreContainerKilled(containerId);
			RestartStateStore();
			recoveredContainers = stateStore.LoadContainersState();
			NUnit.Framework.Assert.AreEqual(1, recoveredContainers.Count);
			rcs = recoveredContainers[0];
			NUnit.Framework.Assert.AreEqual(NMStateStoreService.RecoveredContainerStatus.Launched
				, rcs.GetStatus());
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.Invalid, rcs.GetExitCode());
			NUnit.Framework.Assert.IsTrue(rcs.GetKilled());
			NUnit.Framework.Assert.AreEqual(containerReq, rcs.GetStartRequest());
			NUnit.Framework.Assert.AreEqual(diags.ToString(), rcs.GetDiagnostics());
			// add yet more diags, mark container completed, and verify recovered
			diags.Append("some final diags");
			stateStore.StoreContainerDiagnostics(containerId, diags);
			stateStore.StoreContainerCompleted(containerId, 21);
			RestartStateStore();
			recoveredContainers = stateStore.LoadContainersState();
			NUnit.Framework.Assert.AreEqual(1, recoveredContainers.Count);
			rcs = recoveredContainers[0];
			NUnit.Framework.Assert.AreEqual(NMStateStoreService.RecoveredContainerStatus.Completed
				, rcs.GetStatus());
			NUnit.Framework.Assert.AreEqual(21, rcs.GetExitCode());
			NUnit.Framework.Assert.IsTrue(rcs.GetKilled());
			NUnit.Framework.Assert.AreEqual(containerReq, rcs.GetStartRequest());
			NUnit.Framework.Assert.AreEqual(diags.ToString(), rcs.GetDiagnostics());
			// remove the container and verify not recovered
			stateStore.RemoveContainer(containerId);
			RestartStateStore();
			recoveredContainers = stateStore.LoadContainersState();
			NUnit.Framework.Assert.IsTrue(recoveredContainers.IsEmpty());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStartResourceLocalization()
		{
			string user = "somebody";
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			// start a local resource for an application
			Path appRsrcPath = new Path("hdfs://some/app/resource");
			LocalResourcePBImpl rsrcPb = (LocalResourcePBImpl)LocalResource.NewInstance(ConverterUtils
				.GetYarnUrlFromPath(appRsrcPath), LocalResourceType.Archive, LocalResourceVisibility
				.Application, 123L, 456L);
			YarnProtos.LocalResourceProto appRsrcProto = rsrcPb.GetProto();
			Path appRsrcLocalPath = new Path("/some/local/dir/for/apprsrc");
			stateStore.StartResourceLocalization(user, appId, appRsrcProto, appRsrcLocalPath);
			// restart and verify only app resource is marked in-progress
			RestartStateStore();
			NMStateStoreService.RecoveredLocalizationState state = stateStore.LoadLocalizationState
				();
			NMStateStoreService.LocalResourceTrackerState pubts = state.GetPublicTrackerState
				();
			NUnit.Framework.Assert.IsTrue(pubts.GetLocalizedResources().IsEmpty());
			NUnit.Framework.Assert.IsTrue(pubts.GetInProgressResources().IsEmpty());
			IDictionary<string, NMStateStoreService.RecoveredUserResources> userResources = state
				.GetUserResources();
			NUnit.Framework.Assert.AreEqual(1, userResources.Count);
			NMStateStoreService.RecoveredUserResources rur = userResources[user];
			NMStateStoreService.LocalResourceTrackerState privts = rur.GetPrivateTrackerState
				();
			NUnit.Framework.Assert.IsNotNull(privts);
			NUnit.Framework.Assert.IsTrue(privts.GetLocalizedResources().IsEmpty());
			NUnit.Framework.Assert.IsTrue(privts.GetInProgressResources().IsEmpty());
			NUnit.Framework.Assert.AreEqual(1, rur.GetAppTrackerStates().Count);
			NMStateStoreService.LocalResourceTrackerState appts = rur.GetAppTrackerStates()[appId
				];
			NUnit.Framework.Assert.IsNotNull(appts);
			NUnit.Framework.Assert.IsTrue(appts.GetLocalizedResources().IsEmpty());
			NUnit.Framework.Assert.AreEqual(1, appts.GetInProgressResources().Count);
			NUnit.Framework.Assert.AreEqual(appRsrcLocalPath, appts.GetInProgressResources()[
				appRsrcProto]);
			// start some public and private resources
			Path pubRsrcPath1 = new Path("hdfs://some/public/resource1");
			rsrcPb = (LocalResourcePBImpl)LocalResource.NewInstance(ConverterUtils.GetYarnUrlFromPath
				(pubRsrcPath1), LocalResourceType.File, LocalResourceVisibility.Public, 789L, 135L
				);
			YarnProtos.LocalResourceProto pubRsrcProto1 = rsrcPb.GetProto();
			Path pubRsrcLocalPath1 = new Path("/some/local/dir/for/pubrsrc1");
			stateStore.StartResourceLocalization(null, null, pubRsrcProto1, pubRsrcLocalPath1
				);
			Path pubRsrcPath2 = new Path("hdfs://some/public/resource2");
			rsrcPb = (LocalResourcePBImpl)LocalResource.NewInstance(ConverterUtils.GetYarnUrlFromPath
				(pubRsrcPath2), LocalResourceType.File, LocalResourceVisibility.Public, 789L, 135L
				);
			YarnProtos.LocalResourceProto pubRsrcProto2 = rsrcPb.GetProto();
			Path pubRsrcLocalPath2 = new Path("/some/local/dir/for/pubrsrc2");
			stateStore.StartResourceLocalization(null, null, pubRsrcProto2, pubRsrcLocalPath2
				);
			Path privRsrcPath = new Path("hdfs://some/private/resource");
			rsrcPb = (LocalResourcePBImpl)LocalResource.NewInstance(ConverterUtils.GetYarnUrlFromPath
				(privRsrcPath), LocalResourceType.Pattern, LocalResourceVisibility.Private, 789L
				, 680L, "*pattern*");
			YarnProtos.LocalResourceProto privRsrcProto = rsrcPb.GetProto();
			Path privRsrcLocalPath = new Path("/some/local/dir/for/privrsrc");
			stateStore.StartResourceLocalization(user, null, privRsrcProto, privRsrcLocalPath
				);
			// restart and verify resources are marked in-progress
			RestartStateStore();
			state = stateStore.LoadLocalizationState();
			pubts = state.GetPublicTrackerState();
			NUnit.Framework.Assert.IsTrue(pubts.GetLocalizedResources().IsEmpty());
			NUnit.Framework.Assert.AreEqual(2, pubts.GetInProgressResources().Count);
			NUnit.Framework.Assert.AreEqual(pubRsrcLocalPath1, pubts.GetInProgressResources()
				[pubRsrcProto1]);
			NUnit.Framework.Assert.AreEqual(pubRsrcLocalPath2, pubts.GetInProgressResources()
				[pubRsrcProto2]);
			userResources = state.GetUserResources();
			NUnit.Framework.Assert.AreEqual(1, userResources.Count);
			rur = userResources[user];
			privts = rur.GetPrivateTrackerState();
			NUnit.Framework.Assert.IsNotNull(privts);
			NUnit.Framework.Assert.IsTrue(privts.GetLocalizedResources().IsEmpty());
			NUnit.Framework.Assert.AreEqual(1, privts.GetInProgressResources().Count);
			NUnit.Framework.Assert.AreEqual(privRsrcLocalPath, privts.GetInProgressResources(
				)[privRsrcProto]);
			NUnit.Framework.Assert.AreEqual(1, rur.GetAppTrackerStates().Count);
			appts = rur.GetAppTrackerStates()[appId];
			NUnit.Framework.Assert.IsNotNull(appts);
			NUnit.Framework.Assert.IsTrue(appts.GetLocalizedResources().IsEmpty());
			NUnit.Framework.Assert.AreEqual(1, appts.GetInProgressResources().Count);
			NUnit.Framework.Assert.AreEqual(appRsrcLocalPath, appts.GetInProgressResources()[
				appRsrcProto]);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFinishResourceLocalization()
		{
			string user = "somebody";
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			// start and finish a local resource for an application
			Path appRsrcPath = new Path("hdfs://some/app/resource");
			LocalResourcePBImpl rsrcPb = (LocalResourcePBImpl)LocalResource.NewInstance(ConverterUtils
				.GetYarnUrlFromPath(appRsrcPath), LocalResourceType.Archive, LocalResourceVisibility
				.Application, 123L, 456L);
			YarnProtos.LocalResourceProto appRsrcProto = rsrcPb.GetProto();
			Path appRsrcLocalPath = new Path("/some/local/dir/for/apprsrc");
			stateStore.StartResourceLocalization(user, appId, appRsrcProto, appRsrcLocalPath);
			YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto appLocalizedProto = ((
				YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto)YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto
				.NewBuilder().SetResource(appRsrcProto).SetLocalPath(appRsrcLocalPath.ToString()
				).SetSize(1234567L).Build());
			stateStore.FinishResourceLocalization(user, appId, appLocalizedProto);
			// restart and verify only app resource is completed
			RestartStateStore();
			NMStateStoreService.RecoveredLocalizationState state = stateStore.LoadLocalizationState
				();
			NMStateStoreService.LocalResourceTrackerState pubts = state.GetPublicTrackerState
				();
			NUnit.Framework.Assert.IsTrue(pubts.GetLocalizedResources().IsEmpty());
			NUnit.Framework.Assert.IsTrue(pubts.GetInProgressResources().IsEmpty());
			IDictionary<string, NMStateStoreService.RecoveredUserResources> userResources = state
				.GetUserResources();
			NUnit.Framework.Assert.AreEqual(1, userResources.Count);
			NMStateStoreService.RecoveredUserResources rur = userResources[user];
			NMStateStoreService.LocalResourceTrackerState privts = rur.GetPrivateTrackerState
				();
			NUnit.Framework.Assert.IsNotNull(privts);
			NUnit.Framework.Assert.IsTrue(privts.GetLocalizedResources().IsEmpty());
			NUnit.Framework.Assert.IsTrue(privts.GetInProgressResources().IsEmpty());
			NUnit.Framework.Assert.AreEqual(1, rur.GetAppTrackerStates().Count);
			NMStateStoreService.LocalResourceTrackerState appts = rur.GetAppTrackerStates()[appId
				];
			NUnit.Framework.Assert.IsNotNull(appts);
			NUnit.Framework.Assert.IsTrue(appts.GetInProgressResources().IsEmpty());
			NUnit.Framework.Assert.AreEqual(1, appts.GetLocalizedResources().Count);
			NUnit.Framework.Assert.AreEqual(appLocalizedProto, appts.GetLocalizedResources().
				GetEnumerator().Next());
			// start some public and private resources
			Path pubRsrcPath1 = new Path("hdfs://some/public/resource1");
			rsrcPb = (LocalResourcePBImpl)LocalResource.NewInstance(ConverterUtils.GetYarnUrlFromPath
				(pubRsrcPath1), LocalResourceType.File, LocalResourceVisibility.Public, 789L, 135L
				);
			YarnProtos.LocalResourceProto pubRsrcProto1 = rsrcPb.GetProto();
			Path pubRsrcLocalPath1 = new Path("/some/local/dir/for/pubrsrc1");
			stateStore.StartResourceLocalization(null, null, pubRsrcProto1, pubRsrcLocalPath1
				);
			Path pubRsrcPath2 = new Path("hdfs://some/public/resource2");
			rsrcPb = (LocalResourcePBImpl)LocalResource.NewInstance(ConverterUtils.GetYarnUrlFromPath
				(pubRsrcPath2), LocalResourceType.File, LocalResourceVisibility.Public, 789L, 135L
				);
			YarnProtos.LocalResourceProto pubRsrcProto2 = rsrcPb.GetProto();
			Path pubRsrcLocalPath2 = new Path("/some/local/dir/for/pubrsrc2");
			stateStore.StartResourceLocalization(null, null, pubRsrcProto2, pubRsrcLocalPath2
				);
			Path privRsrcPath = new Path("hdfs://some/private/resource");
			rsrcPb = (LocalResourcePBImpl)LocalResource.NewInstance(ConverterUtils.GetYarnUrlFromPath
				(privRsrcPath), LocalResourceType.Pattern, LocalResourceVisibility.Private, 789L
				, 680L, "*pattern*");
			YarnProtos.LocalResourceProto privRsrcProto = rsrcPb.GetProto();
			Path privRsrcLocalPath = new Path("/some/local/dir/for/privrsrc");
			stateStore.StartResourceLocalization(user, null, privRsrcProto, privRsrcLocalPath
				);
			// finish some of the resources
			YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto pubLocalizedProto1 = (
				(YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto)YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto
				.NewBuilder().SetResource(pubRsrcProto1).SetLocalPath(pubRsrcLocalPath1.ToString
				()).SetSize(pubRsrcProto1.GetSize()).Build());
			stateStore.FinishResourceLocalization(null, null, pubLocalizedProto1);
			YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto privLocalizedProto = (
				(YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto)YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto
				.NewBuilder().SetResource(privRsrcProto).SetLocalPath(privRsrcLocalPath.ToString
				()).SetSize(privRsrcProto.GetSize()).Build());
			stateStore.FinishResourceLocalization(user, null, privLocalizedProto);
			// restart and verify state
			RestartStateStore();
			state = stateStore.LoadLocalizationState();
			pubts = state.GetPublicTrackerState();
			NUnit.Framework.Assert.AreEqual(1, pubts.GetLocalizedResources().Count);
			NUnit.Framework.Assert.AreEqual(pubLocalizedProto1, pubts.GetLocalizedResources()
				.GetEnumerator().Next());
			NUnit.Framework.Assert.AreEqual(1, pubts.GetInProgressResources().Count);
			NUnit.Framework.Assert.AreEqual(pubRsrcLocalPath2, pubts.GetInProgressResources()
				[pubRsrcProto2]);
			userResources = state.GetUserResources();
			NUnit.Framework.Assert.AreEqual(1, userResources.Count);
			rur = userResources[user];
			privts = rur.GetPrivateTrackerState();
			NUnit.Framework.Assert.IsNotNull(privts);
			NUnit.Framework.Assert.AreEqual(1, privts.GetLocalizedResources().Count);
			NUnit.Framework.Assert.AreEqual(privLocalizedProto, privts.GetLocalizedResources(
				).GetEnumerator().Next());
			NUnit.Framework.Assert.IsTrue(privts.GetInProgressResources().IsEmpty());
			NUnit.Framework.Assert.AreEqual(1, rur.GetAppTrackerStates().Count);
			appts = rur.GetAppTrackerStates()[appId];
			NUnit.Framework.Assert.IsNotNull(appts);
			NUnit.Framework.Assert.IsTrue(appts.GetInProgressResources().IsEmpty());
			NUnit.Framework.Assert.AreEqual(1, appts.GetLocalizedResources().Count);
			NUnit.Framework.Assert.AreEqual(appLocalizedProto, appts.GetLocalizedResources().
				GetEnumerator().Next());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveLocalizedResource()
		{
			string user = "somebody";
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			// go through the complete lifecycle for an application local resource
			Path appRsrcPath = new Path("hdfs://some/app/resource");
			LocalResourcePBImpl rsrcPb = (LocalResourcePBImpl)LocalResource.NewInstance(ConverterUtils
				.GetYarnUrlFromPath(appRsrcPath), LocalResourceType.Archive, LocalResourceVisibility
				.Application, 123L, 456L);
			YarnProtos.LocalResourceProto appRsrcProto = rsrcPb.GetProto();
			Path appRsrcLocalPath = new Path("/some/local/dir/for/apprsrc");
			stateStore.StartResourceLocalization(user, appId, appRsrcProto, appRsrcLocalPath);
			YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto appLocalizedProto = ((
				YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto)YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto
				.NewBuilder().SetResource(appRsrcProto).SetLocalPath(appRsrcLocalPath.ToString()
				).SetSize(1234567L).Build());
			stateStore.FinishResourceLocalization(user, appId, appLocalizedProto);
			stateStore.RemoveLocalizedResource(user, appId, appRsrcLocalPath);
			RestartStateStore();
			VerifyEmptyState();
			// remove an app resource that didn't finish
			stateStore.StartResourceLocalization(user, appId, appRsrcProto, appRsrcLocalPath);
			stateStore.RemoveLocalizedResource(user, appId, appRsrcLocalPath);
			RestartStateStore();
			VerifyEmptyState();
			// add public and private resources and remove some
			Path pubRsrcPath1 = new Path("hdfs://some/public/resource1");
			rsrcPb = (LocalResourcePBImpl)LocalResource.NewInstance(ConverterUtils.GetYarnUrlFromPath
				(pubRsrcPath1), LocalResourceType.File, LocalResourceVisibility.Public, 789L, 135L
				);
			YarnProtos.LocalResourceProto pubRsrcProto1 = rsrcPb.GetProto();
			Path pubRsrcLocalPath1 = new Path("/some/local/dir/for/pubrsrc1");
			stateStore.StartResourceLocalization(null, null, pubRsrcProto1, pubRsrcLocalPath1
				);
			YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto pubLocalizedProto1 = (
				(YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto)YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto
				.NewBuilder().SetResource(pubRsrcProto1).SetLocalPath(pubRsrcLocalPath1.ToString
				()).SetSize(789L).Build());
			stateStore.FinishResourceLocalization(null, null, pubLocalizedProto1);
			Path pubRsrcPath2 = new Path("hdfs://some/public/resource2");
			rsrcPb = (LocalResourcePBImpl)LocalResource.NewInstance(ConverterUtils.GetYarnUrlFromPath
				(pubRsrcPath2), LocalResourceType.File, LocalResourceVisibility.Public, 789L, 135L
				);
			YarnProtos.LocalResourceProto pubRsrcProto2 = rsrcPb.GetProto();
			Path pubRsrcLocalPath2 = new Path("/some/local/dir/for/pubrsrc2");
			stateStore.StartResourceLocalization(null, null, pubRsrcProto2, pubRsrcLocalPath2
				);
			YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto pubLocalizedProto2 = (
				(YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto)YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto
				.NewBuilder().SetResource(pubRsrcProto2).SetLocalPath(pubRsrcLocalPath2.ToString
				()).SetSize(7654321L).Build());
			stateStore.FinishResourceLocalization(null, null, pubLocalizedProto2);
			stateStore.RemoveLocalizedResource(null, null, pubRsrcLocalPath2);
			Path privRsrcPath = new Path("hdfs://some/private/resource");
			rsrcPb = (LocalResourcePBImpl)LocalResource.NewInstance(ConverterUtils.GetYarnUrlFromPath
				(privRsrcPath), LocalResourceType.Pattern, LocalResourceVisibility.Private, 789L
				, 680L, "*pattern*");
			YarnProtos.LocalResourceProto privRsrcProto = rsrcPb.GetProto();
			Path privRsrcLocalPath = new Path("/some/local/dir/for/privrsrc");
			stateStore.StartResourceLocalization(user, null, privRsrcProto, privRsrcLocalPath
				);
			stateStore.RemoveLocalizedResource(user, null, privRsrcLocalPath);
			// restart and verify state
			RestartStateStore();
			NMStateStoreService.RecoveredLocalizationState state = stateStore.LoadLocalizationState
				();
			NMStateStoreService.LocalResourceTrackerState pubts = state.GetPublicTrackerState
				();
			NUnit.Framework.Assert.IsTrue(pubts.GetInProgressResources().IsEmpty());
			NUnit.Framework.Assert.AreEqual(1, pubts.GetLocalizedResources().Count);
			NUnit.Framework.Assert.AreEqual(pubLocalizedProto1, pubts.GetLocalizedResources()
				.GetEnumerator().Next());
			IDictionary<string, NMStateStoreService.RecoveredUserResources> userResources = state
				.GetUserResources();
			NUnit.Framework.Assert.IsTrue(userResources.IsEmpty());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDeletionTaskStorage()
		{
			// test empty when no state
			NMStateStoreService.RecoveredDeletionServiceState state = stateStore.LoadDeletionServiceState
				();
			NUnit.Framework.Assert.IsTrue(state.GetTasks().IsEmpty());
			// store a deletion task and verify recovered
			YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto proto = ((YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
				)YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto.NewBuilder()
				.SetId(7).SetUser("someuser").SetSubdir("some/subdir").AddBasedirs("some/dir/path"
				).AddBasedirs("some/other/dir/path").SetDeletionTime(123456L).AddSuccessorIds(8)
				.AddSuccessorIds(9).Build());
			stateStore.StoreDeletionTask(proto.GetId(), proto);
			RestartStateStore();
			state = stateStore.LoadDeletionServiceState();
			NUnit.Framework.Assert.AreEqual(1, state.GetTasks().Count);
			NUnit.Framework.Assert.AreEqual(proto, state.GetTasks()[0]);
			// store another deletion task
			YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto proto2 = ((YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
				)YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto.NewBuilder()
				.SetId(8).SetUser("user2").SetSubdir("subdir2").SetDeletionTime(789L).Build());
			stateStore.StoreDeletionTask(proto2.GetId(), proto2);
			RestartStateStore();
			state = stateStore.LoadDeletionServiceState();
			NUnit.Framework.Assert.AreEqual(2, state.GetTasks().Count);
			NUnit.Framework.Assert.IsTrue(state.GetTasks().Contains(proto));
			NUnit.Framework.Assert.IsTrue(state.GetTasks().Contains(proto2));
			// delete a task and verify gone after recovery
			stateStore.RemoveDeletionTask(proto2.GetId());
			RestartStateStore();
			state = stateStore.LoadDeletionServiceState();
			NUnit.Framework.Assert.AreEqual(1, state.GetTasks().Count);
			NUnit.Framework.Assert.AreEqual(proto, state.GetTasks()[0]);
			// delete the last task and verify none left
			stateStore.RemoveDeletionTask(proto.GetId());
			RestartStateStore();
			state = stateStore.LoadDeletionServiceState();
			NUnit.Framework.Assert.IsTrue(state.GetTasks().IsEmpty());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNMTokenStorage()
		{
			// test empty when no state
			NMStateStoreService.RecoveredNMTokensState state = stateStore.LoadNMTokensState();
			NUnit.Framework.Assert.IsNull(state.GetCurrentMasterKey());
			NUnit.Framework.Assert.IsNull(state.GetPreviousMasterKey());
			NUnit.Framework.Assert.IsTrue(state.GetApplicationMasterKeys().IsEmpty());
			// store a master key and verify recovered
			TestNMLeveldbStateStoreService.NMTokenSecretManagerForTest secretMgr = new TestNMLeveldbStateStoreService.NMTokenSecretManagerForTest
				();
			MasterKey currentKey = secretMgr.GenerateKey();
			stateStore.StoreNMTokenCurrentMasterKey(currentKey);
			RestartStateStore();
			state = stateStore.LoadNMTokensState();
			NUnit.Framework.Assert.AreEqual(currentKey, state.GetCurrentMasterKey());
			NUnit.Framework.Assert.IsNull(state.GetPreviousMasterKey());
			NUnit.Framework.Assert.IsTrue(state.GetApplicationMasterKeys().IsEmpty());
			// store a previous key and verify recovered
			MasterKey prevKey = secretMgr.GenerateKey();
			stateStore.StoreNMTokenPreviousMasterKey(prevKey);
			RestartStateStore();
			state = stateStore.LoadNMTokensState();
			NUnit.Framework.Assert.AreEqual(currentKey, state.GetCurrentMasterKey());
			NUnit.Framework.Assert.AreEqual(prevKey, state.GetPreviousMasterKey());
			NUnit.Framework.Assert.IsTrue(state.GetApplicationMasterKeys().IsEmpty());
			// store a few application keys and verify recovered
			ApplicationAttemptId attempt1 = ApplicationAttemptId.NewInstance(ApplicationId.NewInstance
				(1, 1), 1);
			MasterKey attemptKey1 = secretMgr.GenerateKey();
			stateStore.StoreNMTokenApplicationMasterKey(attempt1, attemptKey1);
			ApplicationAttemptId attempt2 = ApplicationAttemptId.NewInstance(ApplicationId.NewInstance
				(2, 3), 4);
			MasterKey attemptKey2 = secretMgr.GenerateKey();
			stateStore.StoreNMTokenApplicationMasterKey(attempt2, attemptKey2);
			RestartStateStore();
			state = stateStore.LoadNMTokensState();
			NUnit.Framework.Assert.AreEqual(currentKey, state.GetCurrentMasterKey());
			NUnit.Framework.Assert.AreEqual(prevKey, state.GetPreviousMasterKey());
			IDictionary<ApplicationAttemptId, MasterKey> loadedAppKeys = state.GetApplicationMasterKeys
				();
			NUnit.Framework.Assert.AreEqual(2, loadedAppKeys.Count);
			NUnit.Framework.Assert.AreEqual(attemptKey1, loadedAppKeys[attempt1]);
			NUnit.Framework.Assert.AreEqual(attemptKey2, loadedAppKeys[attempt2]);
			// add/update/remove keys and verify recovered
			ApplicationAttemptId attempt3 = ApplicationAttemptId.NewInstance(ApplicationId.NewInstance
				(5, 6), 7);
			MasterKey attemptKey3 = secretMgr.GenerateKey();
			stateStore.StoreNMTokenApplicationMasterKey(attempt3, attemptKey3);
			stateStore.RemoveNMTokenApplicationMasterKey(attempt1);
			attemptKey2 = prevKey;
			stateStore.StoreNMTokenApplicationMasterKey(attempt2, attemptKey2);
			prevKey = currentKey;
			stateStore.StoreNMTokenPreviousMasterKey(prevKey);
			currentKey = secretMgr.GenerateKey();
			stateStore.StoreNMTokenCurrentMasterKey(currentKey);
			RestartStateStore();
			state = stateStore.LoadNMTokensState();
			NUnit.Framework.Assert.AreEqual(currentKey, state.GetCurrentMasterKey());
			NUnit.Framework.Assert.AreEqual(prevKey, state.GetPreviousMasterKey());
			loadedAppKeys = state.GetApplicationMasterKeys();
			NUnit.Framework.Assert.AreEqual(2, loadedAppKeys.Count);
			NUnit.Framework.Assert.IsNull(loadedAppKeys[attempt1]);
			NUnit.Framework.Assert.AreEqual(attemptKey2, loadedAppKeys[attempt2]);
			NUnit.Framework.Assert.AreEqual(attemptKey3, loadedAppKeys[attempt3]);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerTokenStorage()
		{
			// test empty when no state
			NMStateStoreService.RecoveredContainerTokensState state = stateStore.LoadContainerTokensState
				();
			NUnit.Framework.Assert.IsNull(state.GetCurrentMasterKey());
			NUnit.Framework.Assert.IsNull(state.GetPreviousMasterKey());
			NUnit.Framework.Assert.IsTrue(state.GetActiveTokens().IsEmpty());
			// store a master key and verify recovered
			TestNMLeveldbStateStoreService.ContainerTokenKeyGeneratorForTest keygen = new TestNMLeveldbStateStoreService.ContainerTokenKeyGeneratorForTest
				(new YarnConfiguration());
			MasterKey currentKey = keygen.GenerateKey();
			stateStore.StoreContainerTokenCurrentMasterKey(currentKey);
			RestartStateStore();
			state = stateStore.LoadContainerTokensState();
			NUnit.Framework.Assert.AreEqual(currentKey, state.GetCurrentMasterKey());
			NUnit.Framework.Assert.IsNull(state.GetPreviousMasterKey());
			NUnit.Framework.Assert.IsTrue(state.GetActiveTokens().IsEmpty());
			// store a previous key and verify recovered
			MasterKey prevKey = keygen.GenerateKey();
			stateStore.StoreContainerTokenPreviousMasterKey(prevKey);
			RestartStateStore();
			state = stateStore.LoadContainerTokensState();
			NUnit.Framework.Assert.AreEqual(currentKey, state.GetCurrentMasterKey());
			NUnit.Framework.Assert.AreEqual(prevKey, state.GetPreviousMasterKey());
			NUnit.Framework.Assert.IsTrue(state.GetActiveTokens().IsEmpty());
			// store a few container tokens and verify recovered
			ContainerId cid1 = BuilderUtils.NewContainerId(1, 1, 1, 1);
			long expTime1 = 1234567890L;
			ContainerId cid2 = BuilderUtils.NewContainerId(2, 2, 2, 2);
			long expTime2 = 9876543210L;
			stateStore.StoreContainerToken(cid1, expTime1);
			stateStore.StoreContainerToken(cid2, expTime2);
			RestartStateStore();
			state = stateStore.LoadContainerTokensState();
			NUnit.Framework.Assert.AreEqual(currentKey, state.GetCurrentMasterKey());
			NUnit.Framework.Assert.AreEqual(prevKey, state.GetPreviousMasterKey());
			IDictionary<ContainerId, long> loadedActiveTokens = state.GetActiveTokens();
			NUnit.Framework.Assert.AreEqual(2, loadedActiveTokens.Count);
			NUnit.Framework.Assert.AreEqual(expTime1, loadedActiveTokens[cid1]);
			NUnit.Framework.Assert.AreEqual(expTime2, loadedActiveTokens[cid2]);
			// add/update/remove tokens and verify recovered
			ContainerId cid3 = BuilderUtils.NewContainerId(3, 3, 3, 3);
			long expTime3 = 135798642L;
			stateStore.StoreContainerToken(cid3, expTime3);
			stateStore.RemoveContainerToken(cid1);
			expTime2 += 246897531L;
			stateStore.StoreContainerToken(cid2, expTime2);
			prevKey = currentKey;
			stateStore.StoreContainerTokenPreviousMasterKey(prevKey);
			currentKey = keygen.GenerateKey();
			stateStore.StoreContainerTokenCurrentMasterKey(currentKey);
			RestartStateStore();
			state = stateStore.LoadContainerTokensState();
			NUnit.Framework.Assert.AreEqual(currentKey, state.GetCurrentMasterKey());
			NUnit.Framework.Assert.AreEqual(prevKey, state.GetPreviousMasterKey());
			loadedActiveTokens = state.GetActiveTokens();
			NUnit.Framework.Assert.AreEqual(2, loadedActiveTokens.Count);
			NUnit.Framework.Assert.IsNull(loadedActiveTokens[cid1]);
			NUnit.Framework.Assert.AreEqual(expTime2, loadedActiveTokens[cid2]);
			NUnit.Framework.Assert.AreEqual(expTime3, loadedActiveTokens[cid3]);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLogDeleterStorage()
		{
			// test empty when no state
			NMStateStoreService.RecoveredLogDeleterState state = stateStore.LoadLogDeleterState
				();
			NUnit.Framework.Assert.IsTrue(state.GetLogDeleterMap().IsEmpty());
			// store log deleter state
			ApplicationId appId1 = ApplicationId.NewInstance(1, 1);
			YarnServerNodemanagerRecoveryProtos.LogDeleterProto proto1 = ((YarnServerNodemanagerRecoveryProtos.LogDeleterProto
				)YarnServerNodemanagerRecoveryProtos.LogDeleterProto.NewBuilder().SetUser("user1"
				).SetDeletionTime(1234).Build());
			stateStore.StoreLogDeleter(appId1, proto1);
			// restart state store and verify recovered
			RestartStateStore();
			state = stateStore.LoadLogDeleterState();
			NUnit.Framework.Assert.AreEqual(1, state.GetLogDeleterMap().Count);
			NUnit.Framework.Assert.AreEqual(proto1, state.GetLogDeleterMap()[appId1]);
			// store another log deleter
			ApplicationId appId2 = ApplicationId.NewInstance(2, 2);
			YarnServerNodemanagerRecoveryProtos.LogDeleterProto proto2 = ((YarnServerNodemanagerRecoveryProtos.LogDeleterProto
				)YarnServerNodemanagerRecoveryProtos.LogDeleterProto.NewBuilder().SetUser("user2"
				).SetDeletionTime(5678).Build());
			stateStore.StoreLogDeleter(appId2, proto2);
			// restart state store and verify recovered
			RestartStateStore();
			state = stateStore.LoadLogDeleterState();
			NUnit.Framework.Assert.AreEqual(2, state.GetLogDeleterMap().Count);
			NUnit.Framework.Assert.AreEqual(proto1, state.GetLogDeleterMap()[appId1]);
			NUnit.Framework.Assert.AreEqual(proto2, state.GetLogDeleterMap()[appId2]);
			// remove a deleter and verify removed after restart and recovery
			stateStore.RemoveLogDeleter(appId1);
			RestartStateStore();
			state = stateStore.LoadLogDeleterState();
			NUnit.Framework.Assert.AreEqual(1, state.GetLogDeleterMap().Count);
			NUnit.Framework.Assert.AreEqual(proto2, state.GetLogDeleterMap()[appId2]);
			// remove last deleter and verify empty after restart and recovery
			stateStore.RemoveLogDeleter(appId2);
			RestartStateStore();
			state = stateStore.LoadLogDeleterState();
			NUnit.Framework.Assert.IsTrue(state.GetLogDeleterMap().IsEmpty());
		}

		private class NMTokenSecretManagerForTest : BaseNMTokenSecretManager
		{
			public virtual MasterKey GenerateKey()
			{
				return CreateNewMasterKey().GetMasterKey();
			}
		}

		private class ContainerTokenKeyGeneratorForTest : BaseContainerTokenSecretManager
		{
			public ContainerTokenKeyGeneratorForTest(Configuration conf)
				: base(conf)
			{
			}

			public virtual MasterKey GenerateKey()
			{
				return CreateNewMasterKey().GetMasterKey();
			}
		}
	}
}
