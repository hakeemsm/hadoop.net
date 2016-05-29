using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestApplicationMasterService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestFifoScheduler));

		private readonly int Gb = 1024;

		private static YarnConfiguration conf;

		[BeforeClass]
		public static void Setup()
		{
			conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(FifoScheduler), typeof(ResourceScheduler
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMIdentifierOnContainerAllocation()
		{
			MockRM rm = new MockRM(conf);
			rm.Start();
			// Register node1
			MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 6 * Gb);
			// Submit an application
			RMApp app1 = rm.SubmitApp(2048);
			// kick the scheduling
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			MockAM am1 = rm.SendAMLaunched(attempt1.GetAppAttemptId());
			am1.RegisterAppAttempt();
			am1.AddRequests(new string[] { "127.0.0.1" }, Gb, 1, 1);
			AllocateResponse alloc1Response = am1.Schedule();
			// send the request
			// kick the scheduler
			nm1.NodeHeartbeat(true);
			while (alloc1Response.GetAllocatedContainers().Count < 1)
			{
				Log.Info("Waiting for containers to be created for app 1...");
				Sharpen.Thread.Sleep(1000);
				alloc1Response = am1.Schedule();
			}
			// assert RMIdentifer is set properly in allocated containers
			Container allocatedContainer = alloc1Response.GetAllocatedContainers()[0];
			ContainerTokenIdentifier tokenId = BuilderUtils.NewContainerTokenIdentifier(allocatedContainer
				.GetContainerToken());
			NUnit.Framework.Assert.AreEqual(MockRM.GetClusterTimeStamp(), tokenId.GetRMIdentifier
				());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestInvalidContainerReleaseRequest()
		{
			MockRM rm = new MockRM(conf);
			try
			{
				rm.Start();
				// Register node1
				MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 6 * Gb);
				// Submit an application
				RMApp app1 = rm.SubmitApp(1024);
				// kick the scheduling
				nm1.NodeHeartbeat(true);
				RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
				MockAM am1 = rm.SendAMLaunched(attempt1.GetAppAttemptId());
				am1.RegisterAppAttempt();
				am1.AddRequests(new string[] { "127.0.0.1" }, Gb, 1, 1);
				AllocateResponse alloc1Response = am1.Schedule();
				// send the request
				// kick the scheduler
				nm1.NodeHeartbeat(true);
				while (alloc1Response.GetAllocatedContainers().Count < 1)
				{
					Log.Info("Waiting for containers to be created for app 1...");
					Sharpen.Thread.Sleep(1000);
					alloc1Response = am1.Schedule();
				}
				NUnit.Framework.Assert.IsTrue(alloc1Response.GetAllocatedContainers().Count > 0);
				RMApp app2 = rm.SubmitApp(1024);
				nm1.NodeHeartbeat(true);
				RMAppAttempt attempt2 = app2.GetCurrentAppAttempt();
				MockAM am2 = rm.SendAMLaunched(attempt2.GetAppAttemptId());
				am2.RegisterAppAttempt();
				// Now trying to release container allocated for app1 -> appAttempt1.
				ContainerId cId = alloc1Response.GetAllocatedContainers()[0].GetId();
				am2.AddContainerToBeReleased(cId);
				try
				{
					am2.Schedule();
					NUnit.Framework.Assert.Fail("Exception was expected!!");
				}
				catch (InvalidContainerReleaseException e)
				{
					StringBuilder sb = new StringBuilder("Cannot release container : ");
					sb.Append(cId.ToString());
					sb.Append(" not belonging to this application attempt : ");
					sb.Append(attempt2.GetAppAttemptId().ToString());
					NUnit.Framework.Assert.IsTrue(e.Message.Contains(sb.ToString()));
				}
			}
			finally
			{
				if (rm != null)
				{
					rm.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestProgressFilter()
		{
			MockRM rm = new MockRM(conf);
			rm.Start();
			// Register node1
			MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 6 * Gb);
			// Submit an application
			RMApp app1 = rm.SubmitApp(2048);
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			MockAM am1 = rm.SendAMLaunched(attempt1.GetAppAttemptId());
			am1.RegisterAppAttempt();
			AllocateRequestPBImpl allocateRequest = new AllocateRequestPBImpl();
			IList<ContainerId> release = new AList<ContainerId>();
			IList<ResourceRequest> ask = new AList<ResourceRequest>();
			allocateRequest.SetReleaseList(release);
			allocateRequest.SetAskList(ask);
			allocateRequest.SetProgress(float.PositiveInfinity);
			am1.Allocate(allocateRequest);
			while (attempt1.GetProgress() != 1)
			{
				Log.Info("Waiting for allocate event to be handled ...");
				Sharpen.Thread.Sleep(100);
			}
			allocateRequest.SetProgress(float.NaN);
			am1.Allocate(allocateRequest);
			while (attempt1.GetProgress() != 0)
			{
				Log.Info("Waiting for allocate event to be handled ...");
				Sharpen.Thread.Sleep(100);
			}
			allocateRequest.SetProgress((float)9);
			am1.Allocate(allocateRequest);
			while (attempt1.GetProgress() != 1)
			{
				Log.Info("Waiting for allocate event to be handled ...");
				Sharpen.Thread.Sleep(100);
			}
			allocateRequest.SetProgress(float.NegativeInfinity);
			am1.Allocate(allocateRequest);
			while (attempt1.GetProgress() != 0)
			{
				Log.Info("Waiting for allocate event to be handled ...");
				Sharpen.Thread.Sleep(100);
			}
			allocateRequest.SetProgress((float)0.5);
			am1.Allocate(allocateRequest);
			while (attempt1.GetProgress() != 0.5)
			{
				Log.Info("Waiting for allocate event to be handled ...");
				Sharpen.Thread.Sleep(100);
			}
			allocateRequest.SetProgress((float)-1);
			am1.Allocate(allocateRequest);
			while (attempt1.GetProgress() != 0)
			{
				Log.Info("Waiting for allocate event to be handled ...");
				Sharpen.Thread.Sleep(100);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFinishApplicationMasterBeforeRegistering()
		{
			MockRM rm = new MockRM(conf);
			try
			{
				rm.Start();
				// Register node1
				MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 6 * Gb);
				// Submit an application
				RMApp app1 = rm.SubmitApp(2048);
				MockAM am1 = MockRM.LaunchAM(app1, rm, nm1);
				FinishApplicationMasterRequest req = FinishApplicationMasterRequest.NewInstance(FinalApplicationStatus
					.Failed, string.Empty, string.Empty);
				try
				{
					am1.UnregisterAppAttempt(req, false);
					NUnit.Framework.Assert.Fail("ApplicationMasterNotRegisteredException should be thrown"
						);
				}
				catch (ApplicationMasterNotRegisteredException e)
				{
					NUnit.Framework.Assert.IsNotNull(e);
					NUnit.Framework.Assert.IsNotNull(e.Message);
					NUnit.Framework.Assert.IsTrue(e.Message.Contains("Application Master is trying to unregister before registering for:"
						));
				}
				catch (Exception)
				{
					NUnit.Framework.Assert.Fail("ApplicationMasterNotRegisteredException should be thrown"
						);
				}
				am1.RegisterAppAttempt();
				am1.UnregisterAppAttempt(req, false);
				am1.WaitForState(RMAppAttemptState.Finishing);
			}
			finally
			{
				if (rm != null)
				{
					rm.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestResourceTypes()
		{
			Dictionary<YarnConfiguration, EnumSet<YarnServiceProtos.SchedulerResourceTypes>> 
				driver = new Dictionary<YarnConfiguration, EnumSet<YarnServiceProtos.SchedulerResourceTypes
				>>();
			CapacitySchedulerConfiguration csconf = new CapacitySchedulerConfiguration();
			csconf.SetResourceComparator(typeof(DominantResourceCalculator));
			YarnConfiguration testCapacityDRConf = new YarnConfiguration(csconf);
			testCapacityDRConf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler
				), typeof(ResourceScheduler));
			YarnConfiguration testCapacityDefConf = new YarnConfiguration();
			testCapacityDefConf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler
				), typeof(ResourceScheduler));
			YarnConfiguration testFairDefConf = new YarnConfiguration();
			testFairDefConf.SetClass(YarnConfiguration.RmScheduler, typeof(FairScheduler), typeof(
				ResourceScheduler));
			driver[conf] = EnumSet.Of(YarnServiceProtos.SchedulerResourceTypes.Memory);
			driver[testCapacityDRConf] = EnumSet.Of(YarnServiceProtos.SchedulerResourceTypes.
				Cpu, YarnServiceProtos.SchedulerResourceTypes.Memory);
			driver[testCapacityDefConf] = EnumSet.Of(YarnServiceProtos.SchedulerResourceTypes
				.Memory);
			driver[testFairDefConf] = EnumSet.Of(YarnServiceProtos.SchedulerResourceTypes.Memory
				, YarnServiceProtos.SchedulerResourceTypes.Cpu);
			foreach (KeyValuePair<YarnConfiguration, EnumSet<YarnServiceProtos.SchedulerResourceTypes
				>> entry in driver)
			{
				EnumSet<YarnServiceProtos.SchedulerResourceTypes> expectedValue = entry.Value;
				MockRM rm = new MockRM(entry.Key);
				rm.Start();
				MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 6 * Gb);
				RMApp app1 = rm.SubmitApp(2048);
				nm1.NodeHeartbeat(true);
				RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
				MockAM am1 = rm.SendAMLaunched(attempt1.GetAppAttemptId());
				RegisterApplicationMasterResponse resp = am1.RegisterAppAttempt();
				EnumSet<YarnServiceProtos.SchedulerResourceTypes> types = resp.GetSchedulerResourceTypes
					();
				Log.Info("types = " + types.ToString());
				NUnit.Framework.Assert.AreEqual(expectedValue, types);
				rm.Stop();
			}
		}
	}
}
