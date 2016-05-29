using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	public class TestSchedulerUtils
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestSchedulerUtils));

		private RMContext rmContext = GetMockRMContext();

		public virtual void TestNormalizeRequest()
		{
			ResourceCalculator resourceCalculator = new DefaultResourceCalculator();
			int minMemory = 1024;
			int maxMemory = 8192;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource minResource = Resources.CreateResource
				(minMemory, 0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource maxResource = Resources.CreateResource
				(maxMemory, 0);
			ResourceRequest ask = new ResourceRequestPBImpl();
			// case negative memory
			ask.SetCapability(Resources.CreateResource(-1024));
			SchedulerUtils.NormalizeRequest(ask, resourceCalculator, null, minResource, maxResource
				);
			NUnit.Framework.Assert.AreEqual(minMemory, ask.GetCapability().GetMemory());
			// case zero memory
			ask.SetCapability(Resources.CreateResource(0));
			SchedulerUtils.NormalizeRequest(ask, resourceCalculator, null, minResource, maxResource
				);
			NUnit.Framework.Assert.AreEqual(minMemory, ask.GetCapability().GetMemory());
			// case memory is a multiple of minMemory
			ask.SetCapability(Resources.CreateResource(2 * minMemory));
			SchedulerUtils.NormalizeRequest(ask, resourceCalculator, null, minResource, maxResource
				);
			NUnit.Framework.Assert.AreEqual(2 * minMemory, ask.GetCapability().GetMemory());
			// case memory is not a multiple of minMemory
			ask.SetCapability(Resources.CreateResource(minMemory + 10));
			SchedulerUtils.NormalizeRequest(ask, resourceCalculator, null, minResource, maxResource
				);
			NUnit.Framework.Assert.AreEqual(2 * minMemory, ask.GetCapability().GetMemory());
			// case memory is equal to max allowed
			ask.SetCapability(Resources.CreateResource(maxMemory));
			SchedulerUtils.NormalizeRequest(ask, resourceCalculator, null, minResource, maxResource
				);
			NUnit.Framework.Assert.AreEqual(maxMemory, ask.GetCapability().GetMemory());
			// case memory is just less than max
			ask.SetCapability(Resources.CreateResource(maxMemory - 10));
			SchedulerUtils.NormalizeRequest(ask, resourceCalculator, null, minResource, maxResource
				);
			NUnit.Framework.Assert.AreEqual(maxMemory, ask.GetCapability().GetMemory());
			// max is not a multiple of min
			maxResource = Resources.CreateResource(maxMemory - 10, 0);
			ask.SetCapability(Resources.CreateResource(maxMemory - 100));
			// multiple of minMemory > maxMemory, then reduce to maxMemory
			SchedulerUtils.NormalizeRequest(ask, resourceCalculator, null, minResource, maxResource
				);
			NUnit.Framework.Assert.AreEqual(maxResource.GetMemory(), ask.GetCapability().GetMemory
				());
			// ask is more than max
			maxResource = Resources.CreateResource(maxMemory, 0);
			ask.SetCapability(Resources.CreateResource(maxMemory + 100));
			SchedulerUtils.NormalizeRequest(ask, resourceCalculator, null, minResource, maxResource
				);
			NUnit.Framework.Assert.AreEqual(maxResource.GetMemory(), ask.GetCapability().GetMemory
				());
		}

		public virtual void TestNormalizeRequestWithDominantResourceCalculator()
		{
			ResourceCalculator resourceCalculator = new DominantResourceCalculator();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource minResource = Resources.CreateResource
				(1024, 1);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource maxResource = Resources.CreateResource
				(10240, 10);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(10 * 1024, 10);
			ResourceRequest ask = new ResourceRequestPBImpl();
			// case negative memory/vcores
			ask.SetCapability(Resources.CreateResource(-1024, -1));
			SchedulerUtils.NormalizeRequest(ask, resourceCalculator, clusterResource, minResource
				, maxResource);
			NUnit.Framework.Assert.AreEqual(minResource, ask.GetCapability());
			// case zero memory/vcores
			ask.SetCapability(Resources.CreateResource(0, 0));
			SchedulerUtils.NormalizeRequest(ask, resourceCalculator, clusterResource, minResource
				, maxResource);
			NUnit.Framework.Assert.AreEqual(minResource, ask.GetCapability());
			NUnit.Framework.Assert.AreEqual(1, ask.GetCapability().GetVirtualCores());
			NUnit.Framework.Assert.AreEqual(1024, ask.GetCapability().GetMemory());
			// case non-zero memory & zero cores
			ask.SetCapability(Resources.CreateResource(1536, 0));
			SchedulerUtils.NormalizeRequest(ask, resourceCalculator, clusterResource, minResource
				, maxResource);
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(2048, 1), ask.GetCapability
				());
			NUnit.Framework.Assert.AreEqual(1, ask.GetCapability().GetVirtualCores());
			NUnit.Framework.Assert.AreEqual(2048, ask.GetCapability().GetMemory());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestValidateResourceRequestWithErrorLabelsPermission()
		{
			// mock queue and scheduler
			YarnScheduler scheduler = Org.Mockito.Mockito.Mock<YarnScheduler>();
			ICollection<string> queueAccessibleNodeLabels = Sets.NewHashSet();
			QueueInfo queueInfo = Org.Mockito.Mockito.Mock<QueueInfo>();
			Org.Mockito.Mockito.When(queueInfo.GetQueueName()).ThenReturn("queue");
			Org.Mockito.Mockito.When(queueInfo.GetAccessibleNodeLabels()).ThenReturn(queueAccessibleNodeLabels
				);
			Org.Mockito.Mockito.When(scheduler.GetQueueInfo(Matchers.Any<string>(), Matchers.AnyBoolean
				(), Matchers.AnyBoolean())).ThenReturn(queueInfo);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource maxResource = Resources.CreateResource
				(YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb, YarnConfiguration.DefaultRmSchedulerMaximumAllocationVcores
				);
			// queue has labels, success cases
			try
			{
				// set queue accessible node labesl to [x, y]
				queueAccessibleNodeLabels.Clear();
				Sharpen.Collections.AddAll(queueAccessibleNodeLabels, Arrays.AsList("x", "y"));
				rmContext.GetNodeLabelManager().AddToCluserNodeLabels(ImmutableSet.Of("x", "y"));
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(0
					, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				resReq.SetNodeLabelExpression("x");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				resReq.SetNodeLabelExpression("y");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				resReq.SetNodeLabelExpression(string.Empty);
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				resReq.SetNodeLabelExpression(" ");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
			}
			catch (InvalidResourceRequestException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Should be valid when request labels is a subset of queue labels"
					);
			}
			finally
			{
				rmContext.GetNodeLabelManager().RemoveFromClusterNodeLabels(Arrays.AsList("x", "y"
					));
			}
			// same as above, but cluster node labels don't contains label being
			// requested. should fail
			try
			{
				// set queue accessible node labesl to [x, y]
				queueAccessibleNodeLabels.Clear();
				Sharpen.Collections.AddAll(queueAccessibleNodeLabels, Arrays.AsList("x", "y"));
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(0
					, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				resReq.SetNodeLabelExpression("x");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (InvalidResourceRequestException)
			{
			}
			// queue has labels, failed cases (when ask a label not included by queue)
			try
			{
				// set queue accessible node labesl to [x, y]
				queueAccessibleNodeLabels.Clear();
				Sharpen.Collections.AddAll(queueAccessibleNodeLabels, Arrays.AsList("x", "y"));
				rmContext.GetNodeLabelManager().AddToCluserNodeLabels(ImmutableSet.Of("x", "y"));
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(0
					, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				resReq.SetNodeLabelExpression("z");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (InvalidResourceRequestException)
			{
			}
			finally
			{
				rmContext.GetNodeLabelManager().RemoveFromClusterNodeLabels(Arrays.AsList("x", "y"
					));
			}
			// we don't allow specify more than two node labels in a single expression
			// now
			try
			{
				// set queue accessible node labesl to [x, y]
				queueAccessibleNodeLabels.Clear();
				Sharpen.Collections.AddAll(queueAccessibleNodeLabels, Arrays.AsList("x", "y"));
				rmContext.GetNodeLabelManager().AddToCluserNodeLabels(ImmutableSet.Of("x", "y"));
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(0
					, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				resReq.SetNodeLabelExpression("x && y");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (InvalidResourceRequestException)
			{
			}
			finally
			{
				rmContext.GetNodeLabelManager().RemoveFromClusterNodeLabels(Arrays.AsList("x", "y"
					));
			}
			// queue doesn't have label, succeed (when request no label)
			queueAccessibleNodeLabels.Clear();
			try
			{
				// set queue accessible node labels to empty
				queueAccessibleNodeLabels.Clear();
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(0
					, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				resReq.SetNodeLabelExpression(string.Empty);
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				resReq.SetNodeLabelExpression("  ");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
			}
			catch (InvalidResourceRequestException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Should be valid when request labels is empty");
			}
			// queue doesn't have label, failed (when request any label)
			try
			{
				// set queue accessible node labels to empty
				queueAccessibleNodeLabels.Clear();
				rmContext.GetNodeLabelManager().AddToCluserNodeLabels(ImmutableSet.Of("x"));
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(0
					, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				resReq.SetNodeLabelExpression("x");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (InvalidResourceRequestException)
			{
			}
			finally
			{
				rmContext.GetNodeLabelManager().RemoveFromClusterNodeLabels(Arrays.AsList("x"));
			}
			// queue is "*", always succeeded
			try
			{
				// set queue accessible node labels to empty
				queueAccessibleNodeLabels.Clear();
				queueAccessibleNodeLabels.AddItem(RMNodeLabelsManager.Any);
				rmContext.GetNodeLabelManager().AddToCluserNodeLabels(ImmutableSet.Of("x", "y", "z"
					));
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(0
					, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				resReq.SetNodeLabelExpression("x");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				resReq.SetNodeLabelExpression("y");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				resReq.SetNodeLabelExpression("z");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
			}
			catch (InvalidResourceRequestException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Should be valid when queue can access any labels");
			}
			finally
			{
				rmContext.GetNodeLabelManager().RemoveFromClusterNodeLabels(Arrays.AsList("x", "y"
					, "z"));
			}
			// same as above, but cluster node labels don't contains label, should fail
			try
			{
				// set queue accessible node labels to empty
				queueAccessibleNodeLabels.Clear();
				queueAccessibleNodeLabels.AddItem(RMNodeLabelsManager.Any);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(0
					, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				resReq.SetNodeLabelExpression("x");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (InvalidResourceRequestException)
			{
			}
			// we don't allow resource name other than ANY and specify label
			try
			{
				// set queue accessible node labesl to [x, y]
				queueAccessibleNodeLabels.Clear();
				Sharpen.Collections.AddAll(queueAccessibleNodeLabels, Arrays.AsList("x", "y"));
				rmContext.GetNodeLabelManager().AddToCluserNodeLabels(ImmutableSet.Of("x", "y"));
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(0
					, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), "rack", resource, 1);
				resReq.SetNodeLabelExpression("x");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (InvalidResourceRequestException)
			{
			}
			finally
			{
				rmContext.GetNodeLabelManager().RemoveFromClusterNodeLabels(Arrays.AsList("x", "y"
					));
			}
			// we don't allow resource name other than ANY and specify label even if
			// queue has accessible label = *
			try
			{
				// set queue accessible node labesl to *
				queueAccessibleNodeLabels.Clear();
				Sharpen.Collections.AddAll(queueAccessibleNodeLabels, Arrays.AsList(CommonNodeLabelsManager
					.Any));
				rmContext.GetNodeLabelManager().AddToCluserNodeLabels(ImmutableSet.Of("x"));
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(0
					, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), "rack", resource, 1);
				resReq.SetNodeLabelExpression("x");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (InvalidResourceRequestException)
			{
			}
			finally
			{
				rmContext.GetNodeLabelManager().RemoveFromClusterNodeLabels(Arrays.AsList("x"));
			}
		}

		public virtual void TestValidateResourceRequest()
		{
			YarnScheduler mockScheduler = Org.Mockito.Mockito.Mock<YarnScheduler>();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource maxResource = Resources.CreateResource
				(YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb, YarnConfiguration.DefaultRmSchedulerMaximumAllocationVcores
				);
			// zero memory
			try
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(0
					, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, null, mockScheduler
					, rmContext);
			}
			catch (InvalidResourceRequestException)
			{
				NUnit.Framework.Assert.Fail("Zero memory should be accepted");
			}
			// zero vcores
			try
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(YarnConfiguration
					.DefaultRmSchedulerMinimumAllocationMb, 0);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, null, mockScheduler
					, rmContext);
			}
			catch (InvalidResourceRequestException)
			{
				NUnit.Framework.Assert.Fail("Zero vcores should be accepted");
			}
			// max memory
			try
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(YarnConfiguration
					.DefaultRmSchedulerMaximumAllocationMb, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores
					);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, null, mockScheduler
					, rmContext);
			}
			catch (InvalidResourceRequestException)
			{
				NUnit.Framework.Assert.Fail("Max memory should be accepted");
			}
			// max vcores
			try
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(YarnConfiguration
					.DefaultRmSchedulerMinimumAllocationMb, YarnConfiguration.DefaultRmSchedulerMaximumAllocationVcores
					);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, null, mockScheduler
					, rmContext);
			}
			catch (InvalidResourceRequestException)
			{
				NUnit.Framework.Assert.Fail("Max vcores should not be accepted");
			}
			// negative memory
			try
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(-
					1, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, null, mockScheduler
					, rmContext);
				NUnit.Framework.Assert.Fail("Negative memory should not be accepted");
			}
			catch (InvalidResourceRequestException)
			{
			}
			// expected
			// negative vcores
			try
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(YarnConfiguration
					.DefaultRmSchedulerMinimumAllocationMb, -1);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, null, mockScheduler
					, rmContext);
				NUnit.Framework.Assert.Fail("Negative vcores should not be accepted");
			}
			catch (InvalidResourceRequestException)
			{
			}
			// expected
			// more than max memory
			try
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(YarnConfiguration
					.DefaultRmSchedulerMaximumAllocationMb + 1, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores
					);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, null, mockScheduler
					, rmContext);
				NUnit.Framework.Assert.Fail("More than max memory should not be accepted");
			}
			catch (InvalidResourceRequestException)
			{
			}
			// expected
			// more than max vcores
			try
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(YarnConfiguration
					.DefaultRmSchedulerMinimumAllocationMb, YarnConfiguration.DefaultRmSchedulerMaximumAllocationVcores
					 + 1);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, null, mockScheduler
					, rmContext);
				NUnit.Framework.Assert.Fail("More than max vcores should not be accepted");
			}
			catch (InvalidResourceRequestException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestValidateResourceBlacklistRequest()
		{
			TestAMAuthorization.MyContainerManager containerManager = new TestAMAuthorization.MyContainerManager
				();
			TestAMAuthorization.MockRMWithAMS rm = new TestAMAuthorization.MockRMWithAMS(new 
				YarnConfiguration(), containerManager);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("localhost:1234", 5120);
			IDictionary<ApplicationAccessType, string> acls = new Dictionary<ApplicationAccessType
				, string>(2);
			acls[ApplicationAccessType.ViewApp] = "*";
			RMApp app = rm.SubmitApp(1024, "appname", "appuser", acls);
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			ApplicationAttemptId applicationAttemptId = attempt.GetAppAttemptId();
			WaitForLaunchedState(attempt);
			// Create a client to the RM.
			Configuration conf = rm.GetConfig();
			YarnRPC rpc = YarnRPC.Create(conf);
			UserGroupInformation currentUser = UserGroupInformation.CreateRemoteUser(applicationAttemptId
				.ToString());
			Credentials credentials = containerManager.GetContainerCredentials();
			IPEndPoint rmBindAddress = rm.GetApplicationMasterService().GetBindAddress();
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> amRMToken = TestAMAuthorization.MockRMWithAMS
				.SetupAndReturnAMRMToken(rmBindAddress, credentials.GetAllTokens());
			currentUser.AddToken(amRMToken);
			ApplicationMasterProtocol client = currentUser.DoAs(new _PrivilegedAction_626(rpc
				, rmBindAddress, conf));
			RegisterApplicationMasterRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<RegisterApplicationMasterRequest>();
			client.RegisterApplicationMaster(request);
			ResourceBlacklistRequest blacklistRequest = ResourceBlacklistRequest.NewInstance(
				Sharpen.Collections.SingletonList(ResourceRequest.Any), null);
			AllocateRequest allocateRequest = AllocateRequest.NewInstance(0, 0.0f, null, null
				, blacklistRequest);
			bool error = false;
			try
			{
				client.Allocate(allocateRequest);
			}
			catch (InvalidResourceBlacklistRequestException)
			{
				error = true;
			}
			rm.Stop();
			NUnit.Framework.Assert.IsTrue("Didn't not catch InvalidResourceBlacklistRequestException"
				, error);
		}

		private sealed class _PrivilegedAction_626 : PrivilegedAction<ApplicationMasterProtocol
			>
		{
			public _PrivilegedAction_626(YarnRPC rpc, IPEndPoint rmBindAddress, Configuration
				 conf)
			{
				this.rpc = rpc;
				this.rmBindAddress = rmBindAddress;
				this.conf = conf;
			}

			public ApplicationMasterProtocol Run()
			{
				return (ApplicationMasterProtocol)rpc.GetProxy(typeof(ApplicationMasterProtocol), 
					rmBindAddress, conf);
			}

			private readonly YarnRPC rpc;

			private readonly IPEndPoint rmBindAddress;

			private readonly Configuration conf;
		}

		/// <exception cref="System.Exception"/>
		private void WaitForLaunchedState(RMAppAttempt attempt)
		{
			int waitCount = 0;
			while (attempt.GetAppAttemptState() != RMAppAttemptState.Launched && waitCount++ 
				< 20)
			{
				Log.Info("Waiting for AppAttempt to reach LAUNCHED state. " + "Current state is "
					 + attempt.GetAppAttemptState());
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.AreEqual(attempt.GetAppAttemptState(), RMAppAttemptState.Launched
				);
		}

		[NUnit.Framework.Test]
		public virtual void TestComparePriorities()
		{
			Priority high = Priority.NewInstance(1);
			Priority low = Priority.NewInstance(2);
			NUnit.Framework.Assert.IsTrue(high.CompareTo(low) > 0);
		}

		[NUnit.Framework.Test]
		public virtual void TestCreateAbnormalContainerStatus()
		{
			ContainerStatus cd = SchedulerUtils.CreateAbnormalContainerStatus(ContainerId.NewContainerId
				(ApplicationAttemptId.NewInstance(ApplicationId.NewInstance(Runtime.CurrentTimeMillis
				(), 1), 1), 1), "x");
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.Aborted, cd.GetExitStatus());
		}

		[NUnit.Framework.Test]
		public virtual void TestCreatePreemptedContainerStatus()
		{
			ContainerStatus cd = SchedulerUtils.CreatePreemptedContainerStatus(ContainerId.NewContainerId
				(ApplicationAttemptId.NewInstance(ApplicationId.NewInstance(Runtime.CurrentTimeMillis
				(), 1), 1), 1), "x");
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.Preempted, cd.GetExitStatus()
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNormalizeNodeLabelExpression()
		{
			// mock queue and scheduler
			YarnScheduler scheduler = Org.Mockito.Mockito.Mock<YarnScheduler>();
			ICollection<string> queueAccessibleNodeLabels = Sets.NewHashSet();
			QueueInfo queueInfo = Org.Mockito.Mockito.Mock<QueueInfo>();
			Org.Mockito.Mockito.When(queueInfo.GetQueueName()).ThenReturn("queue");
			Org.Mockito.Mockito.When(queueInfo.GetAccessibleNodeLabels()).ThenReturn(queueAccessibleNodeLabels
				);
			Org.Mockito.Mockito.When(queueInfo.GetDefaultNodeLabelExpression()).ThenReturn(" x "
				);
			Org.Mockito.Mockito.When(scheduler.GetQueueInfo(Matchers.Any<string>(), Matchers.AnyBoolean
				(), Matchers.AnyBoolean())).ThenReturn(queueInfo);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource maxResource = Resources.CreateResource
				(YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb, YarnConfiguration.DefaultRmSchedulerMaximumAllocationVcores
				);
			// queue has labels, success cases
			try
			{
				// set queue accessible node labels to [x, y]
				queueAccessibleNodeLabels.Clear();
				Sharpen.Collections.AddAll(queueAccessibleNodeLabels, Arrays.AsList("x", "y"));
				rmContext.GetNodeLabelManager().AddToCluserNodeLabels(ImmutableSet.Of("x", "y"));
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(0
					, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
				ResourceRequest resReq = BuilderUtils.NewResourceRequest(Org.Mockito.Mockito.Mock
					<Priority>(), ResourceRequest.Any, resource, 1);
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				NUnit.Framework.Assert.IsTrue(resReq.GetNodeLabelExpression().Equals("x"));
				resReq.SetNodeLabelExpression(" y ");
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maxResource, "queue", scheduler
					, rmContext);
				NUnit.Framework.Assert.IsTrue(resReq.GetNodeLabelExpression().Equals("y"));
			}
			catch (InvalidResourceRequestException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Should be valid when request labels is a subset of queue labels"
					);
			}
			finally
			{
				rmContext.GetNodeLabelManager().RemoveFromClusterNodeLabels(Arrays.AsList("x", "y"
					));
			}
		}

		/// <exception cref="System.Exception"/>
		public static SchedulerApplication<SchedulerApplicationAttempt> VerifyAppAddedAndRemovedFromScheduler
			(IDictionary<ApplicationId, SchedulerApplication<SchedulerApplicationAttempt>> applications
			, EventHandler<SchedulerEvent> handler, string queueName)
		{
			ApplicationId appId = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 1);
			AppAddedSchedulerEvent appAddedEvent = new AppAddedSchedulerEvent(appId, queueName
				, "user");
			handler.Handle(appAddedEvent);
			SchedulerApplication<SchedulerApplicationAttempt> app = applications[appId];
			// verify application is added.
			NUnit.Framework.Assert.IsNotNull(app);
			NUnit.Framework.Assert.AreEqual("user", app.GetUser());
			AppRemovedSchedulerEvent appRemoveEvent = new AppRemovedSchedulerEvent(appId, RMAppState
				.Finished);
			handler.Handle(appRemoveEvent);
			NUnit.Framework.Assert.IsNull(applications[appId]);
			return app;
		}

		private static RMContext GetMockRMContext()
		{
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			RMNodeLabelsManager nlm = new NullRMNodeLabelsManager();
			nlm.Init(new Configuration(false));
			Org.Mockito.Mockito.When(rmContext.GetNodeLabelManager()).ThenReturn(nlm);
			return rmContext;
		}
	}
}
