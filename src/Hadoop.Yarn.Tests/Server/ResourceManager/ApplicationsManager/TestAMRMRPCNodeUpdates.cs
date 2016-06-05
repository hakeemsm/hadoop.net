using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Applicationsmanager
{
	public class TestAMRMRPCNodeUpdates
	{
		private MockRM rm;

		internal ApplicationMasterService amService = null;

		internal DrainDispatcher dispatcher = null;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			dispatcher = new DrainDispatcher();
			this.rm = new _MockRM_58(this);
			rm.Start();
			amService = rm.GetApplicationMasterService();
		}

		private sealed class _MockRM_58 : MockRM
		{
			public _MockRM_58(TestAMRMRPCNodeUpdates _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Init(Configuration conf)
			{
				conf.Set(CapacitySchedulerConfiguration.MaximumApplicationMastersResourcePercent, 
					"1.0");
				base.Init(conf);
			}

			protected internal override EventHandler<SchedulerEvent> CreateSchedulerEventDispatcher
				()
			{
				return new _SchedulerEventDispatcher_68(this, this.scheduler);
			}

			private sealed class _SchedulerEventDispatcher_68 : ResourceManager.SchedulerEventDispatcher
			{
				public _SchedulerEventDispatcher_68(_MockRM_58 _enclosing, ResourceScheduler baseArg1
					)
					: base(baseArg1)
				{
					this._enclosing = _enclosing;
				}

				public override void Handle(SchedulerEvent @event)
				{
					this._enclosing.scheduler.Handle(@event);
				}

				private readonly _MockRM_58 _enclosing;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return this._enclosing.dispatcher;
			}

			private readonly TestAMRMRPCNodeUpdates _enclosing;
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (rm != null)
			{
				this.rm.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		private void SyncNodeHeartbeat(MockNM nm, bool health)
		{
			nm.NodeHeartbeat(health);
			dispatcher.Await();
		}

		/// <exception cref="System.Exception"/>
		private void SyncNodeLost(MockNM nm)
		{
			rm.SendNodeStarted(nm);
			rm.NMwaitForState(nm.GetNodeId(), NodeState.Running);
			rm.SendNodeLost(nm);
			dispatcher.Await();
		}

		/// <exception cref="System.Exception"/>
		private AllocateResponse Allocate(ApplicationAttemptId attemptId, AllocateRequest
			 req)
		{
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(attemptId.ToString
				());
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> token = rm.GetRMContext
				().GetRMApps()[attemptId.GetApplicationId()].GetRMAppAttempt(attemptId).GetAMRMToken
				();
			ugi.AddTokenIdentifier(token.DecodeIdentifier());
			return ugi.DoAs(new _PrivilegedExceptionAction_112(this, req));
		}

		private sealed class _PrivilegedExceptionAction_112 : PrivilegedExceptionAction<AllocateResponse
			>
		{
			public _PrivilegedExceptionAction_112(TestAMRMRPCNodeUpdates _enclosing, AllocateRequest
				 req)
			{
				this._enclosing = _enclosing;
				this.req = req;
			}

			/// <exception cref="System.Exception"/>
			public AllocateResponse Run()
			{
				return this._enclosing.amService.Allocate(req);
			}

			private readonly TestAMRMRPCNodeUpdates _enclosing;

			private readonly AllocateRequest req;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAMRMUnusableNodes()
		{
			MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 10000);
			MockNM nm2 = rm.RegisterNode("127.0.0.2:1234", 10000);
			MockNM nm3 = rm.RegisterNode("127.0.0.3:1234", 10000);
			MockNM nm4 = rm.RegisterNode("127.0.0.4:1234", 10000);
			dispatcher.Await();
			RMApp app1 = rm.SubmitApp(2000);
			// Trigger the scheduling so the AM gets 'launched' on nm1
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			MockAM am1 = rm.SendAMLaunched(attempt1.GetAppAttemptId());
			// register AM returns no unusable node
			am1.RegisterAppAttempt();
			// allocate request returns no updated node
			AllocateRequest allocateRequest1 = AllocateRequest.NewInstance(0, 0F, null, null, 
				null);
			AllocateResponse response1 = Allocate(attempt1.GetAppAttemptId(), allocateRequest1
				);
			IList<NodeReport> updatedNodes = response1.GetUpdatedNodes();
			NUnit.Framework.Assert.AreEqual(0, updatedNodes.Count);
			SyncNodeHeartbeat(nm4, false);
			// allocate request returns updated node
			allocateRequest1 = AllocateRequest.NewInstance(response1.GetResponseId(), 0F, null
				, null, null);
			response1 = Allocate(attempt1.GetAppAttemptId(), allocateRequest1);
			updatedNodes = response1.GetUpdatedNodes();
			NUnit.Framework.Assert.AreEqual(1, updatedNodes.Count);
			NodeReport nr = updatedNodes.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual(nm4.GetNodeId(), nr.GetNodeId());
			NUnit.Framework.Assert.AreEqual(NodeState.Unhealthy, nr.GetNodeState());
			// resending the allocate request returns the same result
			response1 = Allocate(attempt1.GetAppAttemptId(), allocateRequest1);
			updatedNodes = response1.GetUpdatedNodes();
			NUnit.Framework.Assert.AreEqual(1, updatedNodes.Count);
			nr = updatedNodes.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual(nm4.GetNodeId(), nr.GetNodeId());
			NUnit.Framework.Assert.AreEqual(NodeState.Unhealthy, nr.GetNodeState());
			SyncNodeLost(nm3);
			// subsequent allocate request returns delta
			allocateRequest1 = AllocateRequest.NewInstance(response1.GetResponseId(), 0F, null
				, null, null);
			response1 = Allocate(attempt1.GetAppAttemptId(), allocateRequest1);
			updatedNodes = response1.GetUpdatedNodes();
			NUnit.Framework.Assert.AreEqual(1, updatedNodes.Count);
			nr = updatedNodes.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual(nm3.GetNodeId(), nr.GetNodeId());
			NUnit.Framework.Assert.AreEqual(NodeState.Lost, nr.GetNodeState());
			// registering another AM gives it the complete failed list
			RMApp app2 = rm.SubmitApp(2000);
			// Trigger nm2 heartbeat so that AM gets launched on it
			nm2.NodeHeartbeat(true);
			RMAppAttempt attempt2 = app2.GetCurrentAppAttempt();
			MockAM am2 = rm.SendAMLaunched(attempt2.GetAppAttemptId());
			// register AM returns all unusable nodes
			am2.RegisterAppAttempt();
			// allocate request returns no updated node
			AllocateRequest allocateRequest2 = AllocateRequest.NewInstance(0, 0F, null, null, 
				null);
			AllocateResponse response2 = Allocate(attempt2.GetAppAttemptId(), allocateRequest2
				);
			updatedNodes = response2.GetUpdatedNodes();
			NUnit.Framework.Assert.AreEqual(0, updatedNodes.Count);
			SyncNodeHeartbeat(nm4, true);
			// both AM's should get delta updated nodes
			allocateRequest1 = AllocateRequest.NewInstance(response1.GetResponseId(), 0F, null
				, null, null);
			response1 = Allocate(attempt1.GetAppAttemptId(), allocateRequest1);
			updatedNodes = response1.GetUpdatedNodes();
			NUnit.Framework.Assert.AreEqual(1, updatedNodes.Count);
			nr = updatedNodes.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual(nm4.GetNodeId(), nr.GetNodeId());
			NUnit.Framework.Assert.AreEqual(NodeState.Running, nr.GetNodeState());
			allocateRequest2 = AllocateRequest.NewInstance(response2.GetResponseId(), 0F, null
				, null, null);
			response2 = Allocate(attempt2.GetAppAttemptId(), allocateRequest2);
			updatedNodes = response2.GetUpdatedNodes();
			NUnit.Framework.Assert.AreEqual(1, updatedNodes.Count);
			nr = updatedNodes.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual(nm4.GetNodeId(), nr.GetNodeId());
			NUnit.Framework.Assert.AreEqual(NodeState.Running, nr.GetNodeState());
			// subsequent allocate calls should return no updated nodes
			allocateRequest2 = AllocateRequest.NewInstance(response2.GetResponseId(), 0F, null
				, null, null);
			response2 = Allocate(attempt2.GetAppAttemptId(), allocateRequest2);
			updatedNodes = response2.GetUpdatedNodes();
			NUnit.Framework.Assert.AreEqual(0, updatedNodes.Count);
		}
		// how to do the above for LOST node
	}
}
