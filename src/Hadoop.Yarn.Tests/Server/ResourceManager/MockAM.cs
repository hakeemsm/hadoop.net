using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class MockAM
	{
		private volatile int responseId = 0;

		private readonly ApplicationAttemptId attemptId;

		private RMContext context;

		private ApplicationMasterProtocol amRMProtocol;

		private UserGroupInformation ugi;

		private volatile AllocateResponse lastResponse;

		private readonly IList<ResourceRequest> requests = new AList<ResourceRequest>();

		private readonly IList<ContainerId> releases = new AList<ContainerId>();

		public MockAM(RMContext context, ApplicationMasterProtocol amRMProtocol, ApplicationAttemptId
			 attemptId)
		{
			this.context = context;
			this.amRMProtocol = amRMProtocol;
			this.attemptId = attemptId;
		}

		public virtual void SetAMRMProtocol(ApplicationMasterProtocol amRMProtocol, RMContext
			 context)
		{
			this.context = context;
			this.amRMProtocol = amRMProtocol;
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForState(RMAppAttemptState finalState)
		{
			RMApp app = context.GetRMApps()[attemptId.GetApplicationId()];
			RMAppAttempt attempt = app.GetRMAppAttempt(attemptId);
			int timeoutSecs = 0;
			while (!finalState.Equals(attempt.GetAppAttemptState()) && timeoutSecs++ < 40)
			{
				System.Console.Out.WriteLine("AppAttempt : " + attemptId + " State is : " + attempt
					.GetAppAttemptState() + " Waiting for state : " + finalState);
				Sharpen.Thread.Sleep(1000);
			}
			System.Console.Out.WriteLine("AppAttempt State is : " + attempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.AreEqual("AppAttempt state is not correct (timedout)", finalState
				, attempt.GetAppAttemptState());
		}

		/// <exception cref="System.Exception"/>
		public virtual RegisterApplicationMasterResponse RegisterAppAttempt()
		{
			return RegisterAppAttempt(true);
		}

		/// <exception cref="System.Exception"/>
		public virtual RegisterApplicationMasterResponse RegisterAppAttempt(bool wait)
		{
			if (wait)
			{
				WaitForState(RMAppAttemptState.Launched);
			}
			responseId = 0;
			RegisterApplicationMasterRequest req = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<RegisterApplicationMasterRequest>();
			req.SetHost(string.Empty);
			req.SetRpcPort(1);
			req.SetTrackingUrl(string.Empty);
			if (ugi == null)
			{
				ugi = UserGroupInformation.CreateRemoteUser(attemptId.ToString());
				Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> token = context.GetRMApps
					()[attemptId.GetApplicationId()].GetRMAppAttempt(attemptId).GetAMRMToken();
				ugi.AddTokenIdentifier(token.DecodeIdentifier());
			}
			try
			{
				return ugi.DoAs(new _PrivilegedExceptionAction_117(this, req));
			}
			catch (UndeclaredThrowableException e)
			{
				throw (Exception)e.InnerException;
			}
		}

		private sealed class _PrivilegedExceptionAction_117 : PrivilegedExceptionAction<RegisterApplicationMasterResponse
			>
		{
			public _PrivilegedExceptionAction_117(MockAM _enclosing, RegisterApplicationMasterRequest
				 req)
			{
				this._enclosing = _enclosing;
				this.req = req;
			}

			/// <exception cref="System.Exception"/>
			public RegisterApplicationMasterResponse Run()
			{
				return this._enclosing.amRMProtocol.RegisterApplicationMaster(req);
			}

			private readonly MockAM _enclosing;

			private readonly RegisterApplicationMasterRequest req;
		}

		/// <exception cref="System.Exception"/>
		public virtual void AddRequests(string[] hosts, int memory, int priority, int containers
			)
		{
			Sharpen.Collections.AddAll(requests, CreateReq(hosts, memory, priority, containers
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual AllocateResponse Schedule()
		{
			AllocateResponse response = Allocate(requests, releases);
			requests.Clear();
			releases.Clear();
			return response;
		}

		public virtual void AddContainerToBeReleased(ContainerId containerId)
		{
			releases.AddItem(containerId);
		}

		/// <exception cref="System.Exception"/>
		public virtual AllocateResponse Allocate(string host, int memory, int numContainers
			, IList<ContainerId> releases)
		{
			return Allocate(host, memory, numContainers, releases, null);
		}

		/// <exception cref="System.Exception"/>
		public virtual AllocateResponse Allocate(string host, int memory, int numContainers
			, IList<ContainerId> releases, string labelExpression)
		{
			IList<ResourceRequest> reqs = CreateReq(new string[] { host }, memory, 1, numContainers
				, labelExpression);
			return Allocate(reqs, releases);
		}

		/// <exception cref="System.Exception"/>
		public virtual IList<ResourceRequest> CreateReq(string[] hosts, int memory, int priority
			, int containers)
		{
			return CreateReq(hosts, memory, priority, containers, null);
		}

		/// <exception cref="System.Exception"/>
		public virtual IList<ResourceRequest> CreateReq(string[] hosts, int memory, int priority
			, int containers, string labelExpression)
		{
			IList<ResourceRequest> reqs = new AList<ResourceRequest>();
			foreach (string host in hosts)
			{
				// only add host/rack request when asked host isn't ANY
				if (!host.Equals(ResourceRequest.Any))
				{
					ResourceRequest hostReq = CreateResourceReq(host, memory, priority, containers, labelExpression
						);
					reqs.AddItem(hostReq);
					ResourceRequest rackReq = CreateResourceReq("/default-rack", memory, priority, containers
						, labelExpression);
					reqs.AddItem(rackReq);
				}
			}
			ResourceRequest offRackReq = CreateResourceReq(ResourceRequest.Any, memory, priority
				, containers, labelExpression);
			reqs.AddItem(offRackReq);
			return reqs;
		}

		/// <exception cref="System.Exception"/>
		public virtual ResourceRequest CreateResourceReq(string resource, int memory, int
			 priority, int containers)
		{
			return CreateResourceReq(resource, memory, priority, containers, null);
		}

		/// <exception cref="System.Exception"/>
		public virtual ResourceRequest CreateResourceReq(string resource, int memory, int
			 priority, int containers, string labelExpression)
		{
			ResourceRequest req = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ResourceRequest
				>();
			req.SetResourceName(resource);
			req.SetNumContainers(containers);
			Priority pri = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Priority>();
			pri.SetPriority(priority);
			req.SetPriority(pri);
			Resource capability = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Resource>();
			capability.SetMemory(memory);
			req.SetCapability(capability);
			if (labelExpression != null)
			{
				req.SetNodeLabelExpression(labelExpression);
			}
			return req;
		}

		/// <exception cref="System.Exception"/>
		public virtual AllocateResponse Allocate(IList<ResourceRequest> resourceRequest, 
			IList<ContainerId> releases)
		{
			AllocateRequest req = AllocateRequest.NewInstance(0, 0F, resourceRequest, releases
				, null);
			return Allocate(req);
		}

		/// <exception cref="System.Exception"/>
		public virtual AllocateResponse Allocate(AllocateRequest allocateRequest)
		{
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(attemptId.ToString
				());
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> token = context.GetRMApps
				()[attemptId.GetApplicationId()].GetRMAppAttempt(attemptId).GetAMRMToken();
			ugi.AddTokenIdentifier(token.DecodeIdentifier());
			lastResponse = DoAllocateAs(ugi, allocateRequest);
			return lastResponse;
		}

		/// <exception cref="System.Exception"/>
		public virtual AllocateResponse DoAllocateAs(UserGroupInformation ugi, AllocateRequest
			 req)
		{
			req.SetResponseId(++responseId);
			try
			{
				return ugi.DoAs(new _PrivilegedExceptionAction_234(this, req));
			}
			catch (UndeclaredThrowableException e)
			{
				throw (Exception)e.InnerException;
			}
		}

		private sealed class _PrivilegedExceptionAction_234 : PrivilegedExceptionAction<AllocateResponse
			>
		{
			public _PrivilegedExceptionAction_234(MockAM _enclosing, AllocateRequest req)
			{
				this._enclosing = _enclosing;
				this.req = req;
			}

			/// <exception cref="System.Exception"/>
			public AllocateResponse Run()
			{
				return this._enclosing.amRMProtocol.Allocate(req);
			}

			private readonly MockAM _enclosing;

			private readonly AllocateRequest req;
		}

		/// <exception cref="System.Exception"/>
		public virtual AllocateResponse DoHeartbeat()
		{
			return Allocate(null, null);
		}

		/// <exception cref="System.Exception"/>
		public virtual void UnregisterAppAttempt()
		{
			WaitForState(RMAppAttemptState.Running);
			UnregisterAppAttempt(true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void UnregisterAppAttempt(bool waitForStateRunning)
		{
			FinishApplicationMasterRequest req = FinishApplicationMasterRequest.NewInstance(FinalApplicationStatus
				.Succeeded, string.Empty, string.Empty);
			UnregisterAppAttempt(req, waitForStateRunning);
		}

		/// <exception cref="System.Exception"/>
		public virtual void UnregisterAppAttempt(FinishApplicationMasterRequest req, bool
			 waitForStateRunning)
		{
			if (waitForStateRunning)
			{
				WaitForState(RMAppAttemptState.Running);
			}
			if (ugi == null)
			{
				ugi = UserGroupInformation.CreateRemoteUser(attemptId.ToString());
				Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> token = context.GetRMApps
					()[attemptId.GetApplicationId()].GetRMAppAttempt(attemptId).GetAMRMToken();
				ugi.AddTokenIdentifier(token.DecodeIdentifier());
			}
			try
			{
				ugi.DoAs(new _PrivilegedExceptionAction_276(this, req));
			}
			catch (UndeclaredThrowableException e)
			{
				throw (Exception)e.InnerException;
			}
		}

		private sealed class _PrivilegedExceptionAction_276 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_276(MockAM _enclosing, FinishApplicationMasterRequest
				 req)
			{
				this._enclosing = _enclosing;
				this.req = req;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				this._enclosing.amRMProtocol.FinishApplicationMaster(req);
				return null;
			}

			private readonly MockAM _enclosing;

			private readonly FinishApplicationMasterRequest req;
		}

		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			return this.attemptId;
		}

		/// <exception cref="System.Exception"/>
		public virtual IList<Container> AllocateAndWaitForContainers(int nContainer, int 
			memory, MockNM nm)
		{
			// AM request for containers
			Allocate("ANY", memory, nContainer, null);
			// kick the scheduler
			nm.NodeHeartbeat(true);
			IList<Container> conts = Allocate(new AList<ResourceRequest>(), null).GetAllocatedContainers
				();
			while (conts.Count < nContainer)
			{
				nm.NodeHeartbeat(true);
				Sharpen.Collections.AddAll(conts, Allocate(new AList<ResourceRequest>(), new AList
					<ContainerId>()).GetAllocatedContainers());
				Sharpen.Thread.Sleep(500);
			}
			return conts;
		}
	}
}
