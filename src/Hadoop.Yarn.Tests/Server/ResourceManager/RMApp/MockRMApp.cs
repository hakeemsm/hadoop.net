using System;
using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp
{
	public class MockRMApp : RMApp
	{
		internal const int Dt = 1000000;

		internal string user = MockApps.NewUserName();

		internal string name = MockApps.NewAppName();

		internal string queue = MockApps.NewQueue();

		internal long start = Runtime.CurrentTimeMillis() - (int)(Math.Random() * Dt);

		internal long submit = start - (int)(Math.Random() * Dt);

		internal long finish = 0;

		internal RMAppState state = RMAppState.New;

		internal int failCount = 0;

		internal ApplicationId id;

		internal string url = null;

		internal string oUrl = null;

		internal StringBuilder diagnostics = new StringBuilder();

		internal RMAppAttempt attempt;

		internal int maxAppAttempts = 1;

		internal ResourceRequest amReq;

		public MockRMApp(int newid, long time, RMAppState newState)
		{
			// ms
			finish = time;
			id = MockApps.NewAppID(newid);
			state = newState;
		}

		public MockRMApp(int newid, long time, RMAppState newState, string userName)
			: this(newid, time, newState)
		{
			user = userName;
		}

		public MockRMApp(int newid, long time, RMAppState newState, string userName, string
			 diag)
			: this(newid, time, newState, userName)
		{
			this.diagnostics = new StringBuilder(diag);
		}

		public virtual ApplicationId GetApplicationId()
		{
			return id;
		}

		public virtual ApplicationSubmissionContext GetApplicationSubmissionContext()
		{
			return new ApplicationSubmissionContextPBImpl();
		}

		public virtual RMAppState GetState()
		{
			return state;
		}

		public virtual void SetState(RMAppState state)
		{
			this.state = state;
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual void SetUser(string user)
		{
			this.user = user;
		}

		public virtual float GetProgress()
		{
			return (float)0.0;
		}

		public virtual RMAppAttempt GetRMAppAttempt(ApplicationAttemptId appAttemptId)
		{
			throw new NotSupportedException("Not supported yet.");
		}

		public virtual string GetQueue()
		{
			return queue;
		}

		public virtual void SetQueue(string queue)
		{
			this.queue = queue;
		}

		public virtual string GetName()
		{
			return name;
		}

		public virtual void SetName(string name)
		{
			this.name = name;
		}

		public virtual IDictionary<ApplicationAttemptId, RMAppAttempt> GetAppAttempts()
		{
			IDictionary<ApplicationAttemptId, RMAppAttempt> attempts = new LinkedHashMap<ApplicationAttemptId
				, RMAppAttempt>();
			if (attempt != null)
			{
				attempts[attempt.GetAppAttemptId()] = attempt;
			}
			return attempts;
		}

		public virtual RMAppAttempt GetCurrentAppAttempt()
		{
			return attempt;
		}

		public virtual void SetCurrentAppAttempt(RMAppAttempt attempt)
		{
			this.attempt = attempt;
		}

		public virtual ApplicationReport CreateAndGetApplicationReport(string clientUserName
			, bool allowAccess)
		{
			throw new NotSupportedException("Not supported yet.");
		}

		public virtual long GetFinishTime()
		{
			return finish;
		}

		public virtual void SetFinishTime(long time)
		{
			this.finish = time;
		}

		public virtual long GetStartTime()
		{
			return start;
		}

		public virtual long GetSubmitTime()
		{
			return submit;
		}

		public virtual void SetStartTime(long time)
		{
			this.start = time;
		}

		public virtual string GetTrackingUrl()
		{
			return url;
		}

		public virtual void SetTrackingUrl(string url)
		{
			this.url = url;
		}

		public virtual string GetOriginalTrackingUrl()
		{
			return oUrl;
		}

		public virtual void SetOriginalTrackingUrl(string oUrl)
		{
			this.oUrl = oUrl;
		}

		public virtual StringBuilder GetDiagnostics()
		{
			return diagnostics;
		}

		public virtual void SetDiagnostics(string diag)
		{
			this.diagnostics = new StringBuilder(diag);
		}

		public virtual int GetMaxAppAttempts()
		{
			return maxAppAttempts;
		}

		public virtual void SetNumMaxRetries(int maxAppAttempts)
		{
			this.maxAppAttempts = maxAppAttempts;
		}

		public virtual void Handle(RMAppEvent @event)
		{
		}

		public virtual FinalApplicationStatus GetFinalApplicationStatus()
		{
			return FinalApplicationStatus.Undefined;
		}

		public virtual int PullRMNodeUpdates(ICollection<RMNode> updatedNodes)
		{
			throw new NotSupportedException("Not supported yet.");
		}

		public virtual string GetApplicationType()
		{
			return YarnConfiguration.DefaultApplicationType;
		}

		public virtual ICollection<string> GetApplicationTags()
		{
			return null;
		}

		public virtual bool IsAppFinalStateStored()
		{
			return true;
		}

		public virtual YarnApplicationState CreateApplicationState()
		{
			return null;
		}

		public virtual ICollection<NodeId> GetRanNodes()
		{
			return null;
		}

		public virtual Resource GetResourcePreempted()
		{
			throw new NotSupportedException("Not supported yet.");
		}

		public virtual RMAppMetrics GetRMAppMetrics()
		{
			throw new NotSupportedException("Not supported yet.");
		}

		public virtual ReservationId GetReservationId()
		{
			throw new NotSupportedException("Not supported yet.");
		}

		public virtual ResourceRequest GetAMResourceRequest()
		{
			return this.amReq;
		}
	}
}
