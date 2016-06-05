/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Applicationsmanager
{
	public abstract class MockAsm : MockApps
	{
		public class ApplicationBase : RMApp
		{
			internal ResourceRequest amReq;

			public virtual string GetUser()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual ApplicationSubmissionContext GetApplicationSubmissionContext()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual string GetName()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual string GetQueue()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual long GetStartTime()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual long GetSubmitTime()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual long GetFinishTime()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual StringBuilder GetDiagnostics()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual ApplicationId GetApplicationId()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual RMAppAttempt GetCurrentAppAttempt()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual IDictionary<ApplicationAttemptId, RMAppAttempt> GetAppAttempts()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual float GetProgress()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual RMAppAttempt GetRMAppAttempt(ApplicationAttemptId appAttemptId)
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual RMAppState GetState()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual string GetTrackingUrl()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual string GetOriginalTrackingUrl()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual int GetMaxAppAttempts()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual ApplicationReport CreateAndGetApplicationReport(string clientUserName
				, bool allowAccess)
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual void Handle(RMAppEvent @event)
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual FinalApplicationStatus GetFinalApplicationStatus()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual int PullRMNodeUpdates(ICollection<RMNode> updatedNodes)
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual string GetApplicationType()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual ICollection<string> GetApplicationTags()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual void SetQueue(string name)
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual bool IsAppFinalStateStored()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual YarnApplicationState CreateApplicationState()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual ICollection<NodeId> GetRanNodes()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual RMAppMetrics GetRMAppMetrics()
			{
				return new RMAppMetrics(Resource.NewInstance(0, 0), 0, 0, 0, 0);
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

		public static RMApp NewApplication(int i)
		{
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(NewAppID(i), 
				0);
			Container masterContainer = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Container
				>();
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 0);
			masterContainer.SetId(containerId);
			masterContainer.SetNodeHttpAddress("node:port");
			string user = NewUserName();
			string name = NewAppName();
			string queue = NewQueue();
			long start = 123456 + i * 1000;
			long finish = 234567 + i * 1000;
			string type = YarnConfiguration.DefaultApplicationType;
			YarnApplicationState[] allStates = YarnApplicationState.Values();
			YarnApplicationState state = allStates[i % allStates.Length];
			int maxAppAttempts = i % 1000;
			return new _ApplicationBase_211(appAttemptId, user, name, type, queue, start, finish
				, state, maxAppAttempts);
		}

		private sealed class _ApplicationBase_211 : MockAsm.ApplicationBase
		{
			public _ApplicationBase_211(ApplicationAttemptId appAttemptId, string user, string
				 name, string type, string queue, long start, long finish, YarnApplicationState 
				state, int maxAppAttempts)
			{
				this.appAttemptId = appAttemptId;
				this.user = user;
				this.name = name;
				this.type = type;
				this.queue = queue;
				this.start = start;
				this.finish = finish;
				this.state = state;
				this.maxAppAttempts = maxAppAttempts;
			}

			public override ApplicationId GetApplicationId()
			{
				return appAttemptId.GetApplicationId();
			}

			public override string GetUser()
			{
				return user;
			}

			public override string GetName()
			{
				return name;
			}

			public override string GetApplicationType()
			{
				return type;
			}

			public override string GetQueue()
			{
				return queue;
			}

			public override long GetStartTime()
			{
				return start;
			}

			public override long GetFinishTime()
			{
				return finish;
			}

			public override string GetTrackingUrl()
			{
				return null;
			}

			public override YarnApplicationState CreateApplicationState()
			{
				return state;
			}

			public override StringBuilder GetDiagnostics()
			{
				return new StringBuilder();
			}

			public override float GetProgress()
			{
				return (float)Math.Random();
			}

			public override FinalApplicationStatus GetFinalApplicationStatus()
			{
				return FinalApplicationStatus.Undefined;
			}

			public override RMAppAttempt GetCurrentAppAttempt()
			{
				return null;
			}

			public override int GetMaxAppAttempts()
			{
				return maxAppAttempts;
			}

			public override ICollection<string> GetApplicationTags()
			{
				return null;
			}

			public override ApplicationReport CreateAndGetApplicationReport(string clientUserName
				, bool allowAccess)
			{
				ApplicationResourceUsageReport usageReport = ApplicationResourceUsageReport.NewInstance
					(0, 0, null, null, null, 0, 0);
				ApplicationReport report = ApplicationReport.NewInstance(this.GetApplicationId(), 
					appAttemptId, this.GetUser(), this.GetQueue(), this.GetName(), null, 0, null, null
					, this.GetDiagnostics().ToString(), this.GetTrackingUrl(), this.GetStartTime(), 
					this.GetFinishTime(), this.GetFinalApplicationStatus(), usageReport, null, this.
					GetProgress(), type, null);
				return report;
			}

			private readonly ApplicationAttemptId appAttemptId;

			private readonly string user;

			private readonly string name;

			private readonly string type;

			private readonly string queue;

			private readonly long start;

			private readonly long finish;

			private readonly YarnApplicationState state;

			private readonly int maxAppAttempts;
		}

		public static IList<RMApp> NewApplications(int n)
		{
			IList<RMApp> list = Lists.NewArrayList();
			for (int i = 0; i < n; ++i)
			{
				list.AddItem(NewApplication(i));
			}
			return list;
		}
	}
}
