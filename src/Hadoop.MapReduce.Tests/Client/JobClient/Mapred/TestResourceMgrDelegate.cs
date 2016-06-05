using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestResourceMgrDelegate
	{
		/// <summary>Tests that getRootQueues makes a request for the (recursive) child queues
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetRootQueues()
		{
			ApplicationClientProtocol applicationsManager = Org.Mockito.Mockito.Mock<ApplicationClientProtocol
				>();
			GetQueueInfoResponse response = Org.Mockito.Mockito.Mock<GetQueueInfoResponse>();
			QueueInfo queueInfo = Org.Mockito.Mockito.Mock<QueueInfo>();
			Org.Mockito.Mockito.When(response.GetQueueInfo()).ThenReturn(queueInfo);
			try
			{
				Org.Mockito.Mockito.When(applicationsManager.GetQueueInfo(Org.Mockito.Mockito.Any
					<GetQueueInfoRequest>())).ThenReturn(response);
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
			ResourceMgrDelegate delegate_ = new _ResourceMgrDelegate_69(applicationsManager, 
				new YarnConfiguration());
			delegate_.GetRootQueues();
			ArgumentCaptor<GetQueueInfoRequest> argument = ArgumentCaptor.ForClass<GetQueueInfoRequest
				>();
			try
			{
				Org.Mockito.Mockito.Verify(applicationsManager).GetQueueInfo(argument.Capture());
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
			NUnit.Framework.Assert.IsTrue("Children of root queue not requested", argument.GetValue
				().GetIncludeChildQueues());
			NUnit.Framework.Assert.IsTrue("Request wasn't to recurse through children", argument
				.GetValue().GetRecursive());
		}

		private sealed class _ResourceMgrDelegate_69 : ResourceMgrDelegate
		{
			public _ResourceMgrDelegate_69(ApplicationClientProtocol applicationsManager, YarnConfiguration
				 baseArg1)
				: base(baseArg1)
			{
				this.applicationsManager = applicationsManager;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				NUnit.Framework.Assert.IsTrue(this.client is YarnClientImpl);
				((YarnClientImpl)this.client).SetRMClient(applicationsManager);
			}

			private readonly ApplicationClientProtocol applicationsManager;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TesAllJobs()
		{
			ApplicationClientProtocol applicationsManager = Org.Mockito.Mockito.Mock<ApplicationClientProtocol
				>();
			GetApplicationsResponse allApplicationsResponse = Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<GetApplicationsResponse>();
			IList<ApplicationReport> applications = new AList<ApplicationReport>();
			applications.AddItem(GetApplicationReport(YarnApplicationState.Finished, FinalApplicationStatus
				.Failed));
			applications.AddItem(GetApplicationReport(YarnApplicationState.Finished, FinalApplicationStatus
				.Succeeded));
			applications.AddItem(GetApplicationReport(YarnApplicationState.Finished, FinalApplicationStatus
				.Killed));
			applications.AddItem(GetApplicationReport(YarnApplicationState.Failed, FinalApplicationStatus
				.Failed));
			allApplicationsResponse.SetApplicationList(applications);
			Org.Mockito.Mockito.When(applicationsManager.GetApplications(Org.Mockito.Mockito.
				Any<GetApplicationsRequest>())).ThenReturn(allApplicationsResponse);
			ResourceMgrDelegate resourceMgrDelegate = new _ResourceMgrDelegate_113(applicationsManager
				, new YarnConfiguration());
			JobStatus[] allJobs = resourceMgrDelegate.GetAllJobs();
			NUnit.Framework.Assert.AreEqual(JobStatus.State.Failed, allJobs[0].GetState());
			NUnit.Framework.Assert.AreEqual(JobStatus.State.Succeeded, allJobs[1].GetState());
			NUnit.Framework.Assert.AreEqual(JobStatus.State.Killed, allJobs[2].GetState());
			NUnit.Framework.Assert.AreEqual(JobStatus.State.Failed, allJobs[3].GetState());
		}

		private sealed class _ResourceMgrDelegate_113 : ResourceMgrDelegate
		{
			public _ResourceMgrDelegate_113(ApplicationClientProtocol applicationsManager, YarnConfiguration
				 baseArg1)
				: base(baseArg1)
			{
				this.applicationsManager = applicationsManager;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				NUnit.Framework.Assert.IsTrue(this.client is YarnClientImpl);
				((YarnClientImpl)this.client).SetRMClient(applicationsManager);
			}

			private readonly ApplicationClientProtocol applicationsManager;
		}

		private ApplicationReport GetApplicationReport(YarnApplicationState yarnApplicationState
			, FinalApplicationStatus finalApplicationStatus)
		{
			ApplicationReport appReport = Org.Mockito.Mockito.Mock<ApplicationReport>();
			ApplicationResourceUsageReport appResources = Org.Mockito.Mockito.Mock<ApplicationResourceUsageReport
				>();
			Org.Mockito.Mockito.When(appReport.GetApplicationId()).ThenReturn(ApplicationId.NewInstance
				(0, 0));
			Org.Mockito.Mockito.When(appResources.GetNeededResources()).ThenReturn(Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<Resource>());
			Org.Mockito.Mockito.When(appResources.GetReservedResources()).ThenReturn(Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<Resource>());
			Org.Mockito.Mockito.When(appResources.GetUsedResources()).ThenReturn(Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<Resource>());
			Org.Mockito.Mockito.When(appReport.GetApplicationResourceUsageReport()).ThenReturn
				(appResources);
			Org.Mockito.Mockito.When(appReport.GetYarnApplicationState()).ThenReturn(yarnApplicationState
				);
			Org.Mockito.Mockito.When(appReport.GetFinalApplicationStatus()).ThenReturn(finalApplicationStatus
				);
			return appReport;
		}
	}
}
