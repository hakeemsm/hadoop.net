using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public class TestTypeConverter
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEnums()
		{
			foreach (YarnApplicationState applicationState in YarnApplicationState.Values())
			{
				TypeConverter.FromYarn(applicationState, FinalApplicationStatus.Failed);
			}
			// ad hoc test of NEW_SAVING, which is newly added
			NUnit.Framework.Assert.AreEqual(JobStatus.State.Prep, TypeConverter.FromYarn(YarnApplicationState
				.NewSaving, FinalApplicationStatus.Failed));
			foreach (TaskType taskType in TaskType.Values())
			{
				TypeConverter.FromYarn(taskType);
			}
			foreach (JobState jobState in JobState.Values())
			{
				TypeConverter.FromYarn(jobState);
			}
			foreach (QueueState queueState in QueueState.Values())
			{
				TypeConverter.FromYarn(queueState);
			}
			foreach (TaskState taskState in TaskState.Values())
			{
				TypeConverter.FromYarn(taskState);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFromYarn()
		{
			int appStartTime = 612354;
			int appFinishTime = 612355;
			YarnApplicationState state = YarnApplicationState.Running;
			ApplicationId applicationId = ApplicationId.NewInstance(0, 0);
			ApplicationReport applicationReport = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ApplicationReport>();
			applicationReport.SetApplicationId(applicationId);
			applicationReport.SetYarnApplicationState(state);
			applicationReport.SetStartTime(appStartTime);
			applicationReport.SetFinishTime(appFinishTime);
			applicationReport.SetUser("TestTypeConverter-user");
			ApplicationResourceUsageReport appUsageRpt = Org.Apache.Hadoop.Yarn.Util.Records.
				NewRecord<ApplicationResourceUsageReport>();
			Resource r = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Resource>();
			r.SetMemory(2048);
			appUsageRpt.SetNeededResources(r);
			appUsageRpt.SetNumReservedContainers(1);
			appUsageRpt.SetNumUsedContainers(3);
			appUsageRpt.SetReservedResources(r);
			appUsageRpt.SetUsedResources(r);
			applicationReport.SetApplicationResourceUsageReport(appUsageRpt);
			JobStatus jobStatus = TypeConverter.FromYarn(applicationReport, "dummy-jobfile");
			NUnit.Framework.Assert.AreEqual(appStartTime, jobStatus.GetStartTime());
			NUnit.Framework.Assert.AreEqual(appFinishTime, jobStatus.GetFinishTime());
			NUnit.Framework.Assert.AreEqual(state.ToString(), jobStatus.GetState().ToString()
				);
		}

		[NUnit.Framework.Test]
		public virtual void TestFromYarnApplicationReport()
		{
			ApplicationId mockAppId = Org.Mockito.Mockito.Mock<ApplicationId>();
			Org.Mockito.Mockito.When(mockAppId.GetClusterTimestamp()).ThenReturn(12345L);
			Org.Mockito.Mockito.When(mockAppId.GetId()).ThenReturn(6789);
			ApplicationReport mockReport = Org.Mockito.Mockito.Mock<ApplicationReport>();
			Org.Mockito.Mockito.When(mockReport.GetTrackingUrl()).ThenReturn("dummy-tracking-url"
				);
			Org.Mockito.Mockito.When(mockReport.GetApplicationId()).ThenReturn(mockAppId);
			Org.Mockito.Mockito.When(mockReport.GetYarnApplicationState()).ThenReturn(YarnApplicationState
				.Killed);
			Org.Mockito.Mockito.When(mockReport.GetUser()).ThenReturn("dummy-user");
			Org.Mockito.Mockito.When(mockReport.GetQueue()).ThenReturn("dummy-queue");
			string jobFile = "dummy-path/job.xml";
			try
			{
				JobStatus status = TypeConverter.FromYarn(mockReport, jobFile);
			}
			catch (ArgumentNullException)
			{
				NUnit.Framework.Assert.Fail("Type converstion from YARN fails for jobs without " 
					+ "ApplicationUsageReport");
			}
			ApplicationResourceUsageReport appUsageRpt = Org.Apache.Hadoop.Yarn.Util.Records.
				NewRecord<ApplicationResourceUsageReport>();
			Resource r = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Resource>();
			r.SetMemory(2048);
			appUsageRpt.SetNeededResources(r);
			appUsageRpt.SetNumReservedContainers(1);
			appUsageRpt.SetNumUsedContainers(3);
			appUsageRpt.SetReservedResources(r);
			appUsageRpt.SetUsedResources(r);
			Org.Mockito.Mockito.When(mockReport.GetApplicationResourceUsageReport()).ThenReturn
				(appUsageRpt);
			JobStatus status_1 = TypeConverter.FromYarn(mockReport, jobFile);
			NUnit.Framework.Assert.IsNotNull("fromYarn returned null status", status_1);
			NUnit.Framework.Assert.AreEqual("jobFile set incorrectly", "dummy-path/job.xml", 
				status_1.GetJobFile());
			NUnit.Framework.Assert.AreEqual("queue set incorrectly", "dummy-queue", status_1.
				GetQueue());
			NUnit.Framework.Assert.AreEqual("trackingUrl set incorrectly", "dummy-tracking-url"
				, status_1.GetTrackingUrl());
			NUnit.Framework.Assert.AreEqual("user set incorrectly", "dummy-user", status_1.GetUsername
				());
			NUnit.Framework.Assert.AreEqual("schedulingInfo set incorrectly", "dummy-tracking-url"
				, status_1.GetSchedulingInfo());
			NUnit.Framework.Assert.AreEqual("jobId set incorrectly", 6789, status_1.GetJobID(
				).GetId());
			NUnit.Framework.Assert.AreEqual("state set incorrectly", JobStatus.State.Killed, 
				status_1.GetState());
			NUnit.Framework.Assert.AreEqual("needed mem info set incorrectly", 2048, status_1
				.GetNeededMem());
			NUnit.Framework.Assert.AreEqual("num rsvd slots info set incorrectly", 1, status_1
				.GetNumReservedSlots());
			NUnit.Framework.Assert.AreEqual("num used slots info set incorrectly", 3, status_1
				.GetNumUsedSlots());
			NUnit.Framework.Assert.AreEqual("rsvd mem info set incorrectly", 2048, status_1.GetReservedMem
				());
			NUnit.Framework.Assert.AreEqual("used mem info set incorrectly", 2048, status_1.GetUsedMem
				());
		}

		[NUnit.Framework.Test]
		public virtual void TestFromYarnQueueInfo()
		{
			QueueInfo queueInfo = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<QueueInfo>();
			queueInfo.SetQueueState(QueueState.Stopped);
			QueueInfo returned = TypeConverter.FromYarn(queueInfo, new Configuration());
			NUnit.Framework.Assert.AreEqual("queueInfo translation didn't work.", returned.GetState
				().ToString(), StringUtils.ToLowerCase(queueInfo.GetQueueState().ToString()));
		}

		/// <summary>
		/// Test that child queues are converted too during conversion of the parent
		/// queue
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestFromYarnQueue()
		{
			//Define child queue
			QueueInfo child = Org.Mockito.Mockito.Mock<QueueInfo>();
			Org.Mockito.Mockito.When(child.GetQueueState()).ThenReturn(QueueState.Running);
			//Define parent queue
			QueueInfo queueInfo = Org.Mockito.Mockito.Mock<QueueInfo>();
			IList<QueueInfo> children = new AList<QueueInfo>();
			children.AddItem(child);
			//Add one child
			Org.Mockito.Mockito.When(queueInfo.GetChildQueues()).ThenReturn(children);
			Org.Mockito.Mockito.When(queueInfo.GetQueueState()).ThenReturn(QueueState.Running
				);
			//Call the function we're testing
			QueueInfo returned = TypeConverter.FromYarn(queueInfo, new Configuration());
			//Verify that the converted queue has the 1 child we had added
			NUnit.Framework.Assert.AreEqual("QueueInfo children weren't properly converted", 
				returned.GetQueueChildren().Count, 1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFromYarnJobReport()
		{
			int jobStartTime = 612354;
			int jobFinishTime = 612355;
			JobState state = JobState.Running;
			JobId jobId = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<JobId>();
			JobReport jobReport = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<JobReport>();
			ApplicationId applicationId = ApplicationId.NewInstance(0, 0);
			jobId.SetAppId(applicationId);
			jobId.SetId(0);
			jobReport.SetJobId(jobId);
			jobReport.SetJobState(state);
			jobReport.SetStartTime(jobStartTime);
			jobReport.SetFinishTime(jobFinishTime);
			jobReport.SetUser("TestTypeConverter-user");
			JobStatus jobStatus = TypeConverter.FromYarn(jobReport, "dummy-jobfile");
			NUnit.Framework.Assert.AreEqual(jobStartTime, jobStatus.GetStartTime());
			NUnit.Framework.Assert.AreEqual(jobFinishTime, jobStatus.GetFinishTime());
			NUnit.Framework.Assert.AreEqual(state.ToString(), jobStatus.GetState().ToString()
				);
		}
	}
}
