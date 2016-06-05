using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class TestMRJobsWithHistoryService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMRJobsWithHistoryService
			));

		private static readonly EnumSet<RMAppState> TerminalRmAppStates = EnumSet.Of(RMAppState
			.Finished, RMAppState.Failed, RMAppState.Killed);

		private static MiniMRYarnCluster mrCluster;

		private static Configuration conf = new Configuration();

		private static FileSystem localFs;

		static TestMRJobsWithHistoryService()
		{
			try
			{
				localFs = FileSystem.GetLocal(conf);
			}
			catch (IOException io)
			{
				throw new RuntimeException("problem getting local fs", io);
			}
		}

		private static Path TestRootDir = new Path("target", typeof(TestMRJobs).FullName 
			+ "-tmpDir").MakeQualified(localFs);

		internal static Path AppJar = new Path(TestRootDir, "MRAppJar.jar");

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			if (mrCluster == null)
			{
				mrCluster = new MiniMRYarnCluster(GetType().FullName);
				mrCluster.Init(new Configuration());
				mrCluster.Start();
			}
			// Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
			// workaround the absent public discache.
			localFs.CopyFromLocalFile(new Path(MiniMRYarnCluster.Appjar), AppJar);
			localFs.SetPermission(AppJar, new FsPermission("700"));
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			if (mrCluster != null)
			{
				mrCluster.Stop();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Avro.AvroRemoteException"/>
		/// <exception cref="System.TypeLoadException"/>
		public virtual void TestJobHistoryData()
		{
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			SleepJob sleepJob = new SleepJob();
			sleepJob.SetConf(mrCluster.GetConfig());
			// Job with 3 maps and 2 reduces
			Job job = sleepJob.CreateJob(3, 2, 1000, 1, 500, 1);
			job.SetJarByClass(typeof(SleepJob));
			job.AddFileToClassPath(AppJar);
			// The AppMaster jar itself.
			job.WaitForCompletion(true);
			Counters counterMR = job.GetCounters();
			JobId jobId = TypeConverter.ToYarn(job.GetJobID());
			ApplicationId appID = jobId.GetAppId();
			int pollElapsed = 0;
			while (true)
			{
				Sharpen.Thread.Sleep(1000);
				pollElapsed += 1000;
				if (TerminalRmAppStates.Contains(mrCluster.GetResourceManager().GetRMContext().GetRMApps
					()[appID].GetState()))
				{
					break;
				}
				if (pollElapsed >= 60000)
				{
					Log.Warn("application did not reach terminal state within 60 seconds");
					break;
				}
			}
			NUnit.Framework.Assert.AreEqual(RMAppState.Finished, mrCluster.GetResourceManager
				().GetRMContext().GetRMApps()[appID].GetState());
			Counters counterHS = job.GetCounters();
			//TODO the Assert below worked. need to check
			//Should we compare each field or convert to V2 counter and compare
			Log.Info("CounterHS " + counterHS);
			Log.Info("CounterMR " + counterMR);
			NUnit.Framework.Assert.AreEqual(counterHS, counterMR);
			HSClientProtocol historyClient = InstantiateHistoryProxy();
			GetJobReportRequest gjReq = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetJobReportRequest
				>();
			gjReq.SetJobId(jobId);
			JobReport jobReport = historyClient.GetJobReport(gjReq).GetJobReport();
			VerifyJobReport(jobReport, jobId);
		}

		private void VerifyJobReport(JobReport jobReport, JobId jobId)
		{
			IList<AMInfo> amInfos = jobReport.GetAMInfos();
			NUnit.Framework.Assert.AreEqual(1, amInfos.Count);
			AMInfo amInfo = amInfos[0];
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(jobId.GetAppId
				(), 1);
			ContainerId amContainerId = ContainerId.NewContainerId(appAttemptId, 1);
			NUnit.Framework.Assert.AreEqual(appAttemptId, amInfo.GetAppAttemptId());
			NUnit.Framework.Assert.AreEqual(amContainerId, amInfo.GetContainerId());
			NUnit.Framework.Assert.IsTrue(jobReport.GetSubmitTime() > 0);
			NUnit.Framework.Assert.IsTrue(jobReport.GetStartTime() > 0 && jobReport.GetStartTime
				() >= jobReport.GetSubmitTime());
			NUnit.Framework.Assert.IsTrue(jobReport.GetFinishTime() > 0 && jobReport.GetFinishTime
				() >= jobReport.GetStartTime());
		}

		private HSClientProtocol InstantiateHistoryProxy()
		{
			string serviceAddr = mrCluster.GetConfig().Get(JHAdminConfig.MrHistoryAddress);
			YarnRPC rpc = YarnRPC.Create(conf);
			HSClientProtocol historyClient = (HSClientProtocol)rpc.GetProxy(typeof(HSClientProtocol
				), NetUtils.CreateSocketAddr(serviceAddr), mrCluster.GetConfig());
			return historyClient;
		}
	}
}
