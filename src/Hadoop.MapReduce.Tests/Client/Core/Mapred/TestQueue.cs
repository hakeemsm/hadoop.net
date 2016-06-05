using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>TestCounters checks the sanity and recoverability of Queue</summary>
	public class TestQueue
	{
		private static FilePath testDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp"), typeof(TestJobConf).Name);

		[SetUp]
		public virtual void Setup()
		{
			testDir.Mkdirs();
		}

		[TearDown]
		public virtual void Cleanup()
		{
			FileUtil.FullyDelete(testDir);
		}

		/// <summary>
		/// test QueueManager
		/// configuration from file
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestQueue()
		{
			FilePath f = null;
			try
			{
				f = WriteFile();
				QueueManager manager = new QueueManager(f.GetCanonicalPath(), true);
				manager.SetSchedulerInfo("first", "queueInfo");
				manager.SetSchedulerInfo("second", "queueInfoqueueInfo");
				Queue root = manager.GetRoot();
				NUnit.Framework.Assert.IsTrue(root.GetChildren().Count == 2);
				IEnumerator<Queue> iterator = root.GetChildren().GetEnumerator();
				Queue firstSubQueue = iterator.Next();
				NUnit.Framework.Assert.IsTrue(firstSubQueue.GetName().Equals("first"));
				NUnit.Framework.Assert.AreEqual(firstSubQueue.GetAcls()["mapred.queue.first.acl-submit-job"
					].ToString(), "Users [user1, user2] and members of the groups [group1, group2] are allowed"
					);
				Queue secondSubQueue = iterator.Next();
				NUnit.Framework.Assert.IsTrue(secondSubQueue.GetName().Equals("second"));
				NUnit.Framework.Assert.AreEqual(secondSubQueue.GetProperties().GetProperty("key")
					, "value");
				NUnit.Framework.Assert.AreEqual(secondSubQueue.GetProperties().GetProperty("key1"
					), "value1");
				// test status
				NUnit.Framework.Assert.AreEqual(firstSubQueue.GetState().GetStateName(), "running"
					);
				NUnit.Framework.Assert.AreEqual(secondSubQueue.GetState().GetStateName(), "stopped"
					);
				ICollection<string> template = new HashSet<string>();
				template.AddItem("first");
				template.AddItem("second");
				NUnit.Framework.Assert.AreEqual(manager.GetLeafQueueNames(), template);
				// test user access
				UserGroupInformation mockUGI = Org.Mockito.Mockito.Mock<UserGroupInformation>();
				Org.Mockito.Mockito.When(mockUGI.GetShortUserName()).ThenReturn("user1");
				string[] groups = new string[] { "group1" };
				Org.Mockito.Mockito.When(mockUGI.GetGroupNames()).ThenReturn(groups);
				NUnit.Framework.Assert.IsTrue(manager.HasAccess("first", QueueACL.SubmitJob, mockUGI
					));
				NUnit.Framework.Assert.IsFalse(manager.HasAccess("second", QueueACL.SubmitJob, mockUGI
					));
				NUnit.Framework.Assert.IsFalse(manager.HasAccess("first", QueueACL.AdministerJobs
					, mockUGI));
				Org.Mockito.Mockito.When(mockUGI.GetShortUserName()).ThenReturn("user3");
				NUnit.Framework.Assert.IsTrue(manager.HasAccess("first", QueueACL.AdministerJobs, 
					mockUGI));
				QueueAclsInfo[] qai = manager.GetQueueAcls(mockUGI);
				NUnit.Framework.Assert.AreEqual(qai.Length, 1);
				// test refresh queue
				manager.RefreshQueues(GetConfiguration(), null);
				iterator = root.GetChildren().GetEnumerator();
				Queue firstSubQueue1 = iterator.Next();
				Queue secondSubQueue1 = iterator.Next();
				// tets equal method
				NUnit.Framework.Assert.IsTrue(firstSubQueue.Equals(firstSubQueue1));
				NUnit.Framework.Assert.AreEqual(firstSubQueue1.GetState().GetStateName(), "running"
					);
				NUnit.Framework.Assert.AreEqual(secondSubQueue1.GetState().GetStateName(), "stopped"
					);
				NUnit.Framework.Assert.AreEqual(firstSubQueue1.GetSchedulingInfo(), "queueInfo");
				NUnit.Framework.Assert.AreEqual(secondSubQueue1.GetSchedulingInfo(), "queueInfoqueueInfo"
					);
				// test JobQueueInfo
				NUnit.Framework.Assert.AreEqual(firstSubQueue.GetJobQueueInfo().GetQueueName(), "first"
					);
				NUnit.Framework.Assert.AreEqual(firstSubQueue.GetJobQueueInfo().GetQueueState(), 
					"running");
				NUnit.Framework.Assert.AreEqual(firstSubQueue.GetJobQueueInfo().GetSchedulingInfo
					(), "queueInfo");
				NUnit.Framework.Assert.AreEqual(secondSubQueue.GetJobQueueInfo().GetChildren().Count
					, 0);
				// test
				NUnit.Framework.Assert.AreEqual(manager.GetSchedulerInfo("first"), "queueInfo");
				ICollection<string> queueJobQueueInfos = new HashSet<string>();
				foreach (JobQueueInfo jobInfo in manager.GetJobQueueInfos())
				{
					queueJobQueueInfos.AddItem(jobInfo.GetQueueName());
				}
				ICollection<string> rootJobQueueInfos = new HashSet<string>();
				foreach (Queue queue in root.GetChildren())
				{
					rootJobQueueInfos.AddItem(queue.GetJobQueueInfo().GetQueueName());
				}
				NUnit.Framework.Assert.AreEqual(queueJobQueueInfos, rootJobQueueInfos);
				// test getJobQueueInfoMapping
				NUnit.Framework.Assert.AreEqual(manager.GetJobQueueInfoMapping()["first"].GetQueueName
					(), "first");
				// test dumpConfiguration
				TextWriter writer = new StringWriter();
				Configuration conf = GetConfiguration();
				conf.Unset(DeprecatedQueueConfigurationParser.MapredQueueNamesKey);
				QueueManager.DumpConfiguration(writer, f.GetAbsolutePath(), conf);
				string result = writer.ToString();
				NUnit.Framework.Assert.IsTrue(result.IndexOf("\"name\":\"first\",\"state\":\"running\",\"acl_submit_job\":\"user1,user2 group1,group2\",\"acl_administer_jobs\":\"user3,user4 group3,group4\",\"properties\":[],\"children\":[]"
					) > 0);
				writer = new StringWriter();
				QueueManager.DumpConfiguration(writer, conf);
				result = writer.ToString();
				NUnit.Framework.Assert.AreEqual("{\"queues\":[{\"name\":\"default\",\"state\":\"running\",\"acl_submit_job\":\"*\",\"acl_administer_jobs\":\"*\",\"properties\":[],\"children\":[]},{\"name\":\"q1\",\"state\":\"running\",\"acl_submit_job\":\" \",\"acl_administer_jobs\":\" \",\"properties\":[],\"children\":[{\"name\":\"q1:q2\",\"state\":\"running\",\"acl_submit_job\":\" \",\"acl_administer_jobs\":\" \",\"properties\":[{\"key\":\"capacity\",\"value\":\"20\"},{\"key\":\"user-limit\",\"value\":\"30\"}],\"children\":[]}]}]}"
					, result);
				// test constructor QueueAclsInfo
				QueueAclsInfo qi = new QueueAclsInfo();
				NUnit.Framework.Assert.IsNull(qi.GetQueueName());
			}
			finally
			{
				if (f != null)
				{
					f.Delete();
				}
			}
		}

		private Configuration GetConfiguration()
		{
			Configuration conf = new Configuration();
			conf.Set(DeprecatedQueueConfigurationParser.MapredQueueNamesKey, "first,second");
			conf.Set(QueueManager.QueueConfPropertyNamePrefix + "first.acl-submit-job", "user1,user2 group1,group2"
				);
			conf.Set(MRConfig.MrAclsEnabled, "true");
			conf.Set(QueueManager.QueueConfPropertyNamePrefix + "first.state", "running");
			conf.Set(QueueManager.QueueConfPropertyNamePrefix + "second.state", "stopped");
			return conf;
		}

		public virtual void TestDefaultConfig()
		{
			QueueManager manager = new QueueManager(true);
			NUnit.Framework.Assert.AreEqual(manager.GetRoot().GetChildren().Count, 2);
		}

		/// <summary>test for Qmanager with empty configuration</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Test2Queue()
		{
			Configuration conf = GetConfiguration();
			QueueManager manager = new QueueManager(conf);
			manager.SetSchedulerInfo("first", "queueInfo");
			manager.SetSchedulerInfo("second", "queueInfoqueueInfo");
			Queue root = manager.GetRoot();
			// test children queues
			NUnit.Framework.Assert.IsTrue(root.GetChildren().Count == 2);
			IEnumerator<Queue> iterator = root.GetChildren().GetEnumerator();
			Queue firstSubQueue = iterator.Next();
			NUnit.Framework.Assert.IsTrue(firstSubQueue.GetName().Equals("first"));
			NUnit.Framework.Assert.AreEqual(firstSubQueue.GetAcls()["mapred.queue.first.acl-submit-job"
				].ToString(), "Users [user1, user2] and members of the groups [group1, group2] are allowed"
				);
			Queue secondSubQueue = iterator.Next();
			NUnit.Framework.Assert.IsTrue(secondSubQueue.GetName().Equals("second"));
			NUnit.Framework.Assert.AreEqual(firstSubQueue.GetState().GetStateName(), "running"
				);
			NUnit.Framework.Assert.AreEqual(secondSubQueue.GetState().GetStateName(), "stopped"
				);
			NUnit.Framework.Assert.IsTrue(manager.IsRunning("first"));
			NUnit.Framework.Assert.IsFalse(manager.IsRunning("second"));
			NUnit.Framework.Assert.AreEqual(firstSubQueue.GetSchedulingInfo(), "queueInfo");
			NUnit.Framework.Assert.AreEqual(secondSubQueue.GetSchedulingInfo(), "queueInfoqueueInfo"
				);
			// test leaf queue
			ICollection<string> template = new HashSet<string>();
			template.AddItem("first");
			template.AddItem("second");
			NUnit.Framework.Assert.AreEqual(manager.GetLeafQueueNames(), template);
		}

		/// <summary>write cofiguration</summary>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		private FilePath WriteFile()
		{
			FilePath f = new FilePath(testDir, "tst.xml");
			BufferedWriter @out = new BufferedWriter(new FileWriter(f));
			string properties = "<properties><property key=\"key\" value=\"value\"/><property key=\"key1\" value=\"value1\"/></properties>";
			@out.Write("<queues>");
			@out.NewLine();
			@out.Write("<queue><name>first</name><acl-submit-job>user1,user2 group1,group2</acl-submit-job><acl-administer-jobs>user3,user4 group3,group4</acl-administer-jobs><state>running</state></queue>"
				);
			@out.NewLine();
			@out.Write("<queue><name>second</name><acl-submit-job>u1,u2 g1,g2</acl-submit-job>"
				 + properties + "<state>stopped</state></queue>");
			@out.NewLine();
			@out.Write("</queues>");
			@out.Flush();
			@out.Close();
			return f;
		}
	}
}
