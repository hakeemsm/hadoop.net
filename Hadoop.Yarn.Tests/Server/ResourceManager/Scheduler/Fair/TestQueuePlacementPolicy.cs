using System;
using System.Collections.Generic;
using System.Text;
using Javax.Xml.Parsers;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.W3c.Dom;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class TestQueuePlacementPolicy
	{
		private static readonly Configuration conf = new Configuration();

		private IDictionary<FSQueueType, ICollection<string>> configuredQueues;

		[BeforeClass]
		public static void Setup()
		{
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(SimpleGroupsMapping
				), typeof(GroupMappingServiceProvider));
		}

		[SetUp]
		public virtual void InitTest()
		{
			configuredQueues = new Dictionary<FSQueueType, ICollection<string>>();
			foreach (FSQueueType type in FSQueueType.Values())
			{
				configuredQueues[type] = new HashSet<string>();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSpecifiedUserPolicy()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='specified' />");
			sb.Append("  <rule name='user' />");
			sb.Append("</queuePlacementPolicy>");
			QueuePlacementPolicy policy = Parse(sb.ToString());
			NUnit.Framework.Assert.AreEqual("root.specifiedq", policy.AssignAppToQueue("specifiedq"
				, "someuser"));
			NUnit.Framework.Assert.AreEqual("root.someuser", policy.AssignAppToQueue("default"
				, "someuser"));
			NUnit.Framework.Assert.AreEqual("root.otheruser", policy.AssignAppToQueue("default"
				, "otheruser"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoCreate()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='specified' />");
			sb.Append("  <rule name='user' create=\"false\" />");
			sb.Append("  <rule name='default' />");
			sb.Append("</queuePlacementPolicy>");
			configuredQueues[FSQueueType.Leaf].AddItem("root.someuser");
			QueuePlacementPolicy policy = Parse(sb.ToString());
			NUnit.Framework.Assert.AreEqual("root.specifiedq", policy.AssignAppToQueue("specifiedq"
				, "someuser"));
			NUnit.Framework.Assert.AreEqual("root.someuser", policy.AssignAppToQueue("default"
				, "someuser"));
			NUnit.Framework.Assert.AreEqual("root.specifiedq", policy.AssignAppToQueue("specifiedq"
				, "otheruser"));
			NUnit.Framework.Assert.AreEqual("root.default", policy.AssignAppToQueue("default"
				, "otheruser"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSpecifiedThenReject()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='specified' />");
			sb.Append("  <rule name='reject' />");
			sb.Append("</queuePlacementPolicy>");
			QueuePlacementPolicy policy = Parse(sb.ToString());
			NUnit.Framework.Assert.AreEqual("root.specifiedq", policy.AssignAppToQueue("specifiedq"
				, "someuser"));
			NUnit.Framework.Assert.AreEqual(null, policy.AssignAppToQueue("default", "someuser"
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestOmittedTerminalRule()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='specified' />");
			sb.Append("  <rule name='user' create=\"false\" />");
			sb.Append("</queuePlacementPolicy>");
			Parse(sb.ToString());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTerminalRuleInMiddle()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='specified' />");
			sb.Append("  <rule name='default' />");
			sb.Append("  <rule name='user' />");
			sb.Append("</queuePlacementPolicy>");
			Parse(sb.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTerminals()
		{
			// Should make it through without an exception
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='secondaryGroupExistingQueue' create='true'/>");
			sb.Append("  <rule name='default' create='false'/>");
			sb.Append("</queuePlacementPolicy>");
			Parse(sb.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultRuleWithQueueAttribute()
		{
			// This test covers the use case where we would like default rule
			// to point to a different queue by default rather than root.default
			configuredQueues[FSQueueType.Leaf].AddItem("root.someDefaultQueue");
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='specified' create='false' />");
			sb.Append("  <rule name='default' queue='root.someDefaultQueue'/>");
			sb.Append("</queuePlacementPolicy>");
			QueuePlacementPolicy policy = Parse(sb.ToString());
			NUnit.Framework.Assert.AreEqual("root.someDefaultQueue", policy.AssignAppToQueue(
				"root.default", "user1"));
		}

		[NUnit.Framework.Test]
		public virtual void TestNestedUserQueueParsingErrors()
		{
			// No nested rule specified in hierarchical user queue
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='specified' />");
			sb.Append("  <rule name='nestedUserQueue'/>");
			sb.Append("  <rule name='default' />");
			sb.Append("</queuePlacementPolicy>");
			AssertIfExceptionThrown(sb);
			// Specified nested rule is not a QueuePlacementRule
			sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='specified' />");
			sb.Append("  <rule name='nestedUserQueue'>");
			sb.Append("       <rule name='unknownRule'/>");
			sb.Append("  </rule>");
			sb.Append("  <rule name='default' />");
			sb.Append("</queuePlacementPolicy>");
			AssertIfExceptionThrown(sb);
		}

		private void AssertIfExceptionThrown(StringBuilder sb)
		{
			Exception th = null;
			try
			{
				Parse(sb.ToString());
			}
			catch (Exception e)
			{
				th = e;
			}
			NUnit.Framework.Assert.IsTrue(th is AllocationConfigurationException);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNestedUserQueueParsing()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='specified' />");
			sb.Append("  <rule name='nestedUserQueue'>");
			sb.Append("       <rule name='primaryGroup'/>");
			sb.Append("  </rule>");
			sb.Append("  <rule name='default' />");
			sb.Append("</queuePlacementPolicy>");
			Exception th = null;
			try
			{
				Parse(sb.ToString());
			}
			catch (Exception e)
			{
				th = e;
			}
			NUnit.Framework.Assert.IsNull(th);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNestedUserQueuePrimaryGroup()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='specified' create='false' />");
			sb.Append("  <rule name='nestedUserQueue'>");
			sb.Append("       <rule name='primaryGroup'/>");
			sb.Append("  </rule>");
			sb.Append("  <rule name='default' />");
			sb.Append("</queuePlacementPolicy>");
			// User queue would be created under primary group queue
			QueuePlacementPolicy policy = Parse(sb.ToString());
			NUnit.Framework.Assert.AreEqual("root.user1group.user1", policy.AssignAppToQueue(
				"root.default", "user1"));
			// Other rules above and below hierarchical user queue rule should work as
			// usual
			configuredQueues[FSQueueType.Leaf].AddItem("root.specifiedq");
			// test if specified rule(above nestedUserQueue rule) works ok
			NUnit.Framework.Assert.AreEqual("root.specifiedq", policy.AssignAppToQueue("root.specifiedq"
				, "user2"));
			// test if default rule(below nestedUserQueue rule) works
			configuredQueues[FSQueueType.Leaf].AddItem("root.user3group");
			NUnit.Framework.Assert.AreEqual("root.default", policy.AssignAppToQueue("root.default"
				, "user3"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNestedUserQueuePrimaryGroupNoCreate()
		{
			// Primary group rule has create='false'
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='nestedUserQueue'>");
			sb.Append("       <rule name='primaryGroup' create='false'/>");
			sb.Append("  </rule>");
			sb.Append("  <rule name='default' />");
			sb.Append("</queuePlacementPolicy>");
			QueuePlacementPolicy policy = Parse(sb.ToString());
			// Should return root.default since primary group 'root.user1group' is not
			// configured
			NUnit.Framework.Assert.AreEqual("root.default", policy.AssignAppToQueue("root.default"
				, "user1"));
			// Let's configure primary group and check if user queue is created
			configuredQueues[FSQueueType.Parent].AddItem("root.user1group");
			policy = Parse(sb.ToString());
			NUnit.Framework.Assert.AreEqual("root.user1group.user1", policy.AssignAppToQueue(
				"root.default", "user1"));
			// Both Primary group and nestedUserQueue rule has create='false'
			sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='nestedUserQueue' create='false'>");
			sb.Append("       <rule name='primaryGroup' create='false'/>");
			sb.Append("  </rule>");
			sb.Append("  <rule name='default' />");
			sb.Append("</queuePlacementPolicy>");
			// Should return root.default since primary group and user queue for user 2
			// are not configured.
			NUnit.Framework.Assert.AreEqual("root.default", policy.AssignAppToQueue("root.default"
				, "user2"));
			// Now configure both primary group and the user queue for user2
			configuredQueues[FSQueueType.Parent].AddItem("root.user2group");
			configuredQueues[FSQueueType.Leaf].AddItem("root.user2group.user2");
			policy = Parse(sb.ToString());
			NUnit.Framework.Assert.AreEqual("root.user2group.user2", policy.AssignAppToQueue(
				"root.default", "user2"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNestedUserQueueSecondaryGroup()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='nestedUserQueue'>");
			sb.Append("       <rule name='secondaryGroupExistingQueue'/>");
			sb.Append("  </rule>");
			sb.Append("  <rule name='default' />");
			sb.Append("</queuePlacementPolicy>");
			QueuePlacementPolicy policy = Parse(sb.ToString());
			// Should return root.default since secondary groups are not configured
			NUnit.Framework.Assert.AreEqual("root.default", policy.AssignAppToQueue("root.default"
				, "user1"));
			// configure secondary group for user1
			configuredQueues[FSQueueType.Parent].AddItem("root.user1subgroup1");
			policy = Parse(sb.ToString());
			// user queue created should be created under secondary group
			NUnit.Framework.Assert.AreEqual("root.user1subgroup1.user1", policy.AssignAppToQueue
				("root.default", "user1"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNestedUserQueueSpecificRule()
		{
			// This test covers the use case where users can specify different parent
			// queues and want user queues under those.
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='nestedUserQueue'>");
			sb.Append("       <rule name='specified' create='false'/>");
			sb.Append("  </rule>");
			sb.Append("  <rule name='default' />");
			sb.Append("</queuePlacementPolicy>");
			// Let's create couple of parent queues
			configuredQueues[FSQueueType.Parent].AddItem("root.parent1");
			configuredQueues[FSQueueType.Parent].AddItem("root.parent2");
			QueuePlacementPolicy policy = Parse(sb.ToString());
			NUnit.Framework.Assert.AreEqual("root.parent1.user1", policy.AssignAppToQueue("root.parent1"
				, "user1"));
			NUnit.Framework.Assert.AreEqual("root.parent2.user2", policy.AssignAppToQueue("root.parent2"
				, "user2"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNestedUserQueueDefaultRule()
		{
			// This test covers the use case where we would like user queues to be
			// created under a default parent queue
			configuredQueues[FSQueueType.Parent].AddItem("root.parentq");
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='specified' create='false' />");
			sb.Append("  <rule name='nestedUserQueue'>");
			sb.Append("       <rule name='default' queue='root.parentq'/>");
			sb.Append("  </rule>");
			sb.Append("  <rule name='default' />");
			sb.Append("</queuePlacementPolicy>");
			QueuePlacementPolicy policy = Parse(sb.ToString());
			NUnit.Framework.Assert.AreEqual("root.parentq.user1", policy.AssignAppToQueue("root.default"
				, "user1"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUserContainsPeriod()
		{
			// This test covers the user case where the username contains periods.
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='user' />");
			sb.Append("</queuePlacementPolicy>");
			QueuePlacementPolicy policy = Parse(sb.ToString());
			NUnit.Framework.Assert.AreEqual("root.first_dot_last", policy.AssignAppToQueue("default"
				, "first.last"));
			sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='specified' create='false' />");
			sb.Append("  <rule name='nestedUserQueue'>");
			sb.Append("       <rule name='default'/>");
			sb.Append("  </rule>");
			sb.Append("  <rule name='default' />");
			sb.Append("</queuePlacementPolicy>");
			policy = Parse(sb.ToString());
			NUnit.Framework.Assert.AreEqual("root.default.first_dot_last", policy.AssignAppToQueue
				("root.default", "first.last"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGroupContainsPeriod()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("<queuePlacementPolicy>");
			sb.Append("  <rule name='specified' create='false' />");
			sb.Append("  <rule name='nestedUserQueue'>");
			sb.Append("       <rule name='primaryGroup'/>");
			sb.Append("  </rule>");
			sb.Append("  <rule name='default' />");
			sb.Append("</queuePlacementPolicy>");
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(PeriodGroupsMapping
				), typeof(GroupMappingServiceProvider));
			// User queue would be created under primary group queue, and the period
			// in the group name should be converted into _dot_
			QueuePlacementPolicy policy = Parse(sb.ToString());
			NUnit.Framework.Assert.AreEqual("root.user1_dot_group.user1", policy.AssignAppToQueue
				("root.default", "user1"));
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(SimpleGroupsMapping
				), typeof(GroupMappingServiceProvider));
		}

		/// <exception cref="System.Exception"/>
		private QueuePlacementPolicy Parse(string str)
		{
			// Read and parse the allocations file.
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.NewInstance();
			docBuilderFactory.SetIgnoringComments(true);
			DocumentBuilder builder = docBuilderFactory.NewDocumentBuilder();
			Document doc = builder.Parse(IOUtils.ToInputStream(str));
			Element root = doc.GetDocumentElement();
			return QueuePlacementPolicy.FromXml(root, configuredQueues, conf);
		}
	}
}
