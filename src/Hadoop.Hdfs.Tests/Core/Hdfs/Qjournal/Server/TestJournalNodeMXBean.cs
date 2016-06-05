using System.Collections.Generic;
using Javax.Management;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Qjournal;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Mortbay.Util.Ajax;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Server
{
	/// <summary>
	/// Test
	/// <see cref="JournalNodeMXBean"/>
	/// </summary>
	public class TestJournalNodeMXBean
	{
		private const string Nameservice = "ns1";

		private const int NumJn = 1;

		private MiniJournalCluster jCluster;

		private JournalNode jn;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			// start 1 journal node
			jCluster = new MiniJournalCluster.Builder(new Configuration()).Format(true).NumJournalNodes
				(NumJn).Build();
			jn = jCluster.GetJournalNode(0);
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Cleanup()
		{
			if (jCluster != null)
			{
				jCluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJournalNodeMXBean()
		{
			// we have not formatted the journals yet, and the journal status in jmx
			// should be empty since journal objects are created lazily
			MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
			ObjectName mxbeanName = new ObjectName("Hadoop:service=JournalNode,name=JournalNodeInfo"
				);
			// getJournalsStatus
			string journalStatus = (string)mbs.GetAttribute(mxbeanName, "JournalsStatus");
			NUnit.Framework.Assert.AreEqual(jn.GetJournalsStatus(), journalStatus);
			NUnit.Framework.Assert.IsFalse(journalStatus.Contains(Nameservice));
			// format the journal ns1
			NamespaceInfo FakeNsinfo = new NamespaceInfo(12345, "mycluster", "my-bp", 0L);
			jn.GetOrCreateJournal(Nameservice).Format(FakeNsinfo);
			// check again after format
			// getJournalsStatus
			journalStatus = (string)mbs.GetAttribute(mxbeanName, "JournalsStatus");
			NUnit.Framework.Assert.AreEqual(jn.GetJournalsStatus(), journalStatus);
			IDictionary<string, IDictionary<string, string>> jMap = new Dictionary<string, IDictionary
				<string, string>>();
			IDictionary<string, string> infoMap = new Dictionary<string, string>();
			infoMap["Formatted"] = "true";
			jMap[Nameservice] = infoMap;
			NUnit.Framework.Assert.AreEqual(JSON.ToString(jMap), journalStatus);
			// restart journal node without formatting
			jCluster = new MiniJournalCluster.Builder(new Configuration()).Format(false).NumJournalNodes
				(NumJn).Build();
			jn = jCluster.GetJournalNode(0);
			// re-check 
			journalStatus = (string)mbs.GetAttribute(mxbeanName, "JournalsStatus");
			NUnit.Framework.Assert.AreEqual(jn.GetJournalsStatus(), journalStatus);
			jMap = new Dictionary<string, IDictionary<string, string>>();
			infoMap = new Dictionary<string, string>();
			infoMap["Formatted"] = "true";
			jMap[Nameservice] = infoMap;
			NUnit.Framework.Assert.AreEqual(JSON.ToString(jMap), journalStatus);
		}
	}
}
