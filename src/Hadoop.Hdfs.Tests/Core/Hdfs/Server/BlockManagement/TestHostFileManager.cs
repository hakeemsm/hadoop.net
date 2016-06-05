using System.Collections.Generic;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestHostFileManager
	{
		private static IPEndPoint Entry(string e)
		{
			return HostFileManager.ParseEntry("dummy", "dummy", e);
		}

		[NUnit.Framework.Test]
		public virtual void TestDeduplication()
		{
			HostFileManager.HostSet s = new HostFileManager.HostSet();
			// These entries will be de-duped, since they refer to the same IP
			// address + port combo.
			s.Add(Entry("127.0.0.1:12345"));
			s.Add(Entry("localhost:12345"));
			NUnit.Framework.Assert.AreEqual(1, s.Size());
			s.Add(Entry("127.0.0.1:12345"));
			NUnit.Framework.Assert.AreEqual(1, s.Size());
			// The following entries should not be de-duped.
			s.Add(Entry("127.0.0.1:12346"));
			NUnit.Framework.Assert.AreEqual(2, s.Size());
			s.Add(Entry("127.0.0.1"));
			NUnit.Framework.Assert.AreEqual(3, s.Size());
			s.Add(Entry("127.0.0.10"));
			NUnit.Framework.Assert.AreEqual(4, s.Size());
		}

		[NUnit.Framework.Test]
		public virtual void TestRelation()
		{
			HostFileManager.HostSet s = new HostFileManager.HostSet();
			s.Add(Entry("127.0.0.1:123"));
			NUnit.Framework.Assert.IsTrue(s.Match(Entry("127.0.0.1:123")));
			NUnit.Framework.Assert.IsFalse(s.Match(Entry("127.0.0.1:12")));
			NUnit.Framework.Assert.IsFalse(s.Match(Entry("127.0.0.1")));
			NUnit.Framework.Assert.IsFalse(s.MatchedBy(Entry("127.0.0.1:12")));
			NUnit.Framework.Assert.IsTrue(s.MatchedBy(Entry("127.0.0.1")));
			NUnit.Framework.Assert.IsTrue(s.MatchedBy(Entry("127.0.0.1:123")));
			NUnit.Framework.Assert.IsFalse(s.Match(Entry("127.0.0.2")));
			NUnit.Framework.Assert.IsFalse(s.Match(Entry("127.0.0.2:123")));
			NUnit.Framework.Assert.IsFalse(s.MatchedBy(Entry("127.0.0.2")));
			NUnit.Framework.Assert.IsFalse(s.MatchedBy(Entry("127.0.0.2:123")));
			s.Add(Entry("127.0.0.1"));
			NUnit.Framework.Assert.IsTrue(s.Match(Entry("127.0.0.1:123")));
			NUnit.Framework.Assert.IsTrue(s.Match(Entry("127.0.0.1:12")));
			NUnit.Framework.Assert.IsTrue(s.Match(Entry("127.0.0.1")));
			NUnit.Framework.Assert.IsFalse(s.MatchedBy(Entry("127.0.0.1:12")));
			NUnit.Framework.Assert.IsTrue(s.MatchedBy(Entry("127.0.0.1")));
			NUnit.Framework.Assert.IsTrue(s.MatchedBy(Entry("127.0.0.1:123")));
			NUnit.Framework.Assert.IsFalse(s.Match(Entry("127.0.0.2")));
			NUnit.Framework.Assert.IsFalse(s.Match(Entry("127.0.0.2:123")));
			NUnit.Framework.Assert.IsFalse(s.MatchedBy(Entry("127.0.0.2")));
			NUnit.Framework.Assert.IsFalse(s.MatchedBy(Entry("127.0.0.2:123")));
			s.Add(Entry("127.0.0.2:123"));
			NUnit.Framework.Assert.IsTrue(s.Match(Entry("127.0.0.1:123")));
			NUnit.Framework.Assert.IsTrue(s.Match(Entry("127.0.0.1:12")));
			NUnit.Framework.Assert.IsTrue(s.Match(Entry("127.0.0.1")));
			NUnit.Framework.Assert.IsFalse(s.MatchedBy(Entry("127.0.0.1:12")));
			NUnit.Framework.Assert.IsTrue(s.MatchedBy(Entry("127.0.0.1")));
			NUnit.Framework.Assert.IsTrue(s.MatchedBy(Entry("127.0.0.1:123")));
			NUnit.Framework.Assert.IsFalse(s.Match(Entry("127.0.0.2")));
			NUnit.Framework.Assert.IsTrue(s.Match(Entry("127.0.0.2:123")));
			NUnit.Framework.Assert.IsTrue(s.MatchedBy(Entry("127.0.0.2")));
			NUnit.Framework.Assert.IsTrue(s.MatchedBy(Entry("127.0.0.2:123")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestIncludeExcludeLists()
		{
			BlockManager bm = Org.Mockito.Mockito.Mock<BlockManager>();
			FSNamesystem fsn = Org.Mockito.Mockito.Mock<FSNamesystem>();
			Configuration conf = new Configuration();
			HostFileManager hm = new HostFileManager();
			HostFileManager.HostSet includedNodes = new HostFileManager.HostSet();
			HostFileManager.HostSet excludedNodes = new HostFileManager.HostSet();
			includedNodes.Add(Entry("127.0.0.1:12345"));
			includedNodes.Add(Entry("localhost:12345"));
			includedNodes.Add(Entry("127.0.0.1:12345"));
			includedNodes.Add(Entry("127.0.0.2"));
			excludedNodes.Add(Entry("127.0.0.1:12346"));
			excludedNodes.Add(Entry("127.0.30.1:12346"));
			NUnit.Framework.Assert.AreEqual(2, includedNodes.Size());
			NUnit.Framework.Assert.AreEqual(2, excludedNodes.Size());
			hm.Refresh(includedNodes, excludedNodes);
			DatanodeManager dm = new DatanodeManager(bm, fsn, conf);
			Whitebox.SetInternalState(dm, "hostFileManager", hm);
			IDictionary<string, DatanodeDescriptor> dnMap = (IDictionary<string, DatanodeDescriptor
				>)Whitebox.GetInternalState(dm, "datanodeMap");
			// After the de-duplication, there should be only one DN from the included
			// nodes declared as dead.
			NUnit.Framework.Assert.AreEqual(2, dm.GetDatanodeListForReport(HdfsConstants.DatanodeReportType
				.All).Count);
			NUnit.Framework.Assert.AreEqual(2, dm.GetDatanodeListForReport(HdfsConstants.DatanodeReportType
				.Dead).Count);
			dnMap["uuid-foo"] = new DatanodeDescriptor(new DatanodeID("127.0.0.1", "localhost"
				, "uuid-foo", 12345, 1020, 1021, 1022));
			NUnit.Framework.Assert.AreEqual(1, dm.GetDatanodeListForReport(HdfsConstants.DatanodeReportType
				.Dead).Count);
			dnMap["uuid-bar"] = new DatanodeDescriptor(new DatanodeID("127.0.0.2", "127.0.0.2"
				, "uuid-bar", 12345, 1020, 1021, 1022));
			NUnit.Framework.Assert.AreEqual(0, dm.GetDatanodeListForReport(HdfsConstants.DatanodeReportType
				.Dead).Count);
			DatanodeDescriptor spam = new DatanodeDescriptor(new DatanodeID("127.0.0" + ".3", 
				"127.0.0.3", "uuid-spam", 12345, 1020, 1021, 1022));
			DFSTestUtil.SetDatanodeDead(spam);
			includedNodes.Add(Entry("127.0.0.3:12345"));
			dnMap["uuid-spam"] = spam;
			NUnit.Framework.Assert.AreEqual(1, dm.GetDatanodeListForReport(HdfsConstants.DatanodeReportType
				.Dead).Count);
			Sharpen.Collections.Remove(dnMap, "uuid-spam");
			NUnit.Framework.Assert.AreEqual(1, dm.GetDatanodeListForReport(HdfsConstants.DatanodeReportType
				.Dead).Count);
			excludedNodes.Add(Entry("127.0.0.3"));
			NUnit.Framework.Assert.AreEqual(0, dm.GetDatanodeListForReport(HdfsConstants.DatanodeReportType
				.Dead).Count);
		}
	}
}
