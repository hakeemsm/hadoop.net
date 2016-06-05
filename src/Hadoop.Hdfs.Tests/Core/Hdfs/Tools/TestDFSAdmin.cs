using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Hamcrest;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	public class TestDFSAdmin
	{
		private MiniDFSCluster cluster;

		private DFSAdmin admin;

		private DataNode datanode;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Configuration conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			admin = new DFSAdmin();
			datanode = cluster.GetDataNodes()[0];
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
				cluster = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private IList<string> GetReconfigureStatus(string nodeType, string address)
		{
			ByteArrayOutputStream bufOut = new ByteArrayOutputStream();
			TextWriter @out = new TextWriter(bufOut);
			ByteArrayOutputStream bufErr = new ByteArrayOutputStream();
			TextWriter err = new TextWriter(bufErr);
			admin.GetReconfigurationStatus(nodeType, address, @out, err);
			Scanner scanner = new Scanner(bufOut.ToString());
			IList<string> outputs = Lists.NewArrayList();
			while (scanner.HasNextLine())
			{
				outputs.AddItem(scanner.NextLine());
			}
			return outputs;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetReconfigureStatus()
		{
			ReconfigurationUtil ru = Org.Mockito.Mockito.Mock<ReconfigurationUtil>();
			datanode.SetReconfigurationUtil(ru);
			IList<ReconfigurationUtil.PropertyChange> changes = new AList<ReconfigurationUtil.PropertyChange
				>();
			FilePath newDir = new FilePath(cluster.GetDataDirectory(), "data_new");
			newDir.Mkdirs();
			changes.AddItem(new ReconfigurationUtil.PropertyChange(DFSConfigKeys.DfsDatanodeDataDirKey
				, newDir.ToString(), datanode.GetConf().Get(DFSConfigKeys.DfsDatanodeDataDirKey)
				));
			changes.AddItem(new ReconfigurationUtil.PropertyChange("randomKey", "new123", "old456"
				));
			Org.Mockito.Mockito.When(ru.ParseChangedProperties(Matchers.Any<Configuration>(), 
				Matchers.Any<Configuration>())).ThenReturn(changes);
			int port = datanode.GetIpcPort();
			string address = "localhost:" + port;
			Assert.AssertThat(admin.StartReconfiguration("datanode", address), CoreMatchers.Is
				(0));
			IList<string> outputs = null;
			int count = 100;
			while (count > 0)
			{
				outputs = GetReconfigureStatus("datanode", address);
				if (!outputs.IsEmpty() && outputs[0].Contains("finished"))
				{
					break;
				}
				count--;
				Sharpen.Thread.Sleep(100);
			}
			NUnit.Framework.Assert.IsTrue(count > 0);
			Assert.AssertThat(outputs.Count, CoreMatchers.Is(8));
			// 3 (SUCCESS) + 4 (FAILED)
			IList<StorageLocation> locations = DataNode.GetStorageLocations(datanode.GetConf(
				));
			Assert.AssertThat(locations.Count, CoreMatchers.Is(1));
			Assert.AssertThat(locations[0].GetFile(), CoreMatchers.Is(newDir));
			// Verify the directory is appropriately formatted.
			NUnit.Framework.Assert.IsTrue(new FilePath(newDir, Storage.StorageDirCurrent).IsDirectory
				());
			int successOffset = outputs[1].StartsWith("SUCCESS:") ? 1 : 5;
			int failedOffset = outputs[1].StartsWith("FAILED:") ? 1 : 4;
			Assert.AssertThat(outputs[successOffset], CoreMatchers.ContainsString("Change property "
				 + DFSConfigKeys.DfsDatanodeDataDirKey));
			Assert.AssertThat(outputs[successOffset + 1], CoreMatchers.Is(CoreMatchers.AllOf(
				CoreMatchers.ContainsString("From:"), CoreMatchers.ContainsString("data1"), CoreMatchers.ContainsString
				("data2"))));
			Assert.AssertThat(outputs[successOffset + 2], CoreMatchers.Is(CoreMatchers.Not(CoreMatchers.AnyOf
				(CoreMatchers.ContainsString("data1"), CoreMatchers.ContainsString("data2")))));
			Assert.AssertThat(outputs[successOffset + 2], CoreMatchers.Is(CoreMatchers.AllOf(
				CoreMatchers.ContainsString("To"), CoreMatchers.ContainsString("data_new"))));
			Assert.AssertThat(outputs[failedOffset], CoreMatchers.ContainsString("Change property randomKey"
				));
			Assert.AssertThat(outputs[failedOffset + 1], CoreMatchers.ContainsString("From: \"old456\""
				));
			Assert.AssertThat(outputs[failedOffset + 2], CoreMatchers.ContainsString("To: \"new123\""
				));
		}
	}
}
