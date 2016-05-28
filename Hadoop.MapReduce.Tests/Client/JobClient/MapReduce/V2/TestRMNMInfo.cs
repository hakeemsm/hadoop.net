using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Codehaus.Jackson;
using Org.Codehaus.Jackson.Map;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class TestRMNMInfo
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.TestRMNMInfo
			));

		private const int Numnodemanagers = 4;

		protected internal static MiniMRYarnCluster mrCluster;

		private static Configuration initialConf = new Configuration();

		private static FileSystem localFs;

		static TestRMNMInfo()
		{
			try
			{
				localFs = FileSystem.GetLocal(initialConf);
			}
			catch (IOException io)
			{
				throw new RuntimeException("problem getting local fs", io);
			}
		}

		private static Path TestRootDir = new Path("target", typeof(Org.Apache.Hadoop.Mapreduce.V2.TestRMNMInfo
			).FullName + "-tmpDir").MakeQualified(localFs.GetUri(), localFs.GetWorkingDirectory
			());

		internal static Path AppJar = new Path(TestRootDir, "MRAppJar.jar");

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			if (mrCluster == null)
			{
				mrCluster = new MiniMRYarnCluster(typeof(Org.Apache.Hadoop.Mapreduce.V2.TestRMNMInfo
					).FullName, Numnodemanagers);
				Configuration conf = new Configuration();
				mrCluster.Init(conf);
				mrCluster.Start();
			}
			// workaround the absent public distcache.
			localFs.CopyFromLocalFile(new Path(MiniMRYarnCluster.Appjar), AppJar);
			localFs.SetPermission(AppJar, new FsPermission("700"));
		}

		[AfterClass]
		public static void TearDown()
		{
			if (mrCluster != null)
			{
				mrCluster.Stop();
				mrCluster = null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMNMInfo()
		{
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			RMContext rmc = mrCluster.GetResourceManager().GetRMContext();
			ResourceScheduler rms = mrCluster.GetResourceManager().GetResourceScheduler();
			RMNMInfo rmInfo = new RMNMInfo(rmc, rms);
			string liveNMs = rmInfo.GetLiveNodeManagers();
			ObjectMapper mapper = new ObjectMapper();
			JsonNode jn = mapper.ReadTree(liveNMs);
			NUnit.Framework.Assert.AreEqual("Unexpected number of live nodes:", Numnodemanagers
				, jn.Size());
			IEnumerator<JsonNode> it = jn.GetEnumerator();
			while (it.HasNext())
			{
				JsonNode n = it.Next();
				NUnit.Framework.Assert.IsNotNull(n.Get("HostName"));
				NUnit.Framework.Assert.IsNotNull(n.Get("Rack"));
				NUnit.Framework.Assert.IsTrue("Node " + n.Get("NodeId") + " should be RUNNING", n
					.Get("State").AsText().Contains("RUNNING"));
				NUnit.Framework.Assert.IsNotNull(n.Get("NodeHTTPAddress"));
				NUnit.Framework.Assert.IsNotNull(n.Get("LastHealthUpdate"));
				NUnit.Framework.Assert.IsNotNull(n.Get("HealthReport"));
				NUnit.Framework.Assert.IsNotNull(n.Get("NodeManagerVersion"));
				NUnit.Framework.Assert.IsNotNull(n.Get("NumContainers"));
				NUnit.Framework.Assert.AreEqual(n.Get("NodeId") + ": Unexpected number of used containers"
					, 0, n.Get("NumContainers").AsInt());
				NUnit.Framework.Assert.AreEqual(n.Get("NodeId") + ": Unexpected amount of used memory"
					, 0, n.Get("UsedMemoryMB").AsInt());
				NUnit.Framework.Assert.IsNotNull(n.Get("AvailableMemoryMB"));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMNMInfoMissmatch()
		{
			RMContext rmc = Org.Mockito.Mockito.Mock<RMContext>();
			ResourceScheduler rms = Org.Mockito.Mockito.Mock<ResourceScheduler>();
			ConcurrentMap<NodeId, RMNode> map = new ConcurrentHashMap<NodeId, RMNode>();
			RMNode node = MockNodes.NewNodeInfo(1, MockNodes.NewResource(4 * 1024));
			map[node.GetNodeID()] = node;
			Org.Mockito.Mockito.When(rmc.GetRMNodes()).ThenReturn(map);
			RMNMInfo rmInfo = new RMNMInfo(rmc, rms);
			string liveNMs = rmInfo.GetLiveNodeManagers();
			ObjectMapper mapper = new ObjectMapper();
			JsonNode jn = mapper.ReadTree(liveNMs);
			NUnit.Framework.Assert.AreEqual("Unexpected number of live nodes:", 1, jn.Size());
			IEnumerator<JsonNode> it = jn.GetEnumerator();
			while (it.HasNext())
			{
				JsonNode n = it.Next();
				NUnit.Framework.Assert.IsNotNull(n.Get("HostName"));
				NUnit.Framework.Assert.IsNotNull(n.Get("Rack"));
				NUnit.Framework.Assert.IsTrue("Node " + n.Get("NodeId") + " should be RUNNING", n
					.Get("State").AsText().Contains("RUNNING"));
				NUnit.Framework.Assert.IsNotNull(n.Get("NodeHTTPAddress"));
				NUnit.Framework.Assert.IsNotNull(n.Get("LastHealthUpdate"));
				NUnit.Framework.Assert.IsNotNull(n.Get("HealthReport"));
				NUnit.Framework.Assert.IsNotNull(n.Get("NodeManagerVersion"));
				NUnit.Framework.Assert.IsNull(n.Get("NumContainers"));
				NUnit.Framework.Assert.IsNull(n.Get("UsedMemoryMB"));
				NUnit.Framework.Assert.IsNull(n.Get("AvailableMemoryMB"));
			}
		}
	}
}
