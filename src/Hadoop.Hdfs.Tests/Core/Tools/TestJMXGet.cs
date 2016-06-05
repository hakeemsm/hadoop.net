using System.Collections.Generic;
using System.IO;
using Javax.Management;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Test;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Tools
{
	/// <summary>Startup and checkpoint tests</summary>
	public class TestJMXGet
	{
		private Configuration config;

		private MiniDFSCluster cluster;

		internal const long seed = unchecked((long)(0xAAAAEEFL));

		internal const int blockSize = 4096;

		internal const int fileSize = 8192;

		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FileSystem fileSys, Path name, int repl)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)repl, blockSize);
			byte[] buffer = new byte[fileSize];
			Random rand = new Random(seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			stm.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			config = new HdfsConfiguration();
		}

		/// <summary>clean up</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster.IsClusterUp())
			{
				cluster.Shutdown();
			}
			FilePath data_dir = new FilePath(cluster.GetDataDirectory());
			if (data_dir.Exists() && !FileUtil.FullyDelete(data_dir))
			{
				throw new IOException("Could not delete hdfs directory in tearDown '" + data_dir 
					+ "'");
			}
		}

		/// <summary>test JMX connection to NameNode..</summary>
		/// <exception cref="System.Exception"></exception>
		[NUnit.Framework.Test]
		public virtual void TestNameNode()
		{
			int numDatanodes = 2;
			cluster = new MiniDFSCluster.Builder(config).NumDataNodes(numDatanodes).Build();
			cluster.WaitActive();
			WriteFile(cluster.GetFileSystem(), new Path("/test1"), 2);
			JMXGet jmx = new JMXGet();
			string serviceName = "NameNode";
			jmx.SetService(serviceName);
			jmx.Init();
			// default lists namenode mbeans only
			NUnit.Framework.Assert.IsTrue("error printAllValues", CheckPrintAllValues(jmx));
			//get some data from different source
			NUnit.Framework.Assert.AreEqual(numDatanodes, System.Convert.ToInt32(jmx.GetValue
				("NumLiveDataNodes")));
			MetricsAsserts.AssertGauge("CorruptBlocks", long.Parse(jmx.GetValue("CorruptBlocks"
				)), MetricsAsserts.GetMetrics("FSNamesystem"));
			NUnit.Framework.Assert.AreEqual(numDatanodes, System.Convert.ToInt32(jmx.GetValue
				("NumOpenConnections")));
			cluster.Shutdown();
			MBeanServerConnection mbsc = ManagementFactory.GetPlatformMBeanServer();
			ObjectName query = new ObjectName("Hadoop:service=" + serviceName + ",*");
			ICollection<ObjectName> names = mbsc.QueryNames(query, null);
			NUnit.Framework.Assert.IsTrue("No beans should be registered for " + serviceName, 
				names.IsEmpty());
		}

		/// <exception cref="System.Exception"/>
		private static bool CheckPrintAllValues(JMXGet jmx)
		{
			int size = 0;
			byte[] bytes = null;
			string pattern = "List of all the available keys:";
			PipedOutputStream pipeOut = new PipedOutputStream();
			PipedInputStream pipeIn = new PipedInputStream(pipeOut);
			Runtime.SetErr(new TextWriter(pipeOut));
			jmx.PrintAllValues();
			if ((size = pipeIn.Available()) != 0)
			{
				bytes = new byte[size];
				pipeIn.Read(bytes, 0, bytes.Length);
			}
			pipeOut.Close();
			pipeIn.Close();
			return bytes != null ? Sharpen.Runtime.GetStringForBytes(bytes).Contains(pattern)
				 : false;
		}

		/// <summary>test JMX connection to DataNode..</summary>
		/// <exception cref="System.Exception"></exception>
		[NUnit.Framework.Test]
		public virtual void TestDataNode()
		{
			int numDatanodes = 2;
			cluster = new MiniDFSCluster.Builder(config).NumDataNodes(numDatanodes).Build();
			cluster.WaitActive();
			WriteFile(cluster.GetFileSystem(), new Path("/test"), 2);
			JMXGet jmx = new JMXGet();
			string serviceName = "DataNode";
			jmx.SetService(serviceName);
			jmx.Init();
			NUnit.Framework.Assert.AreEqual(fileSize, System.Convert.ToInt32(jmx.GetValue("BytesWritten"
				)));
			cluster.Shutdown();
			MBeanServerConnection mbsc = ManagementFactory.GetPlatformMBeanServer();
			ObjectName query = new ObjectName("Hadoop:service=" + serviceName + ",*");
			ICollection<ObjectName> names = mbsc.QueryNames(query, null);
			NUnit.Framework.Assert.IsTrue("No beans should be registered for " + serviceName, 
				names.IsEmpty());
		}
	}
}
