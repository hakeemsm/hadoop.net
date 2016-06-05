using System;
using System.Net;
using System.Net.Sockets;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>This class checks that RPCs can use specialized socket factories.</summary>
	public class TestMRCJCSocketFactory
	{
		/// <summary>
		/// Check that we can reach a NameNode or Resource Manager using a specific
		/// socket factory
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSocketFactory()
		{
			// Create a standard mini-cluster
			Configuration sconf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(sconf).NumDataNodes(1).Build(
				);
			int nameNodePort = cluster.GetNameNodePort();
			// Get a reference to its DFS directly
			FileSystem fs = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue(fs is DistributedFileSystem);
			DistributedFileSystem directDfs = (DistributedFileSystem)fs;
			Configuration cconf = GetCustomSocketConfigs(nameNodePort);
			fs = FileSystem.Get(cconf);
			NUnit.Framework.Assert.IsTrue(fs is DistributedFileSystem);
			DistributedFileSystem dfs = (DistributedFileSystem)fs;
			JobClient client = null;
			MiniMRYarnCluster miniMRYarnCluster = null;
			try
			{
				// This will test RPC to the NameNode only.
				// could we test Client-DataNode connections?
				Path filePath = new Path("/dir");
				NUnit.Framework.Assert.IsFalse(directDfs.Exists(filePath));
				NUnit.Framework.Assert.IsFalse(dfs.Exists(filePath));
				directDfs.Mkdirs(filePath);
				NUnit.Framework.Assert.IsTrue(directDfs.Exists(filePath));
				NUnit.Framework.Assert.IsTrue(dfs.Exists(filePath));
				// This will test RPC to a Resource Manager
				fs = FileSystem.Get(sconf);
				JobConf jobConf = new JobConf();
				FileSystem.SetDefaultUri(jobConf, fs.GetUri().ToString());
				miniMRYarnCluster = InitAndStartMiniMRYarnCluster(jobConf);
				JobConf jconf = new JobConf(miniMRYarnCluster.GetConfig());
				jconf.Set("hadoop.rpc.socket.factory.class.default", "org.apache.hadoop.ipc.DummySocketFactory"
					);
				jconf.Set(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
				string rmAddress = jconf.Get("yarn.resourcemanager.address");
				string[] split = rmAddress.Split(":");
				jconf.Set("yarn.resourcemanager.address", split[0] + ':' + (System.Convert.ToInt32
					(split[1]) + 10));
				client = new JobClient(jconf);
				JobStatus[] jobs = client.JobsToComplete();
				NUnit.Framework.Assert.IsTrue(jobs.Length == 0);
			}
			finally
			{
				CloseClient(client);
				CloseDfs(dfs);
				CloseDfs(directDfs);
				StopMiniMRYarnCluster(miniMRYarnCluster);
				ShutdownDFSCluster(cluster);
			}
		}

		private MiniMRYarnCluster InitAndStartMiniMRYarnCluster(JobConf jobConf)
		{
			MiniMRYarnCluster miniMRYarnCluster;
			miniMRYarnCluster = new MiniMRYarnCluster(this.GetType().FullName, 1);
			miniMRYarnCluster.Init(jobConf);
			miniMRYarnCluster.Start();
			return miniMRYarnCluster;
		}

		private Configuration GetCustomSocketConfigs(int nameNodePort)
		{
			// Get another reference via network using a specific socket factory
			Configuration cconf = new Configuration();
			FileSystem.SetDefaultUri(cconf, string.Format("hdfs://localhost:%s/", nameNodePort
				 + 10));
			cconf.Set("hadoop.rpc.socket.factory.class.default", "org.apache.hadoop.ipc.DummySocketFactory"
				);
			cconf.Set("hadoop.rpc.socket.factory.class.ClientProtocol", "org.apache.hadoop.ipc.DummySocketFactory"
				);
			cconf.Set("hadoop.rpc.socket.factory.class.JobSubmissionProtocol", "org.apache.hadoop.ipc.DummySocketFactory"
				);
			return cconf;
		}

		private void ShutdownDFSCluster(MiniDFSCluster cluster)
		{
			try
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			catch (Exception ignored)
			{
				// nothing we can do
				Sharpen.Runtime.PrintStackTrace(ignored);
			}
		}

		private void StopMiniMRYarnCluster(MiniMRYarnCluster miniMRYarnCluster)
		{
			try
			{
				if (miniMRYarnCluster != null)
				{
					miniMRYarnCluster.Stop();
				}
			}
			catch (Exception ignored)
			{
				// nothing we can do
				Sharpen.Runtime.PrintStackTrace(ignored);
			}
		}

		private void CloseDfs(DistributedFileSystem dfs)
		{
			try
			{
				if (dfs != null)
				{
					dfs.Close();
				}
			}
			catch (Exception ignored)
			{
				// nothing we can do
				Sharpen.Runtime.PrintStackTrace(ignored);
			}
		}

		private void CloseClient(JobClient client)
		{
			try
			{
				if (client != null)
				{
					client.Close();
				}
			}
			catch (Exception ignored)
			{
				// nothing we can do
				Sharpen.Runtime.PrintStackTrace(ignored);
			}
		}
	}

	/// <summary>
	/// Dummy socket factory which shift TPC ports by subtracting 10 when
	/// establishing a connection
	/// </summary>
	internal class DummySocketFactory : StandardSocketFactory
	{
		/// <summary>Default empty constructor (for use with the reflection API).</summary>
		public DummySocketFactory()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override Socket CreateSocket()
		{
			return new _Socket_187();
		}

		private sealed class _Socket_187 : Socket
		{
			public _Socket_187()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Connect(EndPoint addr, int timeout)
			{
				System.Diagnostics.Debug.Assert((addr is IPEndPoint));
				IPEndPoint iaddr = (IPEndPoint)addr;
				EndPoint newAddr = null;
				if (iaddr.IsUnresolved())
				{
					newAddr = new IPEndPoint(iaddr.GetHostName(), iaddr.Port - 10);
				}
				else
				{
					newAddr = new IPEndPoint(iaddr.Address, iaddr.Port - 10);
				}
				System.Console.Out.Printf("Test socket: rerouting %s to %s\n", iaddr, newAddr);
				base.Connect(newAddr, timeout);
			}
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (!(obj is Org.Apache.Hadoop.Ipc.DummySocketFactory))
			{
				return false;
			}
			return true;
		}
	}
}
