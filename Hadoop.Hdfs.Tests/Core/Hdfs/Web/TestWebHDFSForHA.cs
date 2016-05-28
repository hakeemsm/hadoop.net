using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestWebHDFSForHA
	{
		private const string LogicalName = "minidfs";

		private static readonly URI WebhdfsUri = URI.Create(WebHdfsFileSystem.Scheme + "://"
			 + LogicalName);

		private static readonly MiniDFSNNTopology topo = new MiniDFSNNTopology().AddNameservice
			(new MiniDFSNNTopology.NSConf(LogicalName).AddNN(new MiniDFSNNTopology.NNConf("nn1"
			)).AddNN(new MiniDFSNNTopology.NNConf("nn2")));

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHA()
		{
			Configuration conf = DFSTestUtil.NewHAConfiguration(LogicalName);
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(topo).NumDataNodes(0).Build
					();
				HATestUtil.SetFailoverConfigurations(cluster, conf, LogicalName);
				cluster.WaitActive();
				fs = FileSystem.Get(WebhdfsUri, conf);
				cluster.TransitionToActive(0);
				Path dir = new Path("/test");
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(dir));
				cluster.ShutdownNameNode(0);
				cluster.TransitionToActive(1);
				Path dir2 = new Path("/test2");
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(dir2));
			}
			finally
			{
				IOUtils.Cleanup(null, fs);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSecureHAToken()
		{
			Configuration conf = DFSTestUtil.NewHAConfiguration(LogicalName);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
			MiniDFSCluster cluster = null;
			WebHdfsFileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(topo).NumDataNodes(0).Build
					();
				HATestUtil.SetFailoverConfigurations(cluster, conf, LogicalName);
				cluster.WaitActive();
				fs = Org.Mockito.Mockito.Spy((WebHdfsFileSystem)FileSystem.Get(WebhdfsUri, conf));
				FileSystemTestHelper.AddFileSystemForTesting(WebhdfsUri, conf, fs);
				cluster.TransitionToActive(0);
				Org.Apache.Hadoop.Security.Token.Token<object> token = ((Org.Apache.Hadoop.Security.Token.Token
					<DelegationTokenIdentifier>)fs.GetDelegationToken(null));
				cluster.ShutdownNameNode(0);
				cluster.TransitionToActive(1);
				token.Renew(conf);
				token.Cancel(conf);
				Org.Mockito.Mockito.Verify(fs).RenewDelegationToken(token);
				Org.Mockito.Mockito.Verify(fs).CancelDelegationToken(token);
			}
			finally
			{
				IOUtils.Cleanup(null, fs);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverAfterOpen()
		{
			Configuration conf = DFSTestUtil.NewHAConfiguration(LogicalName);
			conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, HdfsConstants.HdfsUriScheme
				 + "://" + LogicalName);
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			Path p = new Path("/test");
			byte[] data = Sharpen.Runtime.GetBytesForString("Hello");
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(topo).NumDataNodes(1).Build
					();
				HATestUtil.SetFailoverConfigurations(cluster, conf, LogicalName);
				cluster.WaitActive();
				fs = FileSystem.Get(WebhdfsUri, conf);
				cluster.TransitionToActive(1);
				FSDataOutputStream @out = fs.Create(p);
				cluster.ShutdownNameNode(1);
				cluster.TransitionToActive(0);
				@out.Write(data);
				@out.Close();
				FSDataInputStream @in = fs.Open(p);
				byte[] buf = new byte[data.Length];
				IOUtils.ReadFully(@in, buf, 0, buf.Length);
				Assert.AssertArrayEquals(data, buf);
			}
			finally
			{
				IOUtils.Cleanup(null, fs);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleNamespacesConfigured()
		{
			Configuration conf = DFSTestUtil.NewHAConfiguration(LogicalName);
			MiniDFSCluster cluster = null;
			WebHdfsFileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(topo).NumDataNodes(1).Build
					();
				HATestUtil.SetFailoverConfigurations(cluster, conf, LogicalName);
				cluster.WaitActive();
				DFSTestUtil.AddHAConfiguration(conf, LogicalName + "remote");
				DFSTestUtil.SetFakeHttpAddresses(conf, LogicalName + "remote");
				fs = (WebHdfsFileSystem)FileSystem.Get(WebhdfsUri, conf);
				NUnit.Framework.Assert.AreEqual(2, fs.GetResolvedNNAddr().Length);
			}
			finally
			{
				IOUtils.Cleanup(null, fs);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Make sure the WebHdfsFileSystem will retry based on RetriableException when
		/// rpcServer is null in NamenodeWebHdfsMethods while NameNode starts up.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRetryWhileNNStartup()
		{
			Configuration conf = DFSTestUtil.NewHAConfiguration(LogicalName);
			MiniDFSCluster cluster = null;
			IDictionary<string, bool> resultMap = new Dictionary<string, bool>();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(topo).NumDataNodes(0).Build
					();
				HATestUtil.SetFailoverConfigurations(cluster, conf, LogicalName);
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				NameNode namenode = cluster.GetNameNode(0);
				NamenodeProtocols rpcServer = namenode.GetRpcServer();
				Whitebox.SetInternalState(namenode, "rpcServer", null);
				new _Thread_212(this, conf, resultMap).Start();
				Sharpen.Thread.Sleep(1000);
				Whitebox.SetInternalState(namenode, "rpcServer", rpcServer);
				lock (this)
				{
					while (!resultMap.Contains("mkdirs"))
					{
						Sharpen.Runtime.Wait(this);
					}
					NUnit.Framework.Assert.IsTrue(resultMap["mkdirs"]);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private sealed class _Thread_212 : Sharpen.Thread
		{
			public _Thread_212(TestWebHDFSForHA _enclosing, Configuration conf, IDictionary<string
				, bool> resultMap)
			{
				this._enclosing = _enclosing;
				this.conf = conf;
				this.resultMap = resultMap;
			}

			public override void Run()
			{
				bool result = false;
				FileSystem fs = null;
				try
				{
					fs = FileSystem.Get(TestWebHDFSForHA.WebhdfsUri, conf);
					Path dir = new Path("/test");
					result = fs.Mkdirs(dir);
				}
				catch (IOException)
				{
					result = false;
				}
				finally
				{
					IOUtils.Cleanup(null, fs);
				}
				lock (this._enclosing)
				{
					resultMap["mkdirs"] = result;
					Sharpen.Runtime.NotifyAll(this._enclosing);
				}
			}

			private readonly TestWebHDFSForHA _enclosing;

			private readonly Configuration conf;

			private readonly IDictionary<string, bool> resultMap;
		}
	}
}
