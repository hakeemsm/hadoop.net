using System;
using System.Net;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Web.Resources;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>Test WebHDFS with multiple NameNodes</summary>
	public class TestWebHdfsWithMultipleNameNodes
	{
		internal static readonly Log Log = WebHdfsTestUtil.Log;

		private static void SetLogLevel()
		{
			((Log4JLogger)Log).GetLogger().SetLevel(Level.All);
			GenericTestUtils.SetLogLevel(NamenodeWebHdfsMethods.Log, Level.All);
			DFSTestUtil.SetNameNodeLogLevel(Level.All);
		}

		private static readonly Configuration conf = new HdfsConfiguration();

		private static MiniDFSCluster cluster;

		private static WebHdfsFileSystem[] webhdfs;

		[BeforeClass]
		public static void SetupTest()
		{
			SetLogLevel();
			try
			{
				SetupCluster(4, 3);
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <exception cref="System.Exception"/>
		private static void SetupCluster(int nNameNodes, int nDataNodes)
		{
			Log.Info("nNameNodes=" + nNameNodes + ", nDataNodes=" + nDataNodes);
			conf.SetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, true);
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleFederatedTopology
				(nNameNodes)).NumDataNodes(nDataNodes).Build();
			cluster.WaitActive();
			webhdfs = new WebHdfsFileSystem[nNameNodes];
			for (int i = 0; i < webhdfs.Length; i++)
			{
				IPEndPoint addr = cluster.GetNameNode(i).GetHttpAddress();
				string uri = WebHdfsFileSystem.Scheme + "://" + addr.GetHostName() + ":" + addr.Port
					 + "/";
				webhdfs[i] = (WebHdfsFileSystem)FileSystem.Get(new URI(uri), conf);
			}
		}

		[AfterClass]
		public static void ShutdownCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
				cluster = null;
			}
		}

		private static string CreateString(string prefix, int i)
		{
			//The suffix is to make sure the strings have different lengths.
			string suffix = Sharpen.Runtime.Substring("*********************", 0, i + 1);
			return prefix + i + suffix + "\n";
		}

		private static string[] CreateStrings(string prefix, string name)
		{
			string[] strings = new string[webhdfs.Length];
			for (int i = 0; i < webhdfs.Length; i++)
			{
				strings[i] = CreateString(prefix, i);
				Log.Info(name + "[" + i + "] = " + strings[i]);
			}
			return strings;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRedirect()
		{
			string dir = "/testRedirect/";
			string filename = "file";
			Path p = new Path(dir, filename);
			string[] writeStrings = CreateStrings("write to webhdfs ", "write");
			string[] appendStrings = CreateStrings("append to webhdfs ", "append");
			//test create: create a file for each namenode
			for (int i = 0; i < webhdfs.Length; i++)
			{
				FSDataOutputStream @out = webhdfs[i].Create(p);
				@out.Write(Sharpen.Runtime.GetBytesForString(writeStrings[i]));
				@out.Close();
			}
			for (int i_1 = 0; i_1 < webhdfs.Length; i_1++)
			{
				//check file length
				long expected = writeStrings[i_1].Length;
				NUnit.Framework.Assert.AreEqual(expected, webhdfs[i_1].GetFileStatus(p).GetLen());
			}
			//test read: check file content for each namenode
			for (int i_2 = 0; i_2 < webhdfs.Length; i_2++)
			{
				FSDataInputStream @in = webhdfs[i_2].Open(p);
				for (int c; (c = @in.Read()) != -1; j++)
				{
					NUnit.Framework.Assert.AreEqual(writeStrings[i_2][j], c);
				}
				@in.Close();
			}
			//test append: append to the file for each namenode
			for (int i_3 = 0; i_3 < webhdfs.Length; i_3++)
			{
				FSDataOutputStream @out = webhdfs[i_3].Append(p);
				@out.Write(Sharpen.Runtime.GetBytesForString(appendStrings[i_3]));
				@out.Close();
			}
			for (int i_4 = 0; i_4 < webhdfs.Length; i_4++)
			{
				//check file length
				long expected = writeStrings[i_4].Length + appendStrings[i_4].Length;
				NUnit.Framework.Assert.AreEqual(expected, webhdfs[i_4].GetFileStatus(p).GetLen());
			}
			//test read: check file content for each namenode
			for (int i_5 = 0; i_5 < webhdfs.Length; i_5++)
			{
				StringBuilder b = new StringBuilder();
				FSDataInputStream @in = webhdfs[i_5].Open(p);
				for (int c; (c = @in.Read()) != -1; )
				{
					b.Append((char)c);
				}
				int wlen = writeStrings[i_5].Length;
				NUnit.Framework.Assert.AreEqual(writeStrings[i_5], b.Substring(0, wlen));
				NUnit.Framework.Assert.AreEqual(appendStrings[i_5], b.Substring(wlen));
				@in.Close();
			}
		}
	}
}
