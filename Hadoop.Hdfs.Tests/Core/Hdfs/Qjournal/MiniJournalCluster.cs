using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Qjournal.Server;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal
{
	public class MiniJournalCluster
	{
		public class Builder
		{
			private string baseDir;

			private int numJournalNodes = 3;

			private bool format = true;

			private readonly Configuration conf;

			public Builder(Configuration conf)
			{
				this.conf = conf;
			}

			public virtual MiniJournalCluster.Builder BaseDir(string d)
			{
				this.baseDir = d;
				return this;
			}

			public virtual MiniJournalCluster.Builder NumJournalNodes(int n)
			{
				this.numJournalNodes = n;
				return this;
			}

			public virtual MiniJournalCluster.Builder Format(bool f)
			{
				this.format = f;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual MiniJournalCluster Build()
			{
				return new MiniJournalCluster(this);
			}
		}

		private sealed class JNInfo
		{
			private JournalNode node;

			private readonly IPEndPoint ipcAddr;

			private readonly string httpServerURI;

			private JNInfo(JournalNode node)
			{
				this.node = node;
				this.ipcAddr = node.GetBoundIpcAddress();
				this.httpServerURI = node.GetHttpServerURI();
			}
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(MiniJournalCluster));

		private readonly FilePath baseDir;

		private readonly MiniJournalCluster.JNInfo[] nodes;

		/// <exception cref="System.IO.IOException"/>
		private MiniJournalCluster(MiniJournalCluster.Builder b)
		{
			Log.Info("Starting MiniJournalCluster with " + b.numJournalNodes + " journal nodes"
				);
			if (b.baseDir != null)
			{
				this.baseDir = new FilePath(b.baseDir);
			}
			else
			{
				this.baseDir = new FilePath(MiniDFSCluster.GetBaseDirectory());
			}
			nodes = new MiniJournalCluster.JNInfo[b.numJournalNodes];
			for (int i = 0; i < b.numJournalNodes; i++)
			{
				if (b.format)
				{
					FilePath dir = GetStorageDir(i);
					Log.Debug("Fully deleting JN directory " + dir);
					FileUtil.FullyDelete(dir);
				}
				JournalNode jn = new JournalNode();
				jn.SetConf(CreateConfForNode(b, i));
				jn.Start();
				nodes[i] = new MiniJournalCluster.JNInfo(jn);
			}
		}

		/// <summary>
		/// Set up the given Configuration object to point to the set of JournalNodes
		/// in this cluster.
		/// </summary>
		public virtual URI GetQuorumJournalURI(string jid)
		{
			IList<string> addrs = Lists.NewArrayList();
			foreach (MiniJournalCluster.JNInfo info in nodes)
			{
				addrs.AddItem("127.0.0.1:" + info.ipcAddr.Port);
			}
			string addrsVal = Joiner.On(";").Join(addrs);
			Log.Debug("Setting logger addresses to: " + addrsVal);
			try
			{
				return new URI("qjournal://" + addrsVal + "/" + jid);
			}
			catch (URISyntaxException e)
			{
				throw new Exception(e);
			}
		}

		/// <summary>Start the JournalNodes in the cluster.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Start()
		{
			foreach (MiniJournalCluster.JNInfo info in nodes)
			{
				info.node.Start();
			}
		}

		/// <summary>Shutdown all of the JournalNodes in the cluster.</summary>
		/// <exception cref="System.IO.IOException">if one or more nodes failed to stop</exception>
		public virtual void Shutdown()
		{
			bool failed = false;
			foreach (MiniJournalCluster.JNInfo info in nodes)
			{
				try
				{
					info.node.StopAndJoin(0);
				}
				catch (Exception e)
				{
					failed = true;
					Log.Warn("Unable to stop journal node " + info.node, e);
				}
			}
			if (failed)
			{
				throw new IOException("Unable to shut down. Check log for details");
			}
		}

		private Configuration CreateConfForNode(MiniJournalCluster.Builder b, int idx)
		{
			Configuration conf = new Configuration(b.conf);
			FilePath logDir = GetStorageDir(idx);
			conf.Set(DFSConfigKeys.DfsJournalnodeEditsDirKey, logDir.ToString());
			conf.Set(DFSConfigKeys.DfsJournalnodeRpcAddressKey, "localhost:0");
			conf.Set(DFSConfigKeys.DfsJournalnodeHttpAddressKey, "localhost:0");
			return conf;
		}

		public virtual FilePath GetStorageDir(int idx)
		{
			return new FilePath(baseDir, "journalnode-" + idx).GetAbsoluteFile();
		}

		public virtual FilePath GetJournalDir(int idx, string jid)
		{
			return new FilePath(GetStorageDir(idx), jid);
		}

		public virtual FilePath GetCurrentDir(int idx, string jid)
		{
			return new FilePath(GetJournalDir(idx, jid), "current");
		}

		public virtual FilePath GetPreviousDir(int idx, string jid)
		{
			return new FilePath(GetJournalDir(idx, jid), "previous");
		}

		public virtual JournalNode GetJournalNode(int i)
		{
			return nodes[i].node;
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RestartJournalNode(int i)
		{
			MiniJournalCluster.JNInfo info = nodes[i];
			JournalNode jn = info.node;
			Configuration conf = new Configuration(jn.GetConf());
			if (jn.IsStarted())
			{
				jn.StopAndJoin(0);
			}
			conf.Set(DFSConfigKeys.DfsJournalnodeRpcAddressKey, NetUtils.GetHostPortString(info
				.ipcAddr));
			string uri = info.httpServerURI;
			if (uri.StartsWith("http://"))
			{
				conf.Set(DFSConfigKeys.DfsJournalnodeHttpAddressKey, Sharpen.Runtime.Substring(uri
					, ("http://".Length)));
			}
			else
			{
				if (info.httpServerURI.StartsWith("https://"))
				{
					conf.Set(DFSConfigKeys.DfsJournalnodeHttpsAddressKey, Sharpen.Runtime.Substring(uri
						, ("https://".Length)));
				}
			}
			JournalNode newJN = new JournalNode();
			newJN.SetConf(conf);
			newJN.Start();
			info.node = newJN;
		}

		public virtual int GetQuorumSize()
		{
			return nodes.Length / 2 + 1;
		}

		public virtual int GetNumNodes()
		{
			return nodes.Length;
		}
	}
}
