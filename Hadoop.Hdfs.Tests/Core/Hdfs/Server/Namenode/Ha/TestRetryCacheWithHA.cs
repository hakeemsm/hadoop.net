using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestRetryCacheWithHA
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestRetryCacheWithHA));

		private const int BlockSize = 1024;

		private const short DataNodes = 3;

		private const int Checktimes = 10;

		private const int ResponseSize = 3;

		private MiniDFSCluster cluster;

		private DistributedFileSystem dfs;

		private readonly Configuration conf = new HdfsConfiguration();

		/// <summary>A dummy invocation handler extending RetryInvocationHandler.</summary>
		/// <remarks>
		/// A dummy invocation handler extending RetryInvocationHandler. We can use
		/// a boolean flag to control whether the method invocation succeeds or not.
		/// </remarks>
		private class DummyRetryInvocationHandler : RetryInvocationHandler<ClientProtocol
			>
		{
			internal static readonly AtomicBoolean block = new AtomicBoolean(false);

			internal DummyRetryInvocationHandler(FailoverProxyProvider<ClientProtocol> proxyProvider
				, RetryPolicy retryPolicy)
				: base(proxyProvider, retryPolicy)
			{
			}

			/// <exception cref="System.Exception"/>
			protected override object InvokeMethod(MethodInfo method, object[] args)
			{
				object result = base.InvokeMethod(method, args);
				if (block.Get())
				{
					throw new UnknownHostException("Fake Exception");
				}
				else
				{
					return result;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetInt(DFSConfigKeys.DfsNamenodeListCacheDirectivesNumResponses, ResponseSize
				);
			conf.SetInt(DFSConfigKeys.DfsNamenodeListCachePoolsNumResponses, ResponseSize);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeXattrsEnabledKey, true);
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(DataNodes).Build();
			cluster.WaitActive();
			cluster.TransitionToActive(0);
			// setup the configuration
			HATestUtil.SetFailoverConfigurations(cluster, conf);
			dfs = (DistributedFileSystem)HATestUtil.ConfigureFailoverFs(cluster, conf);
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Cleanup()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>1.</summary>
		/// <remarks>
		/// 1. Run a set of operations
		/// 2. Trigger the NN failover
		/// 3. Check the retry cache on the original standby NN
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestRetryCacheOnStandbyNN()
		{
			// 1. run operations
			DFSTestUtil.RunOperations(cluster, dfs, conf, BlockSize, 0);
			// check retry cache in NN1
			FSNamesystem fsn0 = cluster.GetNamesystem(0);
			LightWeightCache<RetryCache.CacheEntry, RetryCache.CacheEntry> cacheSet = (LightWeightCache
				<RetryCache.CacheEntry, RetryCache.CacheEntry>)fsn0.GetRetryCache().GetCacheSet(
				);
			NUnit.Framework.Assert.AreEqual(25, cacheSet.Size());
			IDictionary<RetryCache.CacheEntry, RetryCache.CacheEntry> oldEntries = new Dictionary
				<RetryCache.CacheEntry, RetryCache.CacheEntry>();
			IEnumerator<RetryCache.CacheEntry> iter = cacheSet.GetEnumerator();
			while (iter.HasNext())
			{
				RetryCache.CacheEntry entry = iter.Next();
				oldEntries[entry] = entry;
			}
			// 2. Failover the current standby to active.
			cluster.GetNameNode(0).GetRpcServer().RollEditLog();
			cluster.GetNameNode(1).GetNamesystem().GetEditLogTailer().DoTailEdits();
			cluster.ShutdownNameNode(0);
			cluster.TransitionToActive(1);
			// 3. check the retry cache on the new active NN
			FSNamesystem fsn1 = cluster.GetNamesystem(1);
			cacheSet = (LightWeightCache<RetryCache.CacheEntry, RetryCache.CacheEntry>)fsn1.GetRetryCache
				().GetCacheSet();
			NUnit.Framework.Assert.AreEqual(25, cacheSet.Size());
			iter = cacheSet.GetEnumerator();
			while (iter.HasNext())
			{
				RetryCache.CacheEntry entry = iter.Next();
				NUnit.Framework.Assert.IsTrue(oldEntries.Contains(entry));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private DFSClient GenClientWithDummyHandler()
		{
			URI nnUri = dfs.GetUri();
			FailoverProxyProvider<ClientProtocol> failoverProxyProvider = NameNodeProxies.CreateFailoverProxyProvider
				<ClientProtocol>(conf, nnUri, true, null);
			InvocationHandler dummyHandler = new TestRetryCacheWithHA.DummyRetryInvocationHandler
				(failoverProxyProvider, RetryPolicies.FailoverOnNetworkException(RetryPolicies.TryOnceThenFail
				, int.MaxValue, DFSConfigKeys.DfsClientFailoverSleeptimeBaseDefault, DFSConfigKeys
				.DfsClientFailoverSleeptimeMaxDefault));
			ClientProtocol proxy = (ClientProtocol)Proxy.NewProxyInstance(failoverProxyProvider
				.GetInterface().GetClassLoader(), new Type[] { typeof(ClientProtocol) }, dummyHandler
				);
			DFSClient client = new DFSClient(null, proxy, conf, null);
			return client;
		}

		internal abstract class AtMostOnceOp
		{
			private readonly string name;

			internal readonly DFSClient client;

			internal int expectedUpdateCount = 0;

			internal AtMostOnceOp(TestRetryCacheWithHA _enclosing, string name, DFSClient client
				)
			{
				this._enclosing = _enclosing;
				this.name = name;
				this.client = client;
			}

			/// <exception cref="System.Exception"/>
			internal abstract void Prepare();

			/// <exception cref="System.Exception"/>
			internal abstract void Invoke();

			/// <exception cref="System.Exception"/>
			internal abstract bool CheckNamenodeBeforeReturn();

			internal abstract object GetResult();

			internal virtual int GetExpectedCacheUpdateCount()
			{
				return this.expectedUpdateCount;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>createSnapshot operaiton</summary>
		internal class CreateSnapshotOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private string snapshotPath;

			private readonly string dir;

			private readonly string snapshotName;

			internal CreateSnapshotOp(TestRetryCacheWithHA _enclosing, DFSClient client, string
				 dir, string snapshotName)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.dir = dir;
				this.snapshotName = snapshotName;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				Path dirPath = new Path(this.dir);
				if (!this._enclosing.dfs.Exists(dirPath))
				{
					this._enclosing.dfs.Mkdirs(dirPath);
					this._enclosing.dfs.AllowSnapshot(dirPath);
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.snapshotPath = this.client.CreateSnapshot(this.dir, this.snapshotName);
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				Path sPath = SnapshotTestHelper.GetSnapshotRoot(new Path(this.dir), this.snapshotName
					);
				bool snapshotCreated = this._enclosing.dfs.Exists(sPath);
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes && !snapshotCreated; i++)
				{
					Sharpen.Thread.Sleep(1000);
					snapshotCreated = this._enclosing.dfs.Exists(sPath);
				}
				return snapshotCreated;
			}

			internal override object GetResult()
			{
				return this.snapshotPath;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>deleteSnapshot</summary>
		internal class DeleteSnapshotOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly string dir;

			private readonly string snapshotName;

			internal DeleteSnapshotOp(TestRetryCacheWithHA _enclosing, DFSClient client, string
				 dir, string snapshotName)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.dir = dir;
				this.snapshotName = snapshotName;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				Path dirPath = new Path(this.dir);
				if (!this._enclosing.dfs.Exists(dirPath))
				{
					this._enclosing.dfs.Mkdirs(dirPath);
				}
				Path sPath = SnapshotTestHelper.GetSnapshotRoot(dirPath, this.snapshotName);
				if (!this._enclosing.dfs.Exists(sPath))
				{
					this._enclosing.dfs.AllowSnapshot(dirPath);
					this._enclosing.dfs.CreateSnapshot(dirPath, this.snapshotName);
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.client.DeleteSnapshot(this.dir, this.snapshotName);
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				Path sPath = SnapshotTestHelper.GetSnapshotRoot(new Path(this.dir), this.snapshotName
					);
				bool snapshotNotDeleted = this._enclosing.dfs.Exists(sPath);
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes && snapshotNotDeleted; i++)
				{
					Sharpen.Thread.Sleep(1000);
					snapshotNotDeleted = this._enclosing.dfs.Exists(sPath);
				}
				return !snapshotNotDeleted;
			}

			internal override object GetResult()
			{
				return null;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>renameSnapshot</summary>
		internal class RenameSnapshotOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly string dir;

			private readonly string oldName;

			private readonly string newName;

			internal RenameSnapshotOp(TestRetryCacheWithHA _enclosing, DFSClient client, string
				 dir, string oldName, string newName)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.dir = dir;
				this.oldName = oldName;
				this.newName = newName;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				Path dirPath = new Path(this.dir);
				if (!this._enclosing.dfs.Exists(dirPath))
				{
					this._enclosing.dfs.Mkdirs(dirPath);
				}
				Path sPath = SnapshotTestHelper.GetSnapshotRoot(dirPath, this.oldName);
				if (!this._enclosing.dfs.Exists(sPath))
				{
					this._enclosing.dfs.AllowSnapshot(dirPath);
					this._enclosing.dfs.CreateSnapshot(dirPath, this.oldName);
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.client.RenameSnapshot(this.dir, this.oldName, this.newName);
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				Path sPath = SnapshotTestHelper.GetSnapshotRoot(new Path(this.dir), this.newName);
				bool snapshotRenamed = this._enclosing.dfs.Exists(sPath);
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes && !snapshotRenamed; i++)
				{
					Sharpen.Thread.Sleep(1000);
					snapshotRenamed = this._enclosing.dfs.Exists(sPath);
				}
				return snapshotRenamed;
			}

			internal override object GetResult()
			{
				return null;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>create file operation (without OverWrite)</summary>
		internal class CreateOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly string fileName;

			private HdfsFileStatus status;

			internal CreateOp(TestRetryCacheWithHA _enclosing, DFSClient client, string fileName
				)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.fileName = fileName;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				Path filePath = new Path(this.fileName);
				if (this._enclosing.dfs.Exists(filePath))
				{
					this._enclosing.dfs.Delete(filePath, true);
				}
				Path fileParent = filePath.GetParent();
				if (!this._enclosing.dfs.Exists(fileParent))
				{
					this._enclosing.dfs.Mkdirs(fileParent);
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				EnumSet<CreateFlag> createFlag = EnumSet.Of(CreateFlag.Create);
				this.status = this.client.GetNamenode().Create(this.fileName, FsPermission.GetFileDefault
					(), this.client.GetClientName(), new EnumSetWritable<CreateFlag>(createFlag), false
					, TestRetryCacheWithHA.DataNodes, TestRetryCacheWithHA.BlockSize, new CryptoProtocolVersion
					[] { CryptoProtocolVersion.EncryptionZones });
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				Path filePath = new Path(this.fileName);
				bool fileCreated = this._enclosing.dfs.Exists(filePath);
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes && !fileCreated; i++)
				{
					Sharpen.Thread.Sleep(1000);
					fileCreated = this._enclosing.dfs.Exists(filePath);
				}
				return fileCreated;
			}

			internal override object GetResult()
			{
				return this.status;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>append operation</summary>
		internal class AppendOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly string fileName;

			private LastBlockWithStatus lbk;

			internal AppendOp(TestRetryCacheWithHA _enclosing, DFSClient client, string fileName
				)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.fileName = fileName;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				Path filePath = new Path(this.fileName);
				if (!this._enclosing.dfs.Exists(filePath))
				{
					DFSTestUtil.CreateFile(this._enclosing.dfs, filePath, TestRetryCacheWithHA.BlockSize
						 / 2, TestRetryCacheWithHA.DataNodes, 0);
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.lbk = this.client.GetNamenode().Append(this.fileName, this.client.GetClientName
					(), new EnumSetWritable<CreateFlag>(EnumSet.Of(CreateFlag.Append)));
			}

			// check if the inode of the file is under construction
			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				INodeFile fileNode = this._enclosing.cluster.GetNameNode(0).GetNamesystem().GetFSDirectory
					().GetINode4Write(this.fileName).AsFile();
				bool fileIsUC = fileNode.IsUnderConstruction();
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes && !fileIsUC; i++)
				{
					Sharpen.Thread.Sleep(1000);
					fileNode = this._enclosing.cluster.GetNameNode(0).GetNamesystem().GetFSDirectory(
						).GetINode4Write(this.fileName).AsFile();
					fileIsUC = fileNode.IsUnderConstruction();
				}
				return fileIsUC;
			}

			internal override object GetResult()
			{
				return this.lbk;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>rename</summary>
		internal class RenameOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly string oldName;

			private readonly string newName;

			private bool renamed;

			internal RenameOp(TestRetryCacheWithHA _enclosing, DFSClient client, string oldName
				, string newName)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.oldName = oldName;
				this.newName = newName;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				Path filePath = new Path(this.oldName);
				if (!this._enclosing.dfs.Exists(filePath))
				{
					DFSTestUtil.CreateFile(this._enclosing.dfs, filePath, TestRetryCacheWithHA.BlockSize
						, TestRetryCacheWithHA.DataNodes, 0);
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.renamed = this.client.Rename(this.oldName, this.newName);
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				Path targetPath = new Path(this.newName);
				bool renamed = this._enclosing.dfs.Exists(targetPath);
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes && !renamed; i++)
				{
					Sharpen.Thread.Sleep(1000);
					renamed = this._enclosing.dfs.Exists(targetPath);
				}
				return renamed;
			}

			internal override object GetResult()
			{
				return this.renamed;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>rename2</summary>
		internal class Rename2Op : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly string oldName;

			private readonly string newName;

			internal Rename2Op(TestRetryCacheWithHA _enclosing, DFSClient client, string oldName
				, string newName)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.oldName = oldName;
				this.newName = newName;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				Path filePath = new Path(this.oldName);
				if (!this._enclosing.dfs.Exists(filePath))
				{
					DFSTestUtil.CreateFile(this._enclosing.dfs, filePath, TestRetryCacheWithHA.BlockSize
						, TestRetryCacheWithHA.DataNodes, 0);
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.client.Rename(this.oldName, this.newName, Options.Rename.Overwrite);
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				Path targetPath = new Path(this.newName);
				bool renamed = this._enclosing.dfs.Exists(targetPath);
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes && !renamed; i++)
				{
					Sharpen.Thread.Sleep(1000);
					renamed = this._enclosing.dfs.Exists(targetPath);
				}
				return renamed;
			}

			internal override object GetResult()
			{
				return null;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>concat</summary>
		internal class ConcatOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly string target;

			private readonly string[] srcs;

			private readonly Path[] srcPaths;

			internal ConcatOp(TestRetryCacheWithHA _enclosing, DFSClient client, Path target, 
				int numSrc)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.target = target.ToString();
				this.srcs = new string[numSrc];
				this.srcPaths = new Path[numSrc];
				Path parent = target.GetParent();
				for (int i = 0; i < numSrc; i++)
				{
					this.srcPaths[i] = new Path(parent, "srcfile" + i);
					this.srcs[i] = this.srcPaths[i].ToString();
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				Path targetPath = new Path(this.target);
				DFSTestUtil.CreateFile(this._enclosing.dfs, targetPath, TestRetryCacheWithHA.BlockSize
					, TestRetryCacheWithHA.DataNodes, 0);
				for (int i = 0; i < this.srcPaths.Length; i++)
				{
					DFSTestUtil.CreateFile(this._enclosing.dfs, this.srcPaths[i], TestRetryCacheWithHA
						.BlockSize, TestRetryCacheWithHA.DataNodes, 0);
				}
				NUnit.Framework.Assert.AreEqual(TestRetryCacheWithHA.BlockSize, this._enclosing.dfs
					.GetFileStatus(targetPath).GetLen());
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.client.Concat(this.target, this.srcs);
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				Path targetPath = new Path(this.target);
				bool done = this._enclosing.dfs.GetFileStatus(targetPath).GetLen() == TestRetryCacheWithHA
					.BlockSize * (this.srcs.Length + 1);
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes && !done; i++)
				{
					Sharpen.Thread.Sleep(1000);
					done = this._enclosing.dfs.GetFileStatus(targetPath).GetLen() == TestRetryCacheWithHA
						.BlockSize * (this.srcs.Length + 1);
				}
				return done;
			}

			internal override object GetResult()
			{
				return null;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>delete</summary>
		internal class DeleteOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly string target;

			private bool deleted;

			internal DeleteOp(TestRetryCacheWithHA _enclosing, DFSClient client, string target
				)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.target = target;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				Path p = new Path(this.target);
				if (!this._enclosing.dfs.Exists(p))
				{
					this.expectedUpdateCount++;
					DFSTestUtil.CreateFile(this._enclosing.dfs, p, TestRetryCacheWithHA.BlockSize, TestRetryCacheWithHA
						.DataNodes, 0);
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.expectedUpdateCount++;
				this.deleted = this.client.Delete(this.target, true);
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				Path targetPath = new Path(this.target);
				bool del = !this._enclosing.dfs.Exists(targetPath);
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes && !del; i++)
				{
					Sharpen.Thread.Sleep(1000);
					del = !this._enclosing.dfs.Exists(targetPath);
				}
				return del;
			}

			internal override object GetResult()
			{
				return this.deleted;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>createSymlink</summary>
		internal class CreateSymlinkOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly string target;

			private readonly string link;

			public CreateSymlinkOp(TestRetryCacheWithHA _enclosing, DFSClient client, string 
				target, string link)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.target = target;
				this.link = link;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				Path p = new Path(this.target);
				if (!this._enclosing.dfs.Exists(p))
				{
					this.expectedUpdateCount++;
					DFSTestUtil.CreateFile(this._enclosing.dfs, p, TestRetryCacheWithHA.BlockSize, TestRetryCacheWithHA
						.DataNodes, 0);
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.expectedUpdateCount++;
				this.client.CreateSymlink(this.target, this.link, false);
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				Path linkPath = new Path(this.link);
				FileStatus linkStatus = null;
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes && linkStatus == null; i++)
				{
					try
					{
						linkStatus = this._enclosing.dfs.GetFileLinkStatus(linkPath);
					}
					catch (FileNotFoundException)
					{
						// Ignoring, this can be legitimate.
						Sharpen.Thread.Sleep(1000);
					}
				}
				return linkStatus != null;
			}

			internal override object GetResult()
			{
				return null;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>updatePipeline</summary>
		internal class UpdatePipelineOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly string file;

			private ExtendedBlock oldBlock;

			private ExtendedBlock newBlock;

			private DatanodeInfo[] nodes;

			private FSDataOutputStream @out;

			public UpdatePipelineOp(TestRetryCacheWithHA _enclosing, DFSClient client, string
				 file)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.file = file;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				Path filePath = new Path(this.file);
				DFSTestUtil.CreateFile(this._enclosing.dfs, filePath, TestRetryCacheWithHA.BlockSize
					, TestRetryCacheWithHA.DataNodes, 0);
				// append to the file and leave the last block under construction
				this.@out = this.client.Append(this.file, TestRetryCacheWithHA.BlockSize, EnumSet
					.Of(CreateFlag.Append), null, null);
				byte[] appendContent = new byte[100];
				new Random().NextBytes(appendContent);
				this.@out.Write(appendContent);
				((HdfsDataOutputStream)this.@out).Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag.
					UpdateLength));
				LocatedBlocks blks = this._enclosing.dfs.GetClient().GetLocatedBlocks(this.file, 
					TestRetryCacheWithHA.BlockSize + 1);
				NUnit.Framework.Assert.AreEqual(1, blks.GetLocatedBlocks().Count);
				this.nodes = blks.Get(0).GetLocations();
				this.oldBlock = blks.Get(0).GetBlock();
				LocatedBlock newLbk = this.client.GetNamenode().UpdateBlockForPipeline(this.oldBlock
					, this.client.GetClientName());
				this.newBlock = new ExtendedBlock(this.oldBlock.GetBlockPoolId(), this.oldBlock.GetBlockId
					(), this.oldBlock.GetNumBytes(), newLbk.GetBlock().GetGenerationStamp());
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				DatanodeInfo[] newNodes = new DatanodeInfo[2];
				newNodes[0] = this.nodes[0];
				newNodes[1] = this.nodes[1];
				string[] storageIDs = new string[] { "s0", "s1" };
				this.client.GetNamenode().UpdatePipeline(this.client.GetClientName(), this.oldBlock
					, this.newBlock, newNodes, storageIDs);
				// close can fail if the out.close() commit the block after block received
				// notifications from Datanode.
				// Since datanodes and output stream have still old genstamps, these
				// blocks will be marked as corrupt after HDFS-5723 if RECEIVED
				// notifications reaches namenode first and close() will fail.
				DFSTestUtil.AbortStream((DFSOutputStream)this.@out.GetWrappedStream());
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				INodeFile fileNode = this._enclosing.cluster.GetNamesystem(0).GetFSDirectory().GetINode4Write
					(this.file).AsFile();
				BlockInfoContiguousUnderConstruction blkUC = (BlockInfoContiguousUnderConstruction
					)(fileNode.GetBlocks())[1];
				int datanodeNum = blkUC.GetExpectedStorageLocations().Length;
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes && datanodeNum != 2; i++)
				{
					Sharpen.Thread.Sleep(1000);
					datanodeNum = blkUC.GetExpectedStorageLocations().Length;
				}
				return datanodeNum == 2;
			}

			internal override object GetResult()
			{
				return null;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>addCacheDirective</summary>
		internal class AddCacheDirectiveInfoOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly CacheDirectiveInfo directive;

			private long result;

			internal AddCacheDirectiveInfoOp(TestRetryCacheWithHA _enclosing, DFSClient client
				, CacheDirectiveInfo directive)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.directive = directive;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				this.expectedUpdateCount++;
				this._enclosing.dfs.AddCachePool(new CachePoolInfo(this.directive.GetPool()));
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.expectedUpdateCount++;
				this.result = this.client.AddCacheDirective(this.directive, EnumSet.Of(CacheFlag.
					Force));
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes; i++)
				{
					RemoteIterator<CacheDirectiveEntry> iter = this._enclosing.dfs.ListCacheDirectives
						(new CacheDirectiveInfo.Builder().SetPool(this.directive.GetPool()).SetPath(this
						.directive.GetPath()).Build());
					if (iter.HasNext())
					{
						return true;
					}
					Sharpen.Thread.Sleep(1000);
				}
				return false;
			}

			internal override object GetResult()
			{
				return this.result;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>modifyCacheDirective</summary>
		internal class ModifyCacheDirectiveInfoOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly CacheDirectiveInfo directive;

			private readonly short newReplication;

			private long id;

			internal ModifyCacheDirectiveInfoOp(TestRetryCacheWithHA _enclosing, DFSClient client
				, CacheDirectiveInfo directive, short newReplication)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.directive = directive;
				this.newReplication = newReplication;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				this.expectedUpdateCount++;
				this._enclosing.dfs.AddCachePool(new CachePoolInfo(this.directive.GetPool()));
				this.expectedUpdateCount++;
				this.id = this.client.AddCacheDirective(this.directive, EnumSet.Of(CacheFlag.Force
					));
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.expectedUpdateCount++;
				this.client.ModifyCacheDirective(new CacheDirectiveInfo.Builder().SetId(this.id).
					SetReplication(this.newReplication).Build(), EnumSet.Of(CacheFlag.Force));
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes; i++)
				{
					RemoteIterator<CacheDirectiveEntry> iter = this._enclosing.dfs.ListCacheDirectives
						(new CacheDirectiveInfo.Builder().SetPool(this.directive.GetPool()).SetPath(this
						.directive.GetPath()).Build());
					while (iter.HasNext())
					{
						CacheDirectiveInfo result = iter.Next().GetInfo();
						if ((result.GetId() == this.id) && (result.GetReplication() == this.newReplication
							))
						{
							return true;
						}
					}
					Sharpen.Thread.Sleep(1000);
				}
				return false;
			}

			internal override object GetResult()
			{
				return null;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>removeCacheDirective</summary>
		internal class RemoveCacheDirectiveInfoOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly CacheDirectiveInfo directive;

			private long id;

			internal RemoveCacheDirectiveInfoOp(TestRetryCacheWithHA _enclosing, DFSClient client
				, string pool, string path)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.directive = new CacheDirectiveInfo.Builder().SetPool(pool).SetPath(new Path(
					path)).Build();
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				this.expectedUpdateCount++;
				this._enclosing.dfs.AddCachePool(new CachePoolInfo(this.directive.GetPool()));
				this.expectedUpdateCount++;
				this.id = this._enclosing.dfs.AddCacheDirective(this.directive, EnumSet.Of(CacheFlag
					.Force));
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.expectedUpdateCount++;
				this.client.RemoveCacheDirective(this.id);
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes; i++)
				{
					RemoteIterator<CacheDirectiveEntry> iter = this._enclosing.dfs.ListCacheDirectives
						(new CacheDirectiveInfo.Builder().SetPool(this.directive.GetPool()).SetPath(this
						.directive.GetPath()).Build());
					if (!iter.HasNext())
					{
						return true;
					}
					Sharpen.Thread.Sleep(1000);
				}
				return false;
			}

			internal override object GetResult()
			{
				return null;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>addCachePool</summary>
		internal class AddCachePoolOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly string pool;

			internal AddCachePoolOp(TestRetryCacheWithHA _enclosing, DFSClient client, string
				 pool)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.pool = pool;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.expectedUpdateCount++;
				this.client.AddCachePool(new CachePoolInfo(this.pool));
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes; i++)
				{
					RemoteIterator<CachePoolEntry> iter = this._enclosing.dfs.ListCachePools();
					if (iter.HasNext())
					{
						return true;
					}
					Sharpen.Thread.Sleep(1000);
				}
				return false;
			}

			internal override object GetResult()
			{
				return null;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>modifyCachePool</summary>
		internal class ModifyCachePoolOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			internal readonly string pool;

			internal ModifyCachePoolOp(TestRetryCacheWithHA _enclosing, DFSClient client, string
				 pool)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.pool = pool;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				this.expectedUpdateCount++;
				this.client.AddCachePool(new CachePoolInfo(this.pool).SetLimit(10l));
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.expectedUpdateCount++;
				this.client.ModifyCachePool(new CachePoolInfo(this.pool).SetLimit(99l));
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes; i++)
				{
					RemoteIterator<CachePoolEntry> iter = this._enclosing.dfs.ListCachePools();
					if (iter.HasNext() && iter.Next().GetInfo().GetLimit() == 99)
					{
						return true;
					}
					Sharpen.Thread.Sleep(1000);
				}
				return false;
			}

			internal override object GetResult()
			{
				return null;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>removeCachePool</summary>
		internal class RemoveCachePoolOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly string pool;

			internal RemoveCachePoolOp(TestRetryCacheWithHA _enclosing, DFSClient client, string
				 pool)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.pool = pool;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				this.expectedUpdateCount++;
				this.client.AddCachePool(new CachePoolInfo(this.pool));
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.expectedUpdateCount++;
				this.client.RemoveCachePool(this.pool);
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes; i++)
				{
					RemoteIterator<CachePoolEntry> iter = this._enclosing.dfs.ListCachePools();
					if (!iter.HasNext())
					{
						return true;
					}
					Sharpen.Thread.Sleep(1000);
				}
				return false;
			}

			internal override object GetResult()
			{
				return null;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>setXAttr</summary>
		internal class SetXAttrOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly string src;

			internal SetXAttrOp(TestRetryCacheWithHA _enclosing, DFSClient client, string src
				)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.src = src;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				Path p = new Path(this.src);
				if (!this._enclosing.dfs.Exists(p))
				{
					this.expectedUpdateCount++;
					DFSTestUtil.CreateFile(this._enclosing.dfs, p, TestRetryCacheWithHA.BlockSize, TestRetryCacheWithHA
						.DataNodes, 0);
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.expectedUpdateCount++;
				this.client.SetXAttr(this.src, "user.key", Sharpen.Runtime.GetBytesForString("value"
					), EnumSet.Of(XAttrSetFlag.Create));
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes; i++)
				{
					IDictionary<string, byte[]> iter = this._enclosing.dfs.GetXAttrs(new Path(this.src
						));
					ICollection<string> keySet = iter.Keys;
					if (keySet.Contains("user.key"))
					{
						return true;
					}
					Sharpen.Thread.Sleep(1000);
				}
				return false;
			}

			internal override object GetResult()
			{
				return null;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <summary>removeXAttr</summary>
		internal class RemoveXAttrOp : TestRetryCacheWithHA.AtMostOnceOp
		{
			private readonly string src;

			internal RemoveXAttrOp(TestRetryCacheWithHA _enclosing, DFSClient client, string 
				src)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.src = src;
			}

			/// <exception cref="System.Exception"/>
			internal override void Prepare()
			{
				Path p = new Path(this.src);
				if (!this._enclosing.dfs.Exists(p))
				{
					this.expectedUpdateCount++;
					DFSTestUtil.CreateFile(this._enclosing.dfs, p, TestRetryCacheWithHA.BlockSize, TestRetryCacheWithHA
						.DataNodes, 0);
					this.expectedUpdateCount++;
					this.client.SetXAttr(this.src, "user.key", Sharpen.Runtime.GetBytesForString("value"
						), EnumSet.Of(XAttrSetFlag.Create));
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Invoke()
			{
				this.expectedUpdateCount++;
				this.client.RemoveXAttr(this.src, "user.key");
			}

			/// <exception cref="System.Exception"/>
			internal override bool CheckNamenodeBeforeReturn()
			{
				for (int i = 0; i < TestRetryCacheWithHA.Checktimes; i++)
				{
					IDictionary<string, byte[]> iter = this._enclosing.dfs.GetXAttrs(new Path(this.src
						));
					ICollection<string> keySet = iter.Keys;
					if (!keySet.Contains("user.key"))
					{
						return true;
					}
					Sharpen.Thread.Sleep(1000);
				}
				return false;
			}

			internal override object GetResult()
			{
				return null;
			}

			private readonly TestRetryCacheWithHA _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCreateSnapshot()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.CreateSnapshotOp(
				this, client, "/test", "s1");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteSnapshot()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.DeleteSnapshotOp(
				this, client, "/test", "s1");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameSnapshot()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.RenameSnapshotOp(
				this, client, "/test", "s1", "s2");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCreate()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.CreateOp(this, client
				, "/testfile");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppend()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.AppendOp(this, client
				, "/testfile");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRename()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.RenameOp(this, client
				, "/file1", "/file2");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRename2()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.Rename2Op(this, client
				, "/file1", "/file2");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestConcat()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.ConcatOp(this, client
				, new Path("/test/file"), 5);
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDelete()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.DeleteOp(this, client
				, "/testfile");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCreateSymlink()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.CreateSymlinkOp(this
				, client, "/testfile", "/testlink");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUpdatePipeline()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.UpdatePipelineOp(
				this, client, "/testfile");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddCacheDirectiveInfo()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.AddCacheDirectiveInfoOp
				(this, client, new CacheDirectiveInfo.Builder().SetPool("pool").SetPath(new Path
				("/path")).Build());
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestModifyCacheDirectiveInfo()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.ModifyCacheDirectiveInfoOp
				(this, client, new CacheDirectiveInfo.Builder().SetPool("pool").SetPath(new Path
				("/path")).SetReplication((short)1).Build(), (short)555);
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRemoveCacheDescriptor()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.RemoveCacheDirectiveInfoOp
				(this, client, "pool", "/path");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddCachePool()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.AddCachePoolOp(this
				, client, "pool");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestModifyCachePool()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.ModifyCachePoolOp
				(this, client, "pool");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRemoveCachePool()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.RemoveCachePoolOp
				(this, client, "pool");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetXAttr()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.SetXAttrOp(this, 
				client, "/setxattr");
			TestClientRetryWithFailover(op);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRemoveXAttr()
		{
			DFSClient client = GenClientWithDummyHandler();
			TestRetryCacheWithHA.AtMostOnceOp op = new TestRetryCacheWithHA.RemoveXAttrOp(this
				, client, "/removexattr");
			TestClientRetryWithFailover(op);
		}

		/// <summary>
		/// When NN failover happens, if the client did not receive the response and
		/// send a retry request to the other NN, the same response should be recieved
		/// based on the retry cache.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestClientRetryWithFailover(TestRetryCacheWithHA.AtMostOnceOp
			 op)
		{
			IDictionary<string, object> results = new Dictionary<string, object>();
			op.Prepare();
			// set DummyRetryInvocationHandler#block to true
			TestRetryCacheWithHA.DummyRetryInvocationHandler.block.Set(true);
			new _Thread_1286(this, op, results).Start();
			// make sure the client's call has actually been handled by the active NN
			NUnit.Framework.Assert.IsTrue("After waiting the operation " + op.name + " still has not taken effect on NN yet"
				, op.CheckNamenodeBeforeReturn());
			// force the failover
			cluster.TransitionToStandby(0);
			cluster.TransitionToActive(1);
			// disable the block in DummyHandler
			Log.Info("Setting block to false");
			TestRetryCacheWithHA.DummyRetryInvocationHandler.block.Set(false);
			lock (this)
			{
				while (!results.Contains(op.name))
				{
					Sharpen.Runtime.Wait(this);
				}
				Log.Info("Got the result of " + op.name + ": " + results[op.name]);
			}
			// Waiting for failover.
			while (cluster.GetNamesystem(1).IsInStandbyState())
			{
				Sharpen.Thread.Sleep(10);
			}
			long hitNN0 = cluster.GetNamesystem(0).GetRetryCache().GetMetricsForTests().GetCacheHit
				();
			long hitNN1 = cluster.GetNamesystem(1).GetRetryCache().GetMetricsForTests().GetCacheHit
				();
			NUnit.Framework.Assert.IsTrue("CacheHit: " + hitNN0 + ", " + hitNN1, hitNN0 + hitNN1
				 > 0);
			long updatedNN0 = cluster.GetNamesystem(0).GetRetryCache().GetMetricsForTests().GetCacheUpdated
				();
			long updatedNN1 = cluster.GetNamesystem(1).GetRetryCache().GetMetricsForTests().GetCacheUpdated
				();
			// Cache updated metrics on NN0 should be >0 since the op was process on NN0
			NUnit.Framework.Assert.IsTrue("CacheUpdated on NN0: " + updatedNN0, updatedNN0 > 
				0);
			// Cache updated metrics on NN0 should be >0 since NN1 applied the editlog
			NUnit.Framework.Assert.IsTrue("CacheUpdated on NN1: " + updatedNN1, updatedNN1 > 
				0);
			long expectedUpdateCount = op.GetExpectedCacheUpdateCount();
			if (expectedUpdateCount > 0)
			{
				NUnit.Framework.Assert.AreEqual("CacheUpdated on NN0: " + updatedNN0, expectedUpdateCount
					, updatedNN0);
				NUnit.Framework.Assert.AreEqual("CacheUpdated on NN0: " + updatedNN1, expectedUpdateCount
					, updatedNN1);
			}
		}

		private sealed class _Thread_1286 : Sharpen.Thread
		{
			public _Thread_1286(TestRetryCacheWithHA _enclosing, TestRetryCacheWithHA.AtMostOnceOp
				 op, IDictionary<string, object> results)
			{
				this._enclosing = _enclosing;
				this.op = op;
				this.results = results;
			}

			public override void Run()
			{
				try
				{
					op.Invoke();
					object result = op.GetResult();
					TestRetryCacheWithHA.Log.Info("Operation " + op.name + " finished");
					lock (this._enclosing)
					{
						results[op.name] = result == null ? "SUCCESS" : result;
						Sharpen.Runtime.NotifyAll(this._enclosing);
					}
				}
				catch (Exception e)
				{
					TestRetryCacheWithHA.Log.Info("Got Exception while calling " + op.name, e);
				}
				finally
				{
					IOUtils.Cleanup(null, op.client);
				}
			}

			private readonly TestRetryCacheWithHA _enclosing;

			private readonly TestRetryCacheWithHA.AtMostOnceOp op;

			private readonly IDictionary<string, object> results;
		}

		/// <summary>
		/// Add a list of cache pools, list cache pools,
		/// switch active NN, and list cache pools again.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestListCachePools()
		{
			int poolCount = 7;
			HashSet<string> poolNames = new HashSet<string>(poolCount);
			for (int i = 0; i < poolCount; i++)
			{
				string poolName = "testListCachePools-" + i;
				dfs.AddCachePool(new CachePoolInfo(poolName));
				poolNames.AddItem(poolName);
			}
			ListCachePools(poolNames, 0);
			cluster.TransitionToStandby(0);
			cluster.TransitionToActive(1);
			cluster.WaitActive(1);
			ListCachePools(poolNames, 1);
		}

		/// <summary>
		/// Add a list of cache directives, list cache directives,
		/// switch active NN, and list cache directives again.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestListCacheDirectives()
		{
			int poolCount = 7;
			HashSet<string> poolNames = new HashSet<string>(poolCount);
			Path path = new Path("/p");
			for (int i = 0; i < poolCount; i++)
			{
				string poolName = "testListCacheDirectives-" + i;
				CacheDirectiveInfo directiveInfo = new CacheDirectiveInfo.Builder().SetPool(poolName
					).SetPath(path).Build();
				dfs.AddCachePool(new CachePoolInfo(poolName));
				dfs.AddCacheDirective(directiveInfo, EnumSet.Of(CacheFlag.Force));
				poolNames.AddItem(poolName);
			}
			ListCacheDirectives(poolNames, 0);
			cluster.TransitionToStandby(0);
			cluster.TransitionToActive(1);
			cluster.WaitActive(1);
			ListCacheDirectives(poolNames, 1);
		}

		/// <exception cref="System.Exception"/>
		private void ListCachePools(HashSet<string> poolNames, int active)
		{
			HashSet<string> tmpNames = (HashSet<string>)poolNames.Clone();
			RemoteIterator<CachePoolEntry> pools = dfs.ListCachePools();
			int poolCount = poolNames.Count;
			for (int i = 0; i < poolCount; i++)
			{
				CachePoolEntry pool = pools.Next();
				string pollName = pool.GetInfo().GetPoolName();
				NUnit.Framework.Assert.IsTrue("The pool name should be expected", tmpNames.Remove
					(pollName));
				if (i % 2 == 0)
				{
					int standby = active;
					active = (standby == 0) ? 1 : 0;
					cluster.TransitionToStandby(standby);
					cluster.TransitionToActive(active);
					cluster.WaitActive(active);
				}
			}
			NUnit.Framework.Assert.IsTrue("All pools must be found", tmpNames.IsEmpty());
		}

		/// <exception cref="System.Exception"/>
		private void ListCacheDirectives(HashSet<string> poolNames, int active)
		{
			HashSet<string> tmpNames = (HashSet<string>)poolNames.Clone();
			RemoteIterator<CacheDirectiveEntry> directives = dfs.ListCacheDirectives(null);
			int poolCount = poolNames.Count;
			for (int i = 0; i < poolCount; i++)
			{
				CacheDirectiveEntry directive = directives.Next();
				string pollName = directive.GetInfo().GetPool();
				NUnit.Framework.Assert.IsTrue("The pool name should be expected", tmpNames.Remove
					(pollName));
				if (i % 2 == 0)
				{
					int standby = active;
					active = (standby == 0) ? 1 : 0;
					cluster.TransitionToStandby(standby);
					cluster.TransitionToActive(active);
					cluster.WaitActive(active);
				}
			}
			NUnit.Framework.Assert.IsTrue("All pools must be found", tmpNames.IsEmpty());
		}
	}
}
