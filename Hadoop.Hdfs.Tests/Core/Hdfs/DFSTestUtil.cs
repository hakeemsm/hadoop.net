using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Utilities for HDFS tests</summary>
	public class DFSTestUtil
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.DFSTestUtil
			));

		private static readonly Random gen = new Random();

		private static readonly string[] dirNames = new string[] { "zero", "one", "two", 
			"three", "four", "five", "six", "seven", "eight", "nine" };

		private readonly int maxLevels;

		private readonly int maxSize;

		private readonly int minSize;

		private readonly int nFiles;

		private DFSTestUtil.MyFile[] files;

		/// <summary>Creates a new instance of DFSTestUtil</summary>
		/// <param name="nFiles">Number of files to be created</param>
		/// <param name="maxLevels">Maximum number of directory levels</param>
		/// <param name="maxSize">Maximum size for file</param>
		/// <param name="minSize">Minimum size for file</param>
		private DFSTestUtil(int nFiles, int maxLevels, int maxSize, int minSize)
		{
			this.nFiles = nFiles;
			this.maxLevels = maxLevels;
			this.maxSize = maxSize;
			this.minSize = minSize;
		}

		/// <summary>Creates a new instance of DFSTestUtil</summary>
		/// <param name="testName">Name of the test from where this utility is used</param>
		/// <param name="nFiles">Number of files to be created</param>
		/// <param name="maxLevels">Maximum number of directory levels</param>
		/// <param name="maxSize">Maximum size for file</param>
		/// <param name="minSize">Minimum size for file</param>
		public DFSTestUtil(string testName, int nFiles, int maxLevels, int maxSize, int minSize
			)
		{
			this.nFiles = nFiles;
			this.maxLevels = maxLevels;
			this.maxSize = maxSize;
			this.minSize = minSize;
		}

		/// <summary>when formatting a namenode - we must provide clusterid.</summary>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		public static void FormatNameNode(Configuration conf)
		{
			string clusterId = HdfsServerConstants.StartupOption.Format.GetClusterId();
			if (clusterId == null || clusterId.IsEmpty())
			{
				HdfsServerConstants.StartupOption.Format.SetClusterId("testClusterID");
			}
			// Use a copy of conf as it can be altered by namenode during format.
			NameNode.Format(new Configuration(conf));
		}

		/// <summary>Create a new HA-enabled configuration.</summary>
		public static Configuration NewHAConfiguration(string logicalName)
		{
			Configuration conf = new Configuration();
			AddHAConfiguration(conf, logicalName);
			return conf;
		}

		/// <summary>Add a new HA configuration.</summary>
		public static void AddHAConfiguration(Configuration conf, string logicalName)
		{
			string nsIds = conf.Get(DFSConfigKeys.DfsNameservices);
			if (nsIds == null)
			{
				conf.Set(DFSConfigKeys.DfsNameservices, logicalName);
			}
			else
			{
				// append the nsid
				conf.Set(DFSConfigKeys.DfsNameservices, nsIds + "," + logicalName);
			}
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, logicalName
				), "nn1,nn2");
			conf.Set(DFSConfigKeys.DfsClientFailoverProxyProviderKeyPrefix + string.Empty + "."
				 + logicalName, typeof(ConfiguredFailoverProxyProvider).FullName);
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, 1);
		}

		public static void SetFakeHttpAddresses(Configuration conf, string logicalName)
		{
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeHttpAddressKey, logicalName
				, "nn1"), "127.0.0.1:12345");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeHttpAddressKey, logicalName
				, "nn2"), "127.0.0.1:12346");
		}

		public static void SetEditLogForTesting(FSNamesystem fsn, FSEditLog newLog)
		{
			Whitebox.SetInternalState(fsn.GetFSImage(), "editLog", newLog);
			Whitebox.SetInternalState(fsn.GetFSDirectory(), "editLog", newLog);
		}

		/// <summary>
		/// class MyFile contains enough information to recreate the contents of
		/// a single file.
		/// </summary>
		private class MyFile
		{
			private string name = string.Empty;

			private readonly int size;

			private readonly long seed;

			internal MyFile(DFSTestUtil _enclosing)
			{
				this._enclosing = _enclosing;
				int nLevels = DFSTestUtil.gen.Next(this._enclosing.maxLevels);
				if (nLevels != 0)
				{
					int[] levels = new int[nLevels];
					for (int idx = 0; idx < nLevels; idx++)
					{
						levels[idx] = DFSTestUtil.gen.Next(10);
					}
					StringBuilder sb = new StringBuilder();
					for (int idx_1 = 0; idx_1 < nLevels; idx_1++)
					{
						sb.Append(DFSTestUtil.dirNames[levels[idx_1]]);
						sb.Append("/");
					}
					this.name = sb.ToString();
				}
				long fidx = -1;
				while (fidx < 0)
				{
					fidx = DFSTestUtil.gen.NextLong();
				}
				this.name = this.name + System.Convert.ToString(fidx);
				this.size = this._enclosing.minSize + DFSTestUtil.gen.Next(this._enclosing.maxSize
					 - this._enclosing.minSize);
				this.seed = DFSTestUtil.gen.NextLong();
			}

			internal virtual string GetName()
			{
				return this.name;
			}

			internal virtual int GetSize()
			{
				return this.size;
			}

			internal virtual long GetSeed()
			{
				return this.seed;
			}

			private readonly DFSTestUtil _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CreateFiles(FileSystem fs, string topdir)
		{
			CreateFiles(fs, topdir, (short)3);
		}

		/// <summary>
		/// create nFiles with random names and directory hierarchies
		/// with random (but reproducible) data in them.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CreateFiles(FileSystem fs, string topdir, short replicationFactor
			)
		{
			files = new DFSTestUtil.MyFile[nFiles];
			for (int idx = 0; idx < nFiles; idx++)
			{
				files[idx] = new DFSTestUtil.MyFile(this);
			}
			Path root = new Path(topdir);
			for (int idx_1 = 0; idx_1 < nFiles; idx_1++)
			{
				CreateFile(fs, new Path(root, files[idx_1].GetName()), files[idx_1].GetSize(), replicationFactor
					, files[idx_1].GetSeed());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static string ReadFile(FileSystem fs, Path fileName)
		{
			byte[] buf = ReadFileBuffer(fs, fileName);
			return Sharpen.Runtime.GetStringForBytes(buf, 0, buf.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		public static byte[] ReadFileBuffer(FileSystem fs, Path fileName)
		{
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			try
			{
				FSDataInputStream @in = fs.Open(fileName);
				try
				{
					IOUtils.CopyBytes(@in, os, 1024, true);
					return os.ToByteArray();
				}
				finally
				{
					@in.Close();
				}
			}
			finally
			{
				os.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CreateFile(FileSystem fs, Path fileName, long fileLen, short replFactor
			, long seed)
		{
			if (!fs.Mkdirs(fileName.GetParent()))
			{
				throw new IOException("Mkdirs failed to create " + fileName.GetParent().ToString(
					));
			}
			FSDataOutputStream @out = null;
			try
			{
				@out = fs.Create(fileName, replFactor);
				byte[] toWrite = new byte[1024];
				Random rb = new Random(seed);
				long bytesToWrite = fileLen;
				while (bytesToWrite > 0)
				{
					rb.NextBytes(toWrite);
					int bytesToWriteNext = (1024 < bytesToWrite) ? 1024 : (int)bytesToWrite;
					@out.Write(toWrite, 0, bytesToWriteNext);
					bytesToWrite -= bytesToWriteNext;
				}
				@out.Close();
				@out = null;
			}
			finally
			{
				IOUtils.CloseStream(@out);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CreateFile(FileSystem fs, Path fileName, int bufferLen, long fileLen
			, long blockSize, short replFactor, long seed)
		{
			CreateFile(fs, fileName, false, bufferLen, fileLen, blockSize, replFactor, seed, 
				false);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CreateFile(FileSystem fs, Path fileName, bool isLazyPersist, int
			 bufferLen, long fileLen, long blockSize, short replFactor, long seed, bool flush
			)
		{
			CreateFile(fs, fileName, isLazyPersist, bufferLen, fileLen, blockSize, replFactor
				, seed, flush, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CreateFile(FileSystem fs, Path fileName, bool isLazyPersist, int
			 bufferLen, long fileLen, long blockSize, short replFactor, long seed, bool flush
			, IPEndPoint[] favoredNodes)
		{
			System.Diagnostics.Debug.Assert(bufferLen > 0);
			if (!fs.Mkdirs(fileName.GetParent()))
			{
				throw new IOException("Mkdirs failed to create " + fileName.GetParent().ToString(
					));
			}
			FSDataOutputStream @out = null;
			EnumSet<CreateFlag> createFlags = EnumSet.Of(CreateFlag.Create);
			createFlags.AddItem(CreateFlag.Overwrite);
			if (isLazyPersist)
			{
				createFlags.AddItem(CreateFlag.LazyPersist);
			}
			try
			{
				if (favoredNodes == null)
				{
					@out = fs.Create(fileName, FsPermission.GetFileDefault(), createFlags, fs.GetConf
						().GetInt(CommonConfigurationKeys.IoFileBufferSizeKey, 4096), replFactor, blockSize
						, null);
				}
				else
				{
					@out = ((DistributedFileSystem)fs).Create(fileName, FsPermission.GetDefault(), true
						, bufferLen, replFactor, blockSize, null, favoredNodes);
				}
				if (fileLen > 0)
				{
					byte[] toWrite = new byte[bufferLen];
					Random rb = new Random(seed);
					long bytesToWrite = fileLen;
					while (bytesToWrite > 0)
					{
						rb.NextBytes(toWrite);
						int bytesToWriteNext = (bufferLen < bytesToWrite) ? bufferLen : (int)bytesToWrite;
						@out.Write(toWrite, 0, bytesToWriteNext);
						bytesToWrite -= bytesToWriteNext;
					}
					if (flush)
					{
						@out.Hsync();
					}
				}
			}
			finally
			{
				if (@out != null)
				{
					@out.Close();
				}
			}
		}

		public static byte[] CalculateFileContentsFromSeed(long seed, int length)
		{
			Random rb = new Random(seed);
			byte[] val = new byte[length];
			rb.NextBytes(val);
			return val;
		}

		/// <summary>check if the files have been copied correctly.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool CheckFiles(FileSystem fs, string topdir)
		{
			Path root = new Path(topdir);
			for (int idx = 0; idx < nFiles; idx++)
			{
				Path fPath = new Path(root, files[idx].GetName());
				FSDataInputStream @in = fs.Open(fPath);
				byte[] toRead = new byte[files[idx].GetSize()];
				byte[] toCompare = new byte[files[idx].GetSize()];
				Random rb = new Random(files[idx].GetSeed());
				rb.NextBytes(toCompare);
				@in.ReadFully(0, toRead);
				@in.Close();
				for (int i = 0; i < toRead.Length; i++)
				{
					if (toRead[i] != toCompare[i])
					{
						return false;
					}
				}
				toRead = null;
				toCompare = null;
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetReplication(FileSystem fs, string topdir, short value)
		{
			Path root = new Path(topdir);
			for (int idx = 0; idx < nFiles; idx++)
			{
				Path fPath = new Path(root, files[idx].GetName());
				fs.SetReplication(fPath, value);
			}
		}

		/*
		* Waits for the replication factor of all files to reach the
		* specified target.
		*/
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		public virtual void WaitReplication(FileSystem fs, string topdir, short value)
		{
			Path root = new Path(topdir);
			for (int idx = 0; idx < nFiles; idx++)
			{
				WaitReplication(fs, new Path(root, files[idx].GetName()), value);
			}
		}

		/*
		* Check if the given block in the given file is corrupt.
		*/
		/// <exception cref="System.IO.IOException"/>
		public static bool AllBlockReplicasCorrupt(MiniDFSCluster cluster, Path file, int
			 blockNo)
		{
			DFSClient client = new DFSClient(new IPEndPoint("localhost", cluster.GetNameNodePort
				()), cluster.GetConfiguration(0));
			LocatedBlocks blocks;
			try
			{
				blocks = client.GetNamenode().GetBlockLocations(file.ToString(), 0, long.MaxValue
					);
			}
			finally
			{
				client.Close();
			}
			return blocks.Get(blockNo).IsCorrupt();
		}

		/*
		* Wait up to 20s for the given block to be replicated across
		* the requested number of racks, with the requested number of
		* replicas, and the requested number of replicas still needed.
		*/
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		public static void WaitForReplication(MiniDFSCluster cluster, ExtendedBlock b, int
			 racks, int replicas, int neededReplicas)
		{
			int curRacks = 0;
			int curReplicas = 0;
			int curNeededReplicas = 0;
			int count = 0;
			int Attempts = 20;
			do
			{
				Sharpen.Thread.Sleep(1000);
				int[] r = BlockManagerTestUtil.GetReplicaInfo(cluster.GetNamesystem(), b.GetLocalBlock
					());
				curRacks = r[0];
				curReplicas = r[1];
				curNeededReplicas = r[2];
				count++;
			}
			while ((curRacks != racks || curReplicas != replicas || curNeededReplicas != neededReplicas
				) && count < Attempts);
			if (count == Attempts)
			{
				throw new TimeoutException("Timed out waiting for replication." + " Needed replicas = "
					 + neededReplicas + " Cur needed replicas = " + curNeededReplicas + " Replicas = "
					 + replicas + " Cur replicas = " + curReplicas + " Racks = " + racks + " Cur racks = "
					 + curRacks);
			}
		}

		/// <summary>
		/// Keep accessing the given file until the namenode reports that the
		/// given block in the file contains the given number of corrupt replicas.
		/// </summary>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		public static void WaitCorruptReplicas(FileSystem fs, FSNamesystem ns, Path file, 
			ExtendedBlock b, int corruptRepls)
		{
			int count = 0;
			int Attempts = 50;
			int repls = ns.GetBlockManager().NumCorruptReplicas(b.GetLocalBlock());
			while (repls != corruptRepls && count < Attempts)
			{
				try
				{
					IOUtils.CopyBytes(fs.Open(file), new IOUtils.NullOutputStream(), 512, true);
				}
				catch (IOException)
				{
				}
				// Swallow exceptions
				System.Console.Out.WriteLine("Waiting for " + corruptRepls + " corrupt replicas");
				count++;
				// check more often so corrupt block reports are not easily missed
				for (int i = 0; i < 10; i++)
				{
					repls = ns.GetBlockManager().NumCorruptReplicas(b.GetLocalBlock());
					Sharpen.Thread.Sleep(100);
					if (repls == corruptRepls)
					{
						break;
					}
				}
			}
			if (count == Attempts)
			{
				throw new TimeoutException("Timed out waiting for corrupt replicas." + " Waiting for "
					 + corruptRepls + ", but only found " + repls);
			}
		}

		/*
		* Wait up to 20s for the given DN (IP:port) to be decommissioned
		*/
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		public static void WaitForDecommission(FileSystem fs, string name)
		{
			DatanodeInfo dn = null;
			int count = 0;
			int Attempts = 20;
			do
			{
				Sharpen.Thread.Sleep(1000);
				DistributedFileSystem dfs = (DistributedFileSystem)fs;
				foreach (DatanodeInfo info in dfs.GetDataNodeStats())
				{
					if (name.Equals(info.GetXferAddr()))
					{
						dn = info;
					}
				}
				count++;
			}
			while ((dn == null || dn.IsDecommissionInProgress() || !dn.IsDecommissioned()) &&
				 count < Attempts);
			if (count == Attempts)
			{
				throw new TimeoutException("Timed out waiting for datanode " + name + " to decommission."
					);
			}
		}

		/*
		* Returns the index of the first datanode which has a copy
		* of the given block, or -1 if no such datanode exists.
		*/
		/// <exception cref="System.IO.IOException"/>
		public static int FirstDnWithBlock(MiniDFSCluster cluster, ExtendedBlock b)
		{
			int numDatanodes = cluster.GetDataNodes().Count;
			for (int i = 0; i < numDatanodes; i++)
			{
				string blockContent = cluster.ReadBlockOnDataNode(i, b);
				if (blockContent != null)
				{
					return i;
				}
			}
			return -1;
		}

		/*
		* Return the total capacity of all live DNs.
		*/
		public static long GetLiveDatanodeCapacity(DatanodeManager dm)
		{
			IList<DatanodeDescriptor> live = new AList<DatanodeDescriptor>();
			dm.FetchDatanodes(live, null, false);
			long capacity = 0;
			foreach (DatanodeDescriptor dn in live)
			{
				capacity += dn.GetCapacity();
			}
			return capacity;
		}

		/*
		* Return the capacity of the given live DN.
		*/
		public static long GetDatanodeCapacity(DatanodeManager dm, int index)
		{
			IList<DatanodeDescriptor> live = new AList<DatanodeDescriptor>();
			dm.FetchDatanodes(live, null, false);
			return live[index].GetCapacity();
		}

		/*
		* Wait for the given # live/dead DNs, total capacity, and # vol failures.
		*/
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		public static void WaitForDatanodeStatus(DatanodeManager dm, int expectedLive, int
			 expectedDead, long expectedVolFails, long expectedTotalCapacity, long timeout)
		{
			IList<DatanodeDescriptor> live = new AList<DatanodeDescriptor>();
			IList<DatanodeDescriptor> dead = new AList<DatanodeDescriptor>();
			int Attempts = 10;
			int count = 0;
			long currTotalCapacity = 0;
			int volFails = 0;
			do
			{
				Sharpen.Thread.Sleep(timeout);
				live.Clear();
				dead.Clear();
				dm.FetchDatanodes(live, dead, false);
				currTotalCapacity = 0;
				volFails = 0;
				foreach (DatanodeDescriptor dd in live)
				{
					currTotalCapacity += dd.GetCapacity();
					volFails += dd.GetVolumeFailures();
				}
				count++;
			}
			while ((expectedLive != live.Count || expectedDead != dead.Count || expectedTotalCapacity
				 != currTotalCapacity || expectedVolFails != volFails) && count < Attempts);
			if (count == Attempts)
			{
				throw new TimeoutException("Timed out waiting for capacity." + " Live = " + live.
					Count + " Expected = " + expectedLive + " Dead = " + dead.Count + " Expected = "
					 + expectedDead + " Total capacity = " + currTotalCapacity + " Expected = " + expectedTotalCapacity
					 + " Vol Fails = " + volFails + " Expected = " + expectedVolFails);
			}
		}

		/*
		* Wait for the given DN to consider itself dead.
		*/
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		public static void WaitForDatanodeDeath(DataNode dn)
		{
			int Attempts = 10;
			int count = 0;
			do
			{
				Sharpen.Thread.Sleep(1000);
				count++;
			}
			while (dn.IsDatanodeUp() && count < Attempts);
			if (count == Attempts)
			{
				throw new TimeoutException("Timed out waiting for DN to die");
			}
		}

		/// <summary>return list of filenames created as part of createFiles</summary>
		public virtual string[] GetFileNames(string topDir)
		{
			if (nFiles == 0)
			{
				return new string[] {  };
			}
			else
			{
				string[] fileNames = new string[nFiles];
				for (int idx = 0; idx < nFiles; idx++)
				{
					fileNames[idx] = topDir + "/" + files[idx].GetName();
				}
				return fileNames;
			}
		}

		/// <summary>Wait for the given file to reach the given replication factor.</summary>
		/// <exception cref="Sharpen.TimeoutException">if we fail to sufficiently replicate the file
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static void WaitReplication(FileSystem fs, Path fileName, short replFactor
			)
		{
			bool correctReplFactor;
			int Attempts = 40;
			int count = 0;
			do
			{
				correctReplFactor = true;
				BlockLocation[] locs = fs.GetFileBlockLocations(fs.GetFileStatus(fileName), 0, long.MaxValue
					);
				count++;
				for (int j = 0; j < locs.Length; j++)
				{
					string[] hostnames = locs[j].GetNames();
					if (hostnames.Length != replFactor)
					{
						correctReplFactor = false;
						System.Console.Out.WriteLine("Block " + j + " of file " + fileName + " has replication factor "
							 + hostnames.Length + " (desired " + replFactor + "); locations " + Joiner.On(' '
							).Join(hostnames));
						Sharpen.Thread.Sleep(1000);
						break;
					}
				}
				if (correctReplFactor)
				{
					System.Console.Out.WriteLine("All blocks of file " + fileName + " verified to have replication factor "
						 + replFactor);
				}
			}
			while (!correctReplFactor && count < Attempts);
			if (count == Attempts)
			{
				throw new TimeoutException("Timed out waiting for " + fileName + " to reach " + replFactor
					 + " replicas");
			}
		}

		/// <summary>delete directory and everything underneath it.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Cleanup(FileSystem fs, string topdir)
		{
			Path root = new Path(topdir);
			fs.Delete(root, true);
			files = null;
		}

		/// <exception cref="System.IO.IOException"/>
		public static ExtendedBlock GetFirstBlock(FileSystem fs, Path path)
		{
			HdfsDataInputStream @in = (HdfsDataInputStream)fs.Open(path);
			try
			{
				@in.ReadByte();
				return @in.GetCurrentBlock();
			}
			finally
			{
				@in.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static IList<LocatedBlock> GetAllBlocks(FSDataInputStream @in)
		{
			return ((HdfsDataInputStream)@in).GetAllBlocks();
		}

		/// <exception cref="System.IO.IOException"/>
		public static IList<LocatedBlock> GetAllBlocks(FileSystem fs, Path path)
		{
			HdfsDataInputStream @in = (HdfsDataInputStream)fs.Open(path);
			return @in.GetAllBlocks();
		}

		public static Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> GetBlockToken
			(FSDataOutputStream @out)
		{
			return ((DFSOutputStream)@out.GetWrappedStream()).GetBlockToken();
		}

		/// <exception cref="System.IO.IOException"/>
		public static string ReadFile(FilePath f)
		{
			StringBuilder b = new StringBuilder();
			BufferedReader @in = new BufferedReader(new FileReader(f));
			for (int c; (c = @in.Read()) != -1; b.Append((char)c))
			{
			}
			@in.Close();
			return b.ToString();
		}

		/* Write the given string to the given file */
		/// <exception cref="System.IO.IOException"/>
		public static void WriteFile(FileSystem fs, Path p, string s)
		{
			if (fs.Exists(p))
			{
				fs.Delete(p, true);
			}
			InputStream @is = new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString(s));
			FSDataOutputStream os = fs.Create(p);
			IOUtils.CopyBytes(@is, os, s.Length, true);
		}

		/* Append the given string to the given file */
		/// <exception cref="System.IO.IOException"/>
		public static void AppendFile(FileSystem fs, Path p, string s)
		{
			System.Diagnostics.Debug.Assert(fs.Exists(p));
			InputStream @is = new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString(s));
			FSDataOutputStream os = fs.Append(p);
			IOUtils.CopyBytes(@is, os, s.Length, true);
		}

		/// <summary>Append specified length of bytes to a given file</summary>
		/// <param name="fs">The file system</param>
		/// <param name="p">Path of the file to append</param>
		/// <param name="length">Length of bytes to append to the file</param>
		/// <exception cref="System.IO.IOException"/>
		public static void AppendFile(FileSystem fs, Path p, int length)
		{
			System.Diagnostics.Debug.Assert(fs.Exists(p));
			System.Diagnostics.Debug.Assert(length >= 0);
			byte[] toAppend = new byte[length];
			Random random = new Random();
			random.NextBytes(toAppend);
			FSDataOutputStream @out = fs.Append(p);
			@out.Write(toAppend);
			@out.Close();
		}

		/// <returns>url content as string (UTF-8 encoding assumed)</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string UrlGet(Uri url)
		{
			return new string(UrlGetBytes(url), Charsets.Utf8);
		}

		/// <returns>URL contents as a byte array</returns>
		/// <exception cref="System.IO.IOException"/>
		public static byte[] UrlGetBytes(Uri url)
		{
			URLConnection conn = url.OpenConnection();
			HttpURLConnection hc = (HttpURLConnection)conn;
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, hc.GetResponseCode());
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			IOUtils.CopyBytes(conn.GetInputStream(), @out, 4096, true);
			return @out.ToByteArray();
		}

		/// <summary>mock class to get group mapping for fake users</summary>
		internal class MockUnixGroupsMapping : ShellBasedUnixGroupsMapping
		{
			internal static IDictionary<string, string[]> fakeUser2GroupsMap;

			private static readonly IList<string> defaultGroups;

			static MockUnixGroupsMapping()
			{
				defaultGroups = new AList<string>(1);
				defaultGroups.AddItem("supergroup");
				fakeUser2GroupsMap = new Dictionary<string, string[]>();
			}

			/// <exception cref="System.IO.IOException"/>
			public override IList<string> GetGroups(string user)
			{
				bool found = false;
				// check to see if this is one of fake users
				IList<string> l = new AList<string>();
				foreach (string u in fakeUser2GroupsMap.Keys)
				{
					if (user.Equals(u))
					{
						found = true;
						foreach (string gr in fakeUser2GroupsMap[u])
						{
							l.AddItem(gr);
						}
					}
				}
				// default
				if (!found)
				{
					l = base.GetGroups(user);
					if (l.Count == 0)
					{
						System.Console.Out.WriteLine("failed to get real group for " + user + "; using default"
							);
						return defaultGroups;
					}
				}
				return l;
			}
		}

		/// <summary>update the configuration with fake class for mapping user to groups</summary>
		/// <param name="conf"/>
		/// <param name="map">- user to groups mapping</param>
		public static void UpdateConfWithFakeGroupMapping(Configuration conf, IDictionary
			<string, string[]> map)
		{
			if (map != null)
			{
				DFSTestUtil.MockUnixGroupsMapping.fakeUser2GroupsMap = map;
			}
			// fake mapping user to groups
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(DFSTestUtil.MockUnixGroupsMapping
				), typeof(ShellBasedUnixGroupsMapping));
		}

		/// <summary>Get a FileSystem instance as specified user in a doAs block.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static FileSystem GetFileSystemAs(UserGroupInformation ugi, Configuration 
			conf)
		{
			try
			{
				return ugi.DoAs(new _PrivilegedExceptionAction_882(conf));
			}
			catch (Exception e)
			{
				throw (ThreadInterruptedException)Sharpen.Extensions.InitCause(new ThreadInterruptedException
					(), e);
			}
		}

		private sealed class _PrivilegedExceptionAction_882 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_882(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return FileSystem.Get(conf);
			}

			private readonly Configuration conf;
		}

		public static byte[] GenerateSequentialBytes(int start, int length)
		{
			byte[] result = new byte[length];
			for (int i = 0; i < length; i++)
			{
				result[i] = unchecked((byte)((start + i) % 127));
			}
			return result;
		}

		public static FileSystem.Statistics GetStatistics(FileSystem fs)
		{
			return FileSystem.GetStatistics(fs.GetUri().GetScheme(), fs.GetType());
		}

		/// <summary>Load file into byte[]</summary>
		/// <exception cref="System.IO.IOException"/>
		public static byte[] LoadFile(string filename)
		{
			FilePath file = new FilePath(filename);
			DataInputStream @in = new DataInputStream(new FileInputStream(file));
			byte[] content = new byte[(int)file.Length()];
			try
			{
				@in.ReadFully(content);
			}
			finally
			{
				IOUtils.Cleanup(Log, @in);
			}
			return content;
		}

		/// <summary>
		/// For
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.TestTransferRbw"/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static DataTransferProtos.BlockOpResponseProto TransferRbw(ExtendedBlock b
			, DFSClient dfsClient, params DatanodeInfo[] datanodes)
		{
			NUnit.Framework.Assert.AreEqual(2, datanodes.Length);
			Socket s = DFSOutputStream.CreateSocketForPipeline(datanodes[0], datanodes.Length
				, dfsClient);
			long writeTimeout = dfsClient.GetDatanodeWriteTimeout(datanodes.Length);
			DataOutputStream @out = new DataOutputStream(new BufferedOutputStream(NetUtils.GetOutputStream
				(s, writeTimeout), HdfsConstants.SmallBufferSize));
			DataInputStream @in = new DataInputStream(NetUtils.GetInputStream(s));
			// send the request
			new Sender(@out).TransferBlock(b, new Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier
				>(), dfsClient.clientName, new DatanodeInfo[] { datanodes[1] }, new StorageType[
				] { StorageType.Default });
			@out.Flush();
			return DataTransferProtos.BlockOpResponseProto.ParseDelimitedFrom(@in);
		}

		public static void SetFederatedConfiguration(MiniDFSCluster cluster, Configuration
			 conf)
		{
			ICollection<string> nameservices = new HashSet<string>();
			foreach (MiniDFSCluster.NameNodeInfo info in cluster.GetNameNodeInfos())
			{
				System.Diagnostics.Debug.Assert(info.nameserviceId != null);
				nameservices.AddItem(info.nameserviceId);
				conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, info.nameserviceId
					), DFSUtil.CreateUri(HdfsConstants.HdfsUriScheme, info.nameNode.GetNameNodeAddress
					()).ToString());
				conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, info
					.nameserviceId), DFSUtil.CreateUri(HdfsConstants.HdfsUriScheme, info.nameNode.GetNameNodeAddress
					()).ToString());
			}
			conf.Set(DFSConfigKeys.DfsNameservices, Joiner.On(",").Join(nameservices));
		}

		public static void SetFederatedHAConfiguration(MiniDFSCluster cluster, Configuration
			 conf)
		{
			IDictionary<string, IList<string>> nameservices = Maps.NewHashMap();
			foreach (MiniDFSCluster.NameNodeInfo info in cluster.GetNameNodeInfos())
			{
				Preconditions.CheckState(info.nameserviceId != null);
				IList<string> nns = nameservices[info.nameserviceId];
				if (nns == null)
				{
					nns = Lists.NewArrayList();
					nameservices[info.nameserviceId] = nns;
				}
				nns.AddItem(info.nnId);
				conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, info.nameserviceId
					, info.nnId), DFSUtil.CreateUri(HdfsConstants.HdfsUriScheme, info.nameNode.GetNameNodeAddress
					()).ToString());
				conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, info
					.nameserviceId, info.nnId), DFSUtil.CreateUri(HdfsConstants.HdfsUriScheme, info.
					nameNode.GetNameNodeAddress()).ToString());
			}
			foreach (KeyValuePair<string, IList<string>> entry in nameservices)
			{
				conf.Set(DFSUtil.AddKeySuffixes(DfsHaNamenodesKeyPrefix, entry.Key), Joiner.On(","
					).Join(entry.Value));
				conf.Set(DfsClientFailoverProxyProviderKeyPrefix + "." + entry.Key, typeof(ConfiguredFailoverProxyProvider
					).FullName);
			}
			conf.Set(DFSConfigKeys.DfsNameservices, Joiner.On(",").Join(nameservices.Keys));
		}

		private static DatanodeID GetDatanodeID(string ipAddr)
		{
			return new DatanodeID(ipAddr, "localhost", UUID.RandomUUID().ToString(), DFSConfigKeys
				.DfsDatanodeDefaultPort, DFSConfigKeys.DfsDatanodeHttpDefaultPort, DFSConfigKeys
				.DfsDatanodeHttpsDefaultPort, DFSConfigKeys.DfsDatanodeIpcDefaultPort);
		}

		public static DatanodeID GetLocalDatanodeID()
		{
			return GetDatanodeID("127.0.0.1");
		}

		public static DatanodeID GetLocalDatanodeID(int port)
		{
			return new DatanodeID("127.0.0.1", "localhost", UUID.RandomUUID().ToString(), port
				, port, port, port);
		}

		public static DatanodeDescriptor GetLocalDatanodeDescriptor()
		{
			return new DatanodeDescriptor(GetLocalDatanodeID());
		}

		public static DatanodeInfo GetLocalDatanodeInfo()
		{
			return new DatanodeInfo(GetLocalDatanodeID());
		}

		public static DatanodeInfo GetDatanodeInfo(string ipAddr)
		{
			return new DatanodeInfo(GetDatanodeID(ipAddr));
		}

		public static DatanodeInfo GetLocalDatanodeInfo(int port)
		{
			return new DatanodeInfo(GetLocalDatanodeID(port));
		}

		public static DatanodeInfo GetDatanodeInfo(string ipAddr, string host, int port)
		{
			return new DatanodeInfo(new DatanodeID(ipAddr, host, UUID.RandomUUID().ToString()
				, port, DFSConfigKeys.DfsDatanodeHttpDefaultPort, DFSConfigKeys.DfsDatanodeHttpsDefaultPort
				, DFSConfigKeys.DfsDatanodeIpcDefaultPort));
		}

		public static DatanodeInfo GetLocalDatanodeInfo(string ipAddr, string hostname, DatanodeInfo.AdminStates
			 adminState)
		{
			return new DatanodeInfo(ipAddr, hostname, string.Empty, DFSConfigKeys.DfsDatanodeDefaultPort
				, DFSConfigKeys.DfsDatanodeHttpDefaultPort, DFSConfigKeys.DfsDatanodeHttpsDefaultPort
				, DFSConfigKeys.DfsDatanodeIpcDefaultPort, 1l, 2l, 3l, 4l, 0l, 0l, 0l, 5, 6, "local"
				, adminState);
		}

		public static DatanodeDescriptor GetDatanodeDescriptor(string ipAddr, string rackLocation
			)
		{
			return GetDatanodeDescriptor(ipAddr, DFSConfigKeys.DfsDatanodeDefaultPort, rackLocation
				);
		}

		public static DatanodeDescriptor GetDatanodeDescriptor(string ipAddr, string rackLocation
			, string hostname)
		{
			return GetDatanodeDescriptor(ipAddr, DFSConfigKeys.DfsDatanodeDefaultPort, rackLocation
				, hostname);
		}

		public static DatanodeStorageInfo CreateDatanodeStorageInfo(string storageID, string
			 ip)
		{
			return CreateDatanodeStorageInfo(storageID, ip, "defaultRack", "host");
		}

		public static DatanodeStorageInfo[] CreateDatanodeStorageInfos(string[] racks)
		{
			return CreateDatanodeStorageInfos(racks, null);
		}

		public static DatanodeStorageInfo[] CreateDatanodeStorageInfos(string[] racks, string
			[] hostnames)
		{
			return CreateDatanodeStorageInfos(racks.Length, racks, hostnames);
		}

		public static DatanodeStorageInfo[] CreateDatanodeStorageInfos(int n)
		{
			return CreateDatanodeStorageInfos(n, null, null);
		}

		public static DatanodeStorageInfo[] CreateDatanodeStorageInfos(int n, string[] racks
			, string[] hostnames)
		{
			return CreateDatanodeStorageInfos(n, racks, hostnames, null);
		}

		public static DatanodeStorageInfo[] CreateDatanodeStorageInfos(int n, string[] racks
			, string[] hostnames, StorageType[] types)
		{
			DatanodeStorageInfo[] storages = new DatanodeStorageInfo[n];
			for (int i = storages.Length; i > 0; )
			{
				string storageID = "s" + i;
				string ip = i + "." + i + "." + i + "." + i;
				i--;
				string rack = (racks != null && i < racks.Length) ? racks[i] : "defaultRack";
				string hostname = (hostnames != null && i < hostnames.Length) ? hostnames[i] : "host";
				StorageType type = (types != null && i < types.Length) ? types[i] : StorageType.Default;
				storages[i] = CreateDatanodeStorageInfo(storageID, ip, rack, hostname, type);
			}
			return storages;
		}

		public static DatanodeStorageInfo CreateDatanodeStorageInfo(string storageID, string
			 ip, string rack, string hostname)
		{
			return CreateDatanodeStorageInfo(storageID, ip, rack, hostname, StorageType.Default
				);
		}

		public static DatanodeStorageInfo CreateDatanodeStorageInfo(string storageID, string
			 ip, string rack, string hostname, StorageType type)
		{
			DatanodeStorage storage = new DatanodeStorage(storageID, DatanodeStorage.State.Normal
				, type);
			DatanodeDescriptor dn = BlockManagerTestUtil.GetDatanodeDescriptor(ip, rack, storage
				, hostname);
			return BlockManagerTestUtil.NewDatanodeStorageInfo(dn, storage);
		}

		public static DatanodeDescriptor[] ToDatanodeDescriptor(DatanodeStorageInfo[] storages
			)
		{
			DatanodeDescriptor[] datanodes = new DatanodeDescriptor[storages.Length];
			for (int i = 0; i < datanodes.Length; i++)
			{
				datanodes[i] = storages[i].GetDatanodeDescriptor();
			}
			return datanodes;
		}

		public static DatanodeDescriptor GetDatanodeDescriptor(string ipAddr, int port, string
			 rackLocation, string hostname)
		{
			DatanodeID dnId = new DatanodeID(ipAddr, hostname, UUID.RandomUUID().ToString(), 
				port, DFSConfigKeys.DfsDatanodeHttpDefaultPort, DFSConfigKeys.DfsDatanodeHttpsDefaultPort
				, DFSConfigKeys.DfsDatanodeIpcDefaultPort);
			return new DatanodeDescriptor(dnId, rackLocation);
		}

		public static DatanodeDescriptor GetDatanodeDescriptor(string ipAddr, int port, string
			 rackLocation)
		{
			return GetDatanodeDescriptor(ipAddr, port, rackLocation, "host");
		}

		public static DatanodeRegistration GetLocalDatanodeRegistration()
		{
			return new DatanodeRegistration(GetLocalDatanodeID(), new StorageInfo(HdfsServerConstants.NodeType
				.DataNode), new ExportedBlockKeys(), VersionInfo.GetVersion());
		}

		/// <summary>Copy one file's contents into the other</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void CopyFile(FilePath src, FilePath dest)
		{
			FileUtils.CopyFile(src, dest);
		}

		public class Builder
		{
			private int maxLevels = 3;

			private int maxSize = 8 * 1024;

			private int minSize = 1;

			private int nFiles = 1;

			public Builder()
			{
			}

			public virtual DFSTestUtil.Builder SetName(string @string)
			{
				return this;
			}

			public virtual DFSTestUtil.Builder SetNumFiles(int nFiles)
			{
				this.nFiles = nFiles;
				return this;
			}

			public virtual DFSTestUtil.Builder SetMaxLevels(int maxLevels)
			{
				this.maxLevels = maxLevels;
				return this;
			}

			public virtual DFSTestUtil.Builder SetMaxSize(int maxSize)
			{
				this.maxSize = maxSize;
				return this;
			}

			public virtual DFSTestUtil.Builder SetMinSize(int minSize)
			{
				this.minSize = minSize;
				return this;
			}

			public virtual DFSTestUtil Build()
			{
				return new DFSTestUtil(nFiles, maxLevels, maxSize, minSize);
			}
		}

		/// <summary>Run a set of operations and generate all edit logs</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void RunOperations(MiniDFSCluster cluster, DistributedFileSystem filesystem
			, Configuration conf, long blockSize, int nnIndex)
		{
			// create FileContext for rename2
			FileContext fc = FileContext.GetFileContext(cluster.GetURI(0), conf);
			// OP_ADD 0
			Path pathFileCreate = new Path("/file_create");
			FSDataOutputStream s = filesystem.Create(pathFileCreate);
			// OP_CLOSE 9
			s.Close();
			// OP_APPEND 47
			FSDataOutputStream s2 = filesystem.Append(pathFileCreate, 4096, null);
			s2.Close();
			// OP_SET_STORAGE_POLICY 45
			filesystem.SetStoragePolicy(pathFileCreate, HdfsConstants.HotStoragePolicyName);
			// OP_RENAME_OLD 1
			Path pathFileMoved = new Path("/file_moved");
			filesystem.Rename(pathFileCreate, pathFileMoved);
			// OP_DELETE 2
			filesystem.Delete(pathFileMoved, false);
			// OP_MKDIR 3
			Path pathDirectoryMkdir = new Path("/directory_mkdir");
			filesystem.Mkdirs(pathDirectoryMkdir);
			// OP_ALLOW_SNAPSHOT 29
			filesystem.AllowSnapshot(pathDirectoryMkdir);
			// OP_DISALLOW_SNAPSHOT 30
			filesystem.DisallowSnapshot(pathDirectoryMkdir);
			// OP_CREATE_SNAPSHOT 26
			string ssName = "snapshot1";
			filesystem.AllowSnapshot(pathDirectoryMkdir);
			filesystem.CreateSnapshot(pathDirectoryMkdir, ssName);
			// OP_RENAME_SNAPSHOT 28
			string ssNewName = "snapshot2";
			filesystem.RenameSnapshot(pathDirectoryMkdir, ssName, ssNewName);
			// OP_DELETE_SNAPSHOT 27
			filesystem.DeleteSnapshot(pathDirectoryMkdir, ssNewName);
			// OP_SET_REPLICATION 4
			s = filesystem.Create(pathFileCreate);
			s.Close();
			filesystem.SetReplication(pathFileCreate, (short)1);
			// OP_SET_PERMISSIONS 7
			short permission = 0x1ff;
			filesystem.SetPermission(pathFileCreate, new FsPermission(permission));
			// OP_SET_OWNER 8
			filesystem.SetOwner(pathFileCreate, new string("newOwner"), null);
			// OP_CLOSE 9 see above
			// OP_SET_GENSTAMP 10 see above
			// OP_SET_NS_QUOTA 11 obsolete
			// OP_CLEAR_NS_QUOTA 12 obsolete
			// OP_TIMES 13
			long mtime = 1285195527000L;
			// Wed, 22 Sep 2010 22:45:27 GMT
			long atime = mtime;
			filesystem.SetTimes(pathFileCreate, mtime, atime);
			// OP_SET_QUOTA 14
			filesystem.SetQuota(pathDirectoryMkdir, 1000L, HdfsConstants.QuotaDontSet);
			// OP_SET_QUOTA_BY_STORAGETYPE
			filesystem.SetQuotaByStorageType(pathDirectoryMkdir, StorageType.Ssd, 888L);
			// OP_RENAME 15
			fc.Rename(pathFileCreate, pathFileMoved, Options.Rename.None);
			// OP_CONCAT_DELETE 16
			Path pathConcatTarget = new Path("/file_concat_target");
			Path[] pathConcatFiles = new Path[2];
			pathConcatFiles[0] = new Path("/file_concat_0");
			pathConcatFiles[1] = new Path("/file_concat_1");
			long length = blockSize * 3;
			// multiple of blocksize for concat
			short replication = 1;
			long seed = 1;
			DFSTestUtil.CreateFile(filesystem, pathConcatTarget, length, replication, seed);
			DFSTestUtil.CreateFile(filesystem, pathConcatFiles[0], length, replication, seed);
			DFSTestUtil.CreateFile(filesystem, pathConcatFiles[1], length, replication, seed);
			filesystem.Concat(pathConcatTarget, pathConcatFiles);
			// OP_TRUNCATE 46
			length = blockSize * 2;
			DFSTestUtil.CreateFile(filesystem, pathFileCreate, length, replication, seed);
			filesystem.Truncate(pathFileCreate, blockSize);
			// OP_SYMLINK 17
			Path pathSymlink = new Path("/file_symlink");
			fc.CreateSymlink(pathConcatTarget, pathSymlink, false);
			// OP_REASSIGN_LEASE 22
			string filePath = "/hard-lease-recovery-test";
			byte[] bytes = Sharpen.Runtime.GetBytesForString("foo-bar-baz");
			DFSClientAdapter.StopLeaseRenewer(filesystem);
			FSDataOutputStream leaseRecoveryPath = filesystem.Create(new Path(filePath));
			leaseRecoveryPath.Write(bytes);
			leaseRecoveryPath.Hflush();
			// Set the hard lease timeout to 1 second.
			cluster.SetLeasePeriod(60 * 1000, 1000, nnIndex);
			// wait for lease recovery to complete
			LocatedBlocks locatedBlocks;
			do
			{
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
				locatedBlocks = DFSClientAdapter.CallGetBlockLocations(cluster.GetNameNodeRpc(nnIndex
					), filePath, 0L, bytes.Length);
			}
			while (locatedBlocks.IsUnderConstruction());
			// OP_ADD_CACHE_POOL
			filesystem.AddCachePool(new CachePoolInfo("pool1"));
			// OP_MODIFY_CACHE_POOL
			filesystem.ModifyCachePool(new CachePoolInfo("pool1").SetLimit(99l));
			// OP_ADD_PATH_BASED_CACHE_DIRECTIVE
			long id = filesystem.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPath(new 
				Path("/path")).SetReplication((short)1).SetPool("pool1").Build(), EnumSet.Of(CacheFlag
				.Force));
			// OP_MODIFY_PATH_BASED_CACHE_DIRECTIVE
			filesystem.ModifyCacheDirective(new CacheDirectiveInfo.Builder().SetId(id).SetReplication
				((short)2).Build(), EnumSet.Of(CacheFlag.Force));
			// OP_REMOVE_PATH_BASED_CACHE_DIRECTIVE
			filesystem.RemoveCacheDirective(id);
			// OP_REMOVE_CACHE_POOL
			filesystem.RemoveCachePool("pool1");
			// OP_SET_ACL
			IList<AclEntry> aclEntryList = Lists.NewArrayList();
			aclEntryList.AddItem(new AclEntry.Builder().SetPermission(FsAction.ReadWrite).SetScope
				(AclEntryScope.Access).SetType(AclEntryType.User).Build());
			aclEntryList.AddItem(new AclEntry.Builder().SetName("user").SetPermission(FsAction
				.ReadWrite).SetScope(AclEntryScope.Access).SetType(AclEntryType.User).Build());
			aclEntryList.AddItem(new AclEntry.Builder().SetPermission(FsAction.Write).SetScope
				(AclEntryScope.Access).SetType(AclEntryType.Group).Build());
			aclEntryList.AddItem(new AclEntry.Builder().SetPermission(FsAction.None).SetScope
				(AclEntryScope.Access).SetType(AclEntryType.Other).Build());
			filesystem.SetAcl(pathConcatTarget, aclEntryList);
			// OP_SET_XATTR
			filesystem.SetXAttr(pathConcatTarget, "user.a1", new byte[] { unchecked((int)(0x31
				)), unchecked((int)(0x32)), unchecked((int)(0x33)) });
			filesystem.SetXAttr(pathConcatTarget, "user.a2", new byte[] { unchecked((int)(0x37
				)), unchecked((int)(0x38)), unchecked((int)(0x39)) });
			// OP_REMOVE_XATTR
			filesystem.RemoveXAttr(pathConcatTarget, "user.a2");
		}

		/// <exception cref="System.IO.IOException"/>
		public static void AbortStream(DFSOutputStream @out)
		{
			@out.Abort();
		}

		public static byte[] AsArray(ByteBuffer buf)
		{
			byte[] arr = new byte[buf.Remaining()];
			buf.Duplicate().Get(arr);
			return arr;
		}

		/// <summary>Blocks until cache usage hits the expected new value.</summary>
		/// <exception cref="System.Exception"/>
		public static long VerifyExpectedCacheUsage<_T0>(long expectedCacheUsed, long expectedBlocks
			, FsDatasetSpi<_T0> fsd)
			where _T0 : FsVolumeSpi
		{
			GenericTestUtils.WaitFor(new _Supplier_1367(fsd, expectedCacheUsed, expectedBlocks
				), 100, 60000);
			return expectedCacheUsed;
		}

		private sealed class _Supplier_1367 : Supplier<bool>
		{
			public _Supplier_1367(FsDatasetSpi<object> fsd, long expectedCacheUsed, long expectedBlocks
				)
			{
				this.fsd = fsd;
				this.expectedCacheUsed = expectedCacheUsed;
				this.expectedBlocks = expectedBlocks;
				this.tries = 0;
			}

			private int tries;

			public bool Get()
			{
				long curCacheUsed = fsd.GetCacheUsed();
				long curBlocks = fsd.GetNumBlocksCached();
				if ((curCacheUsed != expectedCacheUsed) || (curBlocks != expectedBlocks))
				{
					if (this.tries++ > 10)
					{
						DFSTestUtil.Log.Info("verifyExpectedCacheUsage: have " + curCacheUsed + "/" + expectedCacheUsed
							 + " bytes cached; " + curBlocks + "/" + expectedBlocks + " blocks cached. " + "memlock limit = "
							 + NativeIO.POSIX.GetCacheManipulator().GetMemlockLimit() + ".  Waiting...");
					}
					return false;
				}
				DFSTestUtil.Log.Info("verifyExpectedCacheUsage: got " + curCacheUsed + "/" + expectedCacheUsed
					 + " bytes cached; " + curBlocks + "/" + expectedBlocks + " blocks cached. " + "memlock limit = "
					 + NativeIO.POSIX.GetCacheManipulator().GetMemlockLimit());
				return true;
			}

			private readonly FsDatasetSpi<object> fsd;

			private readonly long expectedCacheUsed;

			private readonly long expectedBlocks;
		}

		/// <summary>Round a long value up to a multiple of a factor.</summary>
		/// <param name="val">The value.</param>
		/// <param name="factor">The factor to round up to.  Must be &gt; 1.</param>
		/// <returns>The rounded value.</returns>
		public static long RoundUpToMultiple(long val, int factor)
		{
			System.Diagnostics.Debug.Assert((factor > 1));
			long c = (val + factor - 1) / factor;
			return c * factor;
		}

		public static void CheckComponentsEquals(byte[][] expected, byte[][] actual)
		{
			NUnit.Framework.Assert.AreEqual("expected: " + DFSUtil.ByteArray2PathString(expected
				) + ", actual: " + DFSUtil.ByteArray2PathString(actual), expected.Length, actual
				.Length);
			int i = 0;
			foreach (byte[] e in expected)
			{
				byte[] actualComponent = actual[i++];
				NUnit.Framework.Assert.IsTrue("expected: " + DFSUtil.Bytes2String(e) + ", actual: "
					 + DFSUtil.Bytes2String(actualComponent), Arrays.Equals(e, actualComponent));
			}
		}

		/// <summary>
		/// A short-circuit test context which makes it easier to get a short-circuit
		/// configuration and set everything up.
		/// </summary>
		public class ShortCircuitTestContext : IDisposable
		{
			private readonly string testName;

			private readonly TemporarySocketDirectory sockDir;

			private bool closed = false;

			private readonly bool formerTcpReadsDisabled;

			public ShortCircuitTestContext(string testName)
			{
				this.testName = testName;
				this.sockDir = new TemporarySocketDirectory();
				DomainSocket.DisableBindPathValidation();
				formerTcpReadsDisabled = DFSInputStream.tcpReadsDisabledForTesting;
				Assume.AssumeTrue(DomainSocket.GetLoadingFailureReason() == null);
			}

			public virtual Configuration NewConfiguration()
			{
				Configuration conf = new Configuration();
				conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
				conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, new FilePath(sockDir.GetDir(), testName
					 + "._PORT.sock").GetAbsolutePath());
				return conf;
			}

			public virtual string GetTestName()
			{
				return testName;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				if (closed)
				{
					return;
				}
				closed = true;
				DFSInputStream.tcpReadsDisabledForTesting = formerTcpReadsDisabled;
				sockDir.Close();
			}
		}

		/// <summary>Verify that two files have the same contents.</summary>
		/// <param name="fs">The file system containing the two files.</param>
		/// <param name="p1">The path of the first file.</param>
		/// <param name="p2">The path of the second file.</param>
		/// <param name="len">The length of the two files.</param>
		/// <exception cref="System.IO.IOException"/>
		public static void VerifyFilesEqual(FileSystem fs, Path p1, Path p2, int len)
		{
			FSDataInputStream in1 = fs.Open(p1);
			FSDataInputStream in2 = fs.Open(p2);
			for (int i = 0; i < len; i++)
			{
				NUnit.Framework.Assert.AreEqual("Mismatch at byte " + i, in1.Read(), in2.Read());
			}
			in1.Close();
			in2.Close();
		}

		/// <summary>Verify that two files have different contents.</summary>
		/// <param name="fs">The file system containing the two files.</param>
		/// <param name="p1">The path of the first file.</param>
		/// <param name="p2">The path of the second file.</param>
		/// <param name="len">The length of the two files.</param>
		/// <exception cref="System.IO.IOException"/>
		public static void VerifyFilesNotEqual(FileSystem fs, Path p1, Path p2, int len)
		{
			FSDataInputStream in1 = fs.Open(p1);
			FSDataInputStream in2 = fs.Open(p2);
			try
			{
				for (int i = 0; i < len; i++)
				{
					if (in1.Read() != in2.Read())
					{
						return;
					}
				}
				NUnit.Framework.Assert.Fail("files are equal, but should not be");
			}
			finally
			{
				in1.Close();
				in2.Close();
			}
		}

		/// <summary>
		/// Helper function that verified blocks of a file are placed on the
		/// expected storage type.
		/// </summary>
		/// <param name="fs">The file system containing the the file.</param>
		/// <param name="client">The DFS client used to access the file</param>
		/// <param name="path">name to the file to verify</param>
		/// <param name="storageType">expected storage type</param>
		/// <returns>
		/// true if file exists and its blocks are located on the expected
		/// storage type.
		/// false otherwise.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static bool VerifyFileReplicasOnStorageType(FileSystem fs, DFSClient client
			, Path path, StorageType storageType)
		{
			if (!fs.Exists(path))
			{
				Log.Info("verifyFileReplicasOnStorageType: file " + path + "does not exist");
				return false;
			}
			long fileLength = client.GetFileInfo(path.ToString()).GetLen();
			LocatedBlocks locatedBlocks = client.GetLocatedBlocks(path.ToString(), 0, fileLength
				);
			foreach (LocatedBlock locatedBlock in locatedBlocks.GetLocatedBlocks())
			{
				if (locatedBlock.GetStorageTypes()[0] != storageType)
				{
					Log.Info("verifyFileReplicasOnStorageType: for file " + path + ". Expect blk" + locatedBlock
						 + " on Type: " + storageType + ". Actual Type: " + locatedBlock.GetStorageTypes
						()[0]);
					return false;
				}
			}
			return true;
		}

		/// <summary>Helper function to create a key in the Key Provider.</summary>
		/// <remarks>
		/// Helper function to create a key in the Key Provider. Defaults
		/// to the first indexed NameNode's Key Provider.
		/// </remarks>
		/// <param name="keyName">The name of the key to create</param>
		/// <param name="cluster">The cluster to create it in</param>
		/// <param name="conf">Configuration to use</param>
		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public static void CreateKey(string keyName, MiniDFSCluster cluster, Configuration
			 conf)
		{
			CreateKey(keyName, cluster, 0, conf);
		}

		/// <summary>Helper function to create a key in the Key Provider.</summary>
		/// <param name="keyName">The name of the key to create</param>
		/// <param name="cluster">The cluster to create it in</param>
		/// <param name="idx">The NameNode index</param>
		/// <param name="conf">Configuration to use</param>
		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public static void CreateKey(string keyName, MiniDFSCluster cluster, int idx, Configuration
			 conf)
		{
			NameNode nn = cluster.GetNameNode(idx);
			KeyProvider provider = nn.GetNamesystem().GetProvider();
			KeyProvider.Options options = KeyProvider.Options(conf);
			options.SetDescription(keyName);
			options.SetBitLength(128);
			provider.CreateKey(keyName, options);
			provider.Flush();
		}

		/// <returns>
		/// the node which is expected to run the recovery of the
		/// given block, which is known to be under construction inside the
		/// given NameNOde.
		/// </returns>
		public static DatanodeDescriptor GetExpectedPrimaryNode(NameNode nn, ExtendedBlock
			 blk)
		{
			BlockManager bm0 = nn.GetNamesystem().GetBlockManager();
			BlockInfoContiguous storedBlock = bm0.GetStoredBlock(blk.GetLocalBlock());
			NUnit.Framework.Assert.IsTrue("Block " + blk + " should be under construction, " 
				+ "got: " + storedBlock, storedBlock is BlockInfoContiguousUnderConstruction);
			BlockInfoContiguousUnderConstruction ucBlock = (BlockInfoContiguousUnderConstruction
				)storedBlock;
			// We expect that the replica with the most recent heart beat will be
			// the one to be in charge of the synchronization / recovery protocol.
			DatanodeStorageInfo[] storages = ucBlock.GetExpectedStorageLocations();
			DatanodeStorageInfo expectedPrimary = storages[0];
			long mostRecentLastUpdate = expectedPrimary.GetDatanodeDescriptor().GetLastUpdateMonotonic
				();
			for (int i = 1; i < storages.Length; i++)
			{
				long lastUpdate = storages[i].GetDatanodeDescriptor().GetLastUpdateMonotonic();
				if (lastUpdate > mostRecentLastUpdate)
				{
					expectedPrimary = storages[i];
					mostRecentLastUpdate = lastUpdate;
				}
			}
			return expectedPrimary.GetDatanodeDescriptor();
		}

		/// <exception cref="System.Exception"/>
		public static void ToolRun(Tool tool, string cmd, int retcode, string contain)
		{
			string[] cmds = StringUtils.Split(cmd, ' ');
			System.Console.Out.Flush();
			System.Console.Error.Flush();
			TextWriter origOut = System.Console.Out;
			TextWriter origErr = System.Console.Error;
			string output = null;
			int ret = 0;
			try
			{
				ByteArrayOutputStream bs = new ByteArrayOutputStream(1024);
				TextWriter @out = new TextWriter(bs);
				Runtime.SetOut(@out);
				Runtime.SetErr(@out);
				ret = tool.Run(cmds);
				System.Console.Out.Flush();
				System.Console.Error.Flush();
				@out.Close();
				output = bs.ToString();
			}
			finally
			{
				Runtime.SetOut(origOut);
				Runtime.SetErr(origErr);
			}
			System.Console.Out.WriteLine("Output for command: " + cmd + " retcode: " + ret);
			if (output != null)
			{
				System.Console.Out.WriteLine(output);
			}
			NUnit.Framework.Assert.AreEqual(retcode, ret);
			if (contain != null)
			{
				NUnit.Framework.Assert.IsTrue("The real output is: " + output + ".\n It should contain: "
					 + contain, output.Contains(contain));
			}
		}

		/// <exception cref="System.Exception"/>
		public static void FsShellRun(string cmd, int retcode, string contain, Configuration
			 conf)
		{
			FsShell shell = new FsShell(new Configuration(conf));
			ToolRun(shell, cmd, retcode, contain);
		}

		/// <exception cref="System.Exception"/>
		public static void DFSAdminRun(string cmd, int retcode, string contain, Configuration
			 conf)
		{
			DFSAdmin admin = new DFSAdmin(new Configuration(conf));
			ToolRun(admin, cmd, retcode, contain);
		}

		/// <exception cref="System.Exception"/>
		public static void FsShellRun(string cmd, Configuration conf)
		{
			FsShellRun(cmd, 0, null, conf);
		}

		/// <exception cref="Sharpen.NoSuchFieldException"/>
		/// <exception cref="System.MemberAccessException"/>
		public static void AddDataNodeLayoutVersion(int lv, string description)
		{
			Preconditions.CheckState(lv < DataNodeLayoutVersion.CurrentLayoutVersion);
			// Override {@link DataNodeLayoutVersion#CURRENT_LAYOUT_VERSION} via reflection.
			FieldInfo modifiersField = Sharpen.Runtime.GetDeclaredField(typeof(FieldInfo), "modifiers"
				);
			FieldInfo field = typeof(DataNodeLayoutVersion).GetField("CURRENT_LAYOUT_VERSION"
				);
			modifiersField.SetInt(field, field.GetModifiers() & ~Modifier.Final);
			field.SetInt(null, lv);
			// Override {@link HdfsConstants#DATANODE_LAYOUT_VERSION}
			field = typeof(HdfsConstants).GetField("DATANODE_LAYOUT_VERSION");
			modifiersField.SetInt(field, field.GetModifiers() & ~Modifier.Final);
			field.SetInt(null, lv);
			// Inject the feature into the FEATURES map.
			LayoutVersion.FeatureInfo featureInfo = new LayoutVersion.FeatureInfo(lv, lv + 1, 
				description, false);
			LayoutVersion.LayoutFeature feature = new _LayoutFeature_1680(featureInfo);
			// Update the FEATURES map with the new layout version.
			LayoutVersion.UpdateMap(DataNodeLayoutVersion.Features, new LayoutVersion.LayoutFeature
				[] { feature });
		}

		private sealed class _LayoutFeature_1680 : LayoutVersion.LayoutFeature
		{
			public _LayoutFeature_1680(LayoutVersion.FeatureInfo featureInfo)
			{
				this.featureInfo = featureInfo;
			}

			public LayoutVersion.FeatureInfo GetInfo()
			{
				return featureInfo;
			}

			private readonly LayoutVersion.FeatureInfo featureInfo;
		}

		/// <summary>
		/// Wait for datanode to reach alive or dead state for waitTime given in
		/// milliseconds.
		/// </summary>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		public static void WaitForDatanodeState(MiniDFSCluster cluster, string nodeID, bool
			 alive, int waitTime)
		{
			GenericTestUtils.WaitFor(new _Supplier_1700(cluster, nodeID, alive), 100, waitTime
				);
		}

		private sealed class _Supplier_1700 : Supplier<bool>
		{
			public _Supplier_1700(MiniDFSCluster cluster, string nodeID, bool alive)
			{
				this.cluster = cluster;
				this.nodeID = nodeID;
				this.alive = alive;
			}

			public bool Get()
			{
				FSNamesystem namesystem = cluster.GetNamesystem();
				DatanodeDescriptor dd = BlockManagerTestUtil.GetDatanode(namesystem, nodeID);
				return (dd.isAlive == alive);
			}

			private readonly MiniDFSCluster cluster;

			private readonly string nodeID;

			private readonly bool alive;
		}

		public static void SetNameNodeLogLevel(Level level)
		{
			GenericTestUtils.SetLogLevel(FSNamesystem.Log, level);
			GenericTestUtils.SetLogLevel(BlockManager.Log, level);
			GenericTestUtils.SetLogLevel(LeaseManager.Log, level);
			GenericTestUtils.SetLogLevel(NameNode.Log, level);
			GenericTestUtils.SetLogLevel(NameNode.stateChangeLog, level);
			GenericTestUtils.SetLogLevel(NameNode.blockStateChangeLog, level);
		}

		/// <summary>Change the length of a block at datanode dnIndex</summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool ChangeReplicaLength(MiniDFSCluster cluster, ExtendedBlock blk, 
			int dnIndex, int lenDelta)
		{
			FilePath blockFile = cluster.GetBlockFile(dnIndex, blk);
			if (blockFile != null && blockFile.Exists())
			{
				RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
				raFile.SetLength(raFile.Length() + lenDelta);
				raFile.Close();
				return true;
			}
			Log.Info("failed to change length of block " + blk);
			return false;
		}

		/// <summary>Set the datanode dead</summary>
		public static void SetDatanodeDead(DatanodeInfo dn)
		{
			dn.SetLastUpdate(0);
			dn.SetLastUpdateMonotonic(0);
		}

		/// <summary>Update lastUpdate and lastUpdateMonotonic with some offset.</summary>
		public static void ResetLastUpdatesWithOffset(DatanodeInfo dn, long offset)
		{
			dn.SetLastUpdate(Time.Now() + offset);
			dn.SetLastUpdateMonotonic(Time.MonotonicNow() + offset);
		}

		public static StorageReceivedDeletedBlocks[] MakeReportForReceivedBlock(Org.Apache.Hadoop.Hdfs.Protocol.Block
			 block, ReceivedDeletedBlockInfo.BlockStatus blockStatus, DatanodeStorage storage
			)
		{
			ReceivedDeletedBlockInfo[] receivedBlocks = new ReceivedDeletedBlockInfo[1];
			receivedBlocks[0] = new ReceivedDeletedBlockInfo(block, blockStatus, null);
			StorageReceivedDeletedBlocks[] reports = new StorageReceivedDeletedBlocks[1];
			reports[0] = new StorageReceivedDeletedBlocks(storage, receivedBlocks);
			return reports;
		}

		/// <summary>Adds a block to a file.</summary>
		/// <remarks>
		/// Adds a block to a file.
		/// This method only manipulates NameNode
		/// states of the file and the block without injecting data to DataNode.
		/// It does mimic block reports.
		/// You should disable periodical heartbeat before use this.
		/// </remarks>
		/// <param name="dataNodes">List DataNodes to host the block</param>
		/// <param name="previous">Previous block in the file</param>
		/// <param name="len">block size</param>
		/// <returns>The added block</returns>
		/// <exception cref="System.Exception"/>
		public static Org.Apache.Hadoop.Hdfs.Protocol.Block AddBlockToFile(IList<DataNode
			> dataNodes, DistributedFileSystem fs, FSNamesystem ns, string file, INodeFile fileNode
			, string clientName, ExtendedBlock previous, int len)
		{
			fs.GetClient().namenode.AddBlock(file, clientName, previous, null, fileNode.GetId
				(), null);
			BlockInfoContiguous lastBlock = fileNode.GetLastBlock();
			int groupSize = fileNode.GetBlockReplication();
			System.Diagnostics.Debug.Assert(dataNodes.Count >= groupSize);
			// 1. RECEIVING_BLOCK IBR
			for (int i = 0; i < groupSize; i++)
			{
				DataNode dn = dataNodes[i];
				Org.Apache.Hadoop.Hdfs.Protocol.Block block = new Org.Apache.Hadoop.Hdfs.Protocol.Block
					(lastBlock.GetBlockId() + i, 0, lastBlock.GetGenerationStamp());
				DatanodeStorage storage = new DatanodeStorage(UUID.RandomUUID().ToString());
				StorageReceivedDeletedBlocks[] reports = DFSTestUtil.MakeReportForReceivedBlock(block
					, ReceivedDeletedBlockInfo.BlockStatus.ReceivingBlock, storage);
				foreach (StorageReceivedDeletedBlocks report in reports)
				{
					ns.ProcessIncrementalBlockReport(dn.GetDatanodeId(), report);
				}
			}
			// 2. RECEIVED_BLOCK IBR
			for (int i_1 = 0; i_1 < groupSize; i_1++)
			{
				DataNode dn = dataNodes[i_1];
				Org.Apache.Hadoop.Hdfs.Protocol.Block block = new Org.Apache.Hadoop.Hdfs.Protocol.Block
					(lastBlock.GetBlockId() + i_1, len, lastBlock.GetGenerationStamp());
				DatanodeStorage storage = new DatanodeStorage(UUID.RandomUUID().ToString());
				StorageReceivedDeletedBlocks[] reports = DFSTestUtil.MakeReportForReceivedBlock(block
					, ReceivedDeletedBlockInfo.BlockStatus.ReceivedBlock, storage);
				foreach (StorageReceivedDeletedBlocks report in reports)
				{
					ns.ProcessIncrementalBlockReport(dn.GetDatanodeId(), report);
				}
			}
			lastBlock.SetNumBytes(len);
			return lastBlock;
		}
	}
}
