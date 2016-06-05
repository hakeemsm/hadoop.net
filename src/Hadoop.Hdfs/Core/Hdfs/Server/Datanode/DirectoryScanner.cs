using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Periodically scans the data directories for block and block metadata files.
	/// 	</summary>
	/// <remarks>
	/// Periodically scans the data directories for block and block metadata files.
	/// Reconciles the differences with block information maintained in the dataset.
	/// </remarks>
	public class DirectoryScanner : Runnable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.DirectoryScanner
			));

		private readonly FsDatasetSpi<object> dataset;

		private readonly ExecutorService reportCompileThreadPool;

		private readonly ScheduledExecutorService masterThread;

		private readonly long scanPeriodMsecs;

		private volatile bool shouldRun = false;

		private bool retainDiffs = false;

		private readonly DataNode datanode;

		internal readonly DirectoryScanner.ScanInfoPerBlockPool diffs = new DirectoryScanner.ScanInfoPerBlockPool
			();

		internal readonly IDictionary<string, DirectoryScanner.Stats> stats = new Dictionary
			<string, DirectoryScanner.Stats>();

		/// <summary>Allow retaining diffs for unit test and analysis</summary>
		/// <param name="b">- defaults to false (off)</param>
		internal virtual void SetRetainDiffs(bool b)
		{
			retainDiffs = b;
		}

		/// <summary>Stats tracked for reporting and testing, per blockpool</summary>
		internal class Stats
		{
			internal readonly string bpid;

			internal long totalBlocks = 0;

			internal long missingMetaFile = 0;

			internal long missingBlockFile = 0;

			internal long missingMemoryBlocks = 0;

			internal long mismatchBlocks = 0;

			internal long duplicateBlocks = 0;

			public Stats(string bpid)
			{
				this.bpid = bpid;
			}

			public override string ToString()
			{
				return "BlockPool " + bpid + " Total blocks: " + totalBlocks + ", missing metadata files:"
					 + missingMetaFile + ", missing block files:" + missingBlockFile + ", missing blocks in memory:"
					 + missingMemoryBlocks + ", mismatched blocks:" + mismatchBlocks;
			}
		}

		[System.Serializable]
		internal class ScanInfoPerBlockPool : Dictionary<string, List<DirectoryScanner.ScanInfo
			>>
		{
			private const long serialVersionUID = 1L;

			internal ScanInfoPerBlockPool()
				: base()
			{
			}

			internal ScanInfoPerBlockPool(int sz)
				: base(sz)
			{
			}

			/// <summary>
			/// Merges
			/// <paramref name="that"/>
			/// ScanInfoPerBlockPool into this one
			/// </summary>
			public virtual void AddAll(DirectoryScanner.ScanInfoPerBlockPool that)
			{
				if (that == null)
				{
					return;
				}
				foreach (KeyValuePair<string, List<DirectoryScanner.ScanInfo>> entry in that)
				{
					string bpid = entry.Key;
					List<DirectoryScanner.ScanInfo> list = entry.Value;
					if (this.Contains(bpid))
					{
						//merge that per-bpid linked list with this one
						Sharpen.Collections.AddAll(this[bpid], list);
					}
					else
					{
						//add that new bpid and its linked list to this
						this[bpid] = list;
					}
				}
			}

			/// <summary>
			/// Convert all the LinkedList values in this ScanInfoPerBlockPool map
			/// into sorted arrays, and return a new map of these arrays per blockpool
			/// </summary>
			/// <returns>a map of ScanInfo arrays per blockpool</returns>
			public virtual IDictionary<string, DirectoryScanner.ScanInfo[]> ToSortedArrays()
			{
				IDictionary<string, DirectoryScanner.ScanInfo[]> result = new Dictionary<string, 
					DirectoryScanner.ScanInfo[]>(this.Count);
				foreach (KeyValuePair<string, List<DirectoryScanner.ScanInfo>> entry in this)
				{
					string bpid = entry.Key;
					List<DirectoryScanner.ScanInfo> list = entry.Value;
					// convert list to array
					DirectoryScanner.ScanInfo[] record = Sharpen.Collections.ToArray(list, new DirectoryScanner.ScanInfo
						[list.Count]);
					// Sort array based on blockId
					Arrays.Sort(record);
					result[bpid] = record;
				}
				return result;
			}
		}

		/// <summary>
		/// Tracks the files and other information related to a block on the disk
		/// Missing file is indicated by setting the corresponding member
		/// to null.
		/// </summary>
		/// <remarks>
		/// Tracks the files and other information related to a block on the disk
		/// Missing file is indicated by setting the corresponding member
		/// to null.
		/// Because millions of these structures may be created, we try to save
		/// memory here.  So instead of storing full paths, we store path suffixes.
		/// The block file, if it exists, will have a path like this:
		/// <volume_base_path>/<block_path>
		/// So we don't need to store the volume path, since we already know what the
		/// volume is.
		/// The metadata file, if it exists, will have a path like this:
		/// <volume_base_path>/<block_path>_<genstamp>.meta
		/// So if we have a block file, there isn't any need to store the block path
		/// again.
		/// The accessor functions take care of these manipulations.
		/// </remarks>
		internal class ScanInfo : Comparable<DirectoryScanner.ScanInfo>
		{
			private readonly long blockId;

			/// <summary>The block file path, relative to the volume's base directory.</summary>
			/// <remarks>
			/// The block file path, relative to the volume's base directory.
			/// If there was no block file found, this may be null. If 'vol'
			/// is null, then this is the full path of the block file.
			/// </remarks>
			private readonly string blockSuffix;

			/// <summary>The suffix of the meta file path relative to the block file.</summary>
			/// <remarks>
			/// The suffix of the meta file path relative to the block file.
			/// If blockSuffix is null, then this will be the entire path relative
			/// to the volume base directory, or an absolute path if vol is also
			/// null.
			/// </remarks>
			private readonly string metaSuffix;

			private readonly FsVolumeSpi volume;

			/// <summary>Get the file's length in async block scan</summary>
			private readonly long blockFileLength;

			private static readonly Sharpen.Pattern CondensedPathRegex = Sharpen.Pattern.Compile
				("(?<!^)(\\\\|/){2,}");

			private static readonly string QuotedFileSeparator = Matcher.QuoteReplacement(FilePath
				.separator);

			/// <summary>Get the most condensed version of the path.</summary>
			/// <remarks>
			/// Get the most condensed version of the path.
			/// For example, the condensed version of /foo//bar is /foo/bar
			/// Unlike
			/// <see cref="Sharpen.FilePath.GetCanonicalPath()"/>
			/// , this will never perform I/O
			/// on the filesystem.
			/// </remarks>
			private static string GetCondensedPath(string path)
			{
				return CondensedPathRegex.Matcher(path).ReplaceAll(QuotedFileSeparator);
			}

			/// <summary>Get a path suffix.</summary>
			/// <param name="f">The file to get the suffix for.</param>
			/// <param name="prefix">The prefix we're stripping off.</param>
			/// <returns>A suffix such that prefix + suffix = path to f</returns>
			private static string GetSuffix(FilePath f, string prefix)
			{
				string fullPath = GetCondensedPath(f.GetAbsolutePath());
				if (fullPath.StartsWith(prefix))
				{
					return Sharpen.Runtime.Substring(fullPath, prefix.Length);
				}
				throw new RuntimeException(prefix + " is not a prefix of " + fullPath);
			}

			internal ScanInfo(long blockId, FilePath blockFile, FilePath metaFile, FsVolumeSpi
				 vol)
			{
				this.blockId = blockId;
				string condensedVolPath = vol == null ? null : GetCondensedPath(vol.GetBasePath()
					);
				this.blockSuffix = blockFile == null ? null : GetSuffix(blockFile, condensedVolPath
					);
				this.blockFileLength = (blockFile != null) ? blockFile.Length() : 0;
				if (metaFile == null)
				{
					this.metaSuffix = null;
				}
				else
				{
					if (blockFile == null)
					{
						this.metaSuffix = GetSuffix(metaFile, condensedVolPath);
					}
					else
					{
						this.metaSuffix = GetSuffix(metaFile, condensedVolPath + blockSuffix);
					}
				}
				this.volume = vol;
			}

			internal virtual FilePath GetBlockFile()
			{
				return (blockSuffix == null) ? null : new FilePath(volume.GetBasePath(), blockSuffix
					);
			}

			internal virtual long GetBlockFileLength()
			{
				return blockFileLength;
			}

			internal virtual FilePath GetMetaFile()
			{
				if (metaSuffix == null)
				{
					return null;
				}
				else
				{
					if (blockSuffix == null)
					{
						return new FilePath(volume.GetBasePath(), metaSuffix);
					}
					else
					{
						return new FilePath(volume.GetBasePath(), blockSuffix + metaSuffix);
					}
				}
			}

			internal virtual long GetBlockId()
			{
				return blockId;
			}

			internal virtual FsVolumeSpi GetVolume()
			{
				return volume;
			}

			public virtual int CompareTo(DirectoryScanner.ScanInfo b)
			{
				// Comparable
				if (blockId < b.blockId)
				{
					return -1;
				}
				else
				{
					if (blockId == b.blockId)
					{
						return 0;
					}
					else
					{
						return 1;
					}
				}
			}

			public override bool Equals(object o)
			{
				// Object
				if (this == o)
				{
					return true;
				}
				if (!(o is DirectoryScanner.ScanInfo))
				{
					return false;
				}
				return blockId == ((DirectoryScanner.ScanInfo)o).blockId;
			}

			public override int GetHashCode()
			{
				// Object
				return (int)(blockId ^ ((long)(((ulong)blockId) >> 32)));
			}

			public virtual long GetGenStamp()
			{
				return metaSuffix != null ? Block.GetGenerationStamp(GetMetaFile().GetName()) : GenerationStamp
					.GrandfatherGenerationStamp;
			}
		}

		internal DirectoryScanner(DataNode datanode, FsDatasetSpi<object> dataset, Configuration
			 conf)
		{
			this.datanode = datanode;
			this.dataset = dataset;
			int interval = conf.GetInt(DFSConfigKeys.DfsDatanodeDirectoryscanIntervalKey, DFSConfigKeys
				.DfsDatanodeDirectoryscanIntervalDefault);
			scanPeriodMsecs = interval * 1000L;
			//msec
			int threads = conf.GetInt(DFSConfigKeys.DfsDatanodeDirectoryscanThreadsKey, DFSConfigKeys
				.DfsDatanodeDirectoryscanThreadsDefault);
			reportCompileThreadPool = Executors.NewFixedThreadPool(threads, new Daemon.DaemonFactory
				());
			masterThread = new ScheduledThreadPoolExecutor(1, new Daemon.DaemonFactory());
		}

		internal virtual void Start()
		{
			shouldRun = true;
			long offset = DFSUtil.GetRandom().Next((int)(scanPeriodMsecs / 1000L)) * 1000L;
			//msec
			long firstScanTime = Time.Now() + offset;
			Log.Info("Periodic Directory Tree Verification scan starting at " + firstScanTime
				 + " with interval " + scanPeriodMsecs);
			masterThread.ScheduleAtFixedRate(this, offset, scanPeriodMsecs, TimeUnit.Milliseconds
				);
		}

		// for unit test
		internal virtual bool GetRunStatus()
		{
			return shouldRun;
		}

		private void Clear()
		{
			diffs.Clear();
			stats.Clear();
		}

		/// <summary>
		/// Main program loop for DirectoryScanner
		/// Runs "reconcile()" periodically under the masterThread.
		/// </summary>
		public virtual void Run()
		{
			try
			{
				if (!shouldRun)
				{
					//shutdown has been activated
					Log.Warn("this cycle terminating immediately because 'shouldRun' has been deactivated"
						);
					return;
				}
				//We're are okay to run - do it
				Reconcile();
			}
			catch (Exception e)
			{
				//Log and continue - allows Executor to run again next cycle
				Log.Error("Exception during DirectoryScanner execution - will continue next cycle"
					, e);
			}
			catch (Error er)
			{
				//Non-recoverable error - re-throw after logging the problem
				Log.Error("System Error during DirectoryScanner execution - permanently terminating periodic scanner"
					, er);
				throw;
			}
		}

		internal virtual void Shutdown()
		{
			if (!shouldRun)
			{
				Log.Warn("DirectoryScanner: shutdown has been called, but periodic scanner not started"
					);
			}
			else
			{
				Log.Warn("DirectoryScanner: shutdown has been called");
			}
			shouldRun = false;
			if (masterThread != null)
			{
				masterThread.Shutdown();
			}
			if (reportCompileThreadPool != null)
			{
				reportCompileThreadPool.Shutdown();
			}
			if (masterThread != null)
			{
				try
				{
					masterThread.AwaitTermination(1, TimeUnit.Minutes);
				}
				catch (Exception e)
				{
					Log.Error("interrupted while waiting for masterThread to " + "terminate", e);
				}
			}
			if (reportCompileThreadPool != null)
			{
				try
				{
					reportCompileThreadPool.AwaitTermination(1, TimeUnit.Minutes);
				}
				catch (Exception e)
				{
					Log.Error("interrupted while waiting for reportCompileThreadPool to " + "terminate"
						, e);
				}
			}
			if (!retainDiffs)
			{
				Clear();
			}
		}

		/// <summary>Reconcile differences between disk and in-memory blocks</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void Reconcile()
		{
			Scan();
			foreach (KeyValuePair<string, List<DirectoryScanner.ScanInfo>> entry in diffs)
			{
				string bpid = entry.Key;
				List<DirectoryScanner.ScanInfo> diff = entry.Value;
				foreach (DirectoryScanner.ScanInfo info in diff)
				{
					dataset.CheckAndUpdate(bpid, info.GetBlockId(), info.GetBlockFile(), info.GetMetaFile
						(), info.GetVolume());
				}
			}
			if (!retainDiffs)
			{
				Clear();
			}
		}

		/// <summary>
		/// Scan for the differences between disk and in-memory blocks
		/// Scan only the "finalized blocks" lists of both disk and memory.
		/// </summary>
		internal virtual void Scan()
		{
			Clear();
			IDictionary<string, DirectoryScanner.ScanInfo[]> diskReport = GetDiskReport();
			// Hold FSDataset lock to prevent further changes to the block map
			lock (dataset)
			{
				foreach (KeyValuePair<string, DirectoryScanner.ScanInfo[]> entry in diskReport)
				{
					string bpid = entry.Key;
					DirectoryScanner.ScanInfo[] blockpoolReport = entry.Value;
					DirectoryScanner.Stats statsRecord = new DirectoryScanner.Stats(bpid);
					stats[bpid] = statsRecord;
					List<DirectoryScanner.ScanInfo> diffRecord = new List<DirectoryScanner.ScanInfo>(
						);
					diffs[bpid] = diffRecord;
					statsRecord.totalBlocks = blockpoolReport.Length;
					IList<FinalizedReplica> bl = dataset.GetFinalizedBlocks(bpid);
					FinalizedReplica[] memReport = Sharpen.Collections.ToArray(bl, new FinalizedReplica
						[bl.Count]);
					Arrays.Sort(memReport);
					// Sort based on blockId
					int d = 0;
					// index for blockpoolReport
					int m = 0;
					// index for memReprot
					while (m < memReport.Length && d < blockpoolReport.Length)
					{
						FinalizedReplica memBlock = memReport[m];
						DirectoryScanner.ScanInfo info = blockpoolReport[d];
						if (info.GetBlockId() < memBlock.GetBlockId())
						{
							if (!dataset.IsDeletingBlock(bpid, info.GetBlockId()))
							{
								// Block is missing in memory
								statsRecord.missingMemoryBlocks++;
								AddDifference(diffRecord, statsRecord, info);
							}
							d++;
							continue;
						}
						if (info.GetBlockId() > memBlock.GetBlockId())
						{
							// Block is missing on the disk
							AddDifference(diffRecord, statsRecord, memBlock.GetBlockId(), info.GetVolume());
							m++;
							continue;
						}
						// Block file and/or metadata file exists on the disk
						// Block exists in memory
						if (info.GetBlockFile() == null)
						{
							// Block metadata file exits and block file is missing
							AddDifference(diffRecord, statsRecord, info);
						}
						else
						{
							if (info.GetGenStamp() != memBlock.GetGenerationStamp() || info.GetBlockFileLength
								() != memBlock.GetNumBytes())
							{
								// Block metadata file is missing or has wrong generation stamp,
								// or block file length is different than expected
								statsRecord.mismatchBlocks++;
								AddDifference(diffRecord, statsRecord, info);
							}
							else
							{
								if (info.GetBlockFile().CompareTo(memBlock.GetBlockFile()) != 0)
								{
									// volumeMap record and on-disk files don't match.
									statsRecord.duplicateBlocks++;
									AddDifference(diffRecord, statsRecord, info);
								}
							}
						}
						d++;
						if (d < blockpoolReport.Length)
						{
							// There may be multiple on-disk records for the same block, don't increment
							// the memory record pointer if so.
							DirectoryScanner.ScanInfo nextInfo = blockpoolReport[Math.Min(d, blockpoolReport.
								Length - 1)];
							if (nextInfo.GetBlockId() != info.blockId)
							{
								++m;
							}
						}
						else
						{
							++m;
						}
					}
					while (m < memReport.Length)
					{
						FinalizedReplica current = memReport[m++];
						AddDifference(diffRecord, statsRecord, current.GetBlockId(), current.GetVolume());
					}
					while (d < blockpoolReport.Length)
					{
						if (!dataset.IsDeletingBlock(bpid, blockpoolReport[d].GetBlockId()))
						{
							statsRecord.missingMemoryBlocks++;
							AddDifference(diffRecord, statsRecord, blockpoolReport[d]);
						}
						d++;
					}
					Log.Info(statsRecord.ToString());
				}
			}
		}

		//end for
		//end synchronized
		/// <summary>Block is found on the disk.</summary>
		/// <remarks>
		/// Block is found on the disk. In-memory block is missing or does not match
		/// the block on the disk
		/// </remarks>
		private void AddDifference(List<DirectoryScanner.ScanInfo> diffRecord, DirectoryScanner.Stats
			 statsRecord, DirectoryScanner.ScanInfo info)
		{
			statsRecord.missingMetaFile += info.GetMetaFile() == null ? 1 : 0;
			statsRecord.missingBlockFile += info.GetBlockFile() == null ? 1 : 0;
			diffRecord.AddItem(info);
		}

		/// <summary>Block is not found on the disk</summary>
		private void AddDifference(List<DirectoryScanner.ScanInfo> diffRecord, DirectoryScanner.Stats
			 statsRecord, long blockId, FsVolumeSpi vol)
		{
			statsRecord.missingBlockFile++;
			statsRecord.missingMetaFile++;
			diffRecord.AddItem(new DirectoryScanner.ScanInfo(blockId, null, null, vol));
		}

		/// <summary>Is the given volume still valid in the dataset?</summary>
		private static bool IsValid<_T0>(FsDatasetSpi<_T0> dataset, FsVolumeSpi volume)
			where _T0 : FsVolumeSpi
		{
			foreach (FsVolumeSpi vol in dataset.GetVolumes())
			{
				if (vol == volume)
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>Get lists of blocks on the disk sorted by blockId, per blockpool</summary>
		private IDictionary<string, DirectoryScanner.ScanInfo[]> GetDiskReport()
		{
			// First get list of data directories
			IList<FsVolumeSpi> volumes = dataset.GetVolumes();
			// Use an array since the threads may return out of order and
			// compilersInProgress#keySet may return out of order as well.
			DirectoryScanner.ScanInfoPerBlockPool[] dirReports = new DirectoryScanner.ScanInfoPerBlockPool
				[volumes.Count];
			IDictionary<int, Future<DirectoryScanner.ScanInfoPerBlockPool>> compilersInProgress
				 = new Dictionary<int, Future<DirectoryScanner.ScanInfoPerBlockPool>>();
			for (int i = 0; i < volumes.Count; i++)
			{
				if (IsValid(dataset, volumes[i]))
				{
					DirectoryScanner.ReportCompiler reportCompiler = new DirectoryScanner.ReportCompiler
						(datanode, volumes[i]);
					Future<DirectoryScanner.ScanInfoPerBlockPool> result = reportCompileThreadPool.Submit
						(reportCompiler);
					compilersInProgress[i] = result;
				}
			}
			foreach (KeyValuePair<int, Future<DirectoryScanner.ScanInfoPerBlockPool>> report in 
				compilersInProgress)
			{
				try
				{
					dirReports[report.Key] = report.Value.Get();
				}
				catch (Exception ex)
				{
					Log.Error("Error compiling report", ex);
					// Propagate ex to DataBlockScanner to deal with
					throw new RuntimeException(ex);
				}
			}
			// Compile consolidated report for all the volumes
			DirectoryScanner.ScanInfoPerBlockPool list = new DirectoryScanner.ScanInfoPerBlockPool
				();
			for (int i_1 = 0; i_1 < volumes.Count; i_1++)
			{
				if (IsValid(dataset, volumes[i_1]))
				{
					// volume is still valid
					list.AddAll(dirReports[i_1]);
				}
			}
			return list.ToSortedArrays();
		}

		private static bool IsBlockMetaFile(string blockId, string metaFile)
		{
			return metaFile.StartsWith(blockId) && metaFile.EndsWith(Block.MetadataExtension);
		}

		private class ReportCompiler : Callable<DirectoryScanner.ScanInfoPerBlockPool>
		{
			private readonly FsVolumeSpi volume;

			private readonly DataNode datanode;

			public ReportCompiler(DataNode datanode, FsVolumeSpi volume)
			{
				this.datanode = datanode;
				this.volume = volume;
			}

			/// <exception cref="System.Exception"/>
			public virtual DirectoryScanner.ScanInfoPerBlockPool Call()
			{
				string[] bpList = volume.GetBlockPoolList();
				DirectoryScanner.ScanInfoPerBlockPool result = new DirectoryScanner.ScanInfoPerBlockPool
					(bpList.Length);
				foreach (string bpid in bpList)
				{
					List<DirectoryScanner.ScanInfo> report = new List<DirectoryScanner.ScanInfo>();
					FilePath bpFinalizedDir = volume.GetFinalizedDir(bpid);
					result[bpid] = CompileReport(volume, bpFinalizedDir, bpFinalizedDir, report);
				}
				return result;
			}

			/// <summary>
			/// Compile list
			/// <see cref="ScanInfo"/>
			/// for the blocks in the directory <dir>
			/// </summary>
			private List<DirectoryScanner.ScanInfo> CompileReport(FsVolumeSpi vol, FilePath bpFinalizedDir
				, FilePath dir, List<DirectoryScanner.ScanInfo> report)
			{
				FilePath[] files;
				try
				{
					files = FileUtil.ListFiles(dir);
				}
				catch (IOException ioe)
				{
					Log.Warn("Exception occured while compiling report: ", ioe);
					// Initiate a check on disk failure.
					datanode.CheckDiskErrorAsync();
					// Ignore this directory and proceed.
					return report;
				}
				Arrays.Sort(files);
				/*
				* Assumption: In the sorted list of files block file appears immediately
				* before block metadata file. This is true for the current naming
				* convention for block file blk_<blockid> and meta file
				* blk_<blockid>_<genstamp>.meta
				*/
				for (int i = 0; i < files.Length; i++)
				{
					if (files[i].IsDirectory())
					{
						CompileReport(vol, bpFinalizedDir, files[i], report);
						continue;
					}
					if (!Block.IsBlockFilename(files[i]))
					{
						if (IsBlockMetaFile(Block.BlockFilePrefix, files[i].GetName()))
						{
							long blockId = Block.GetBlockId(files[i].GetName());
							VerifyFileLocation(files[i].GetParentFile(), bpFinalizedDir, blockId);
							report.AddItem(new DirectoryScanner.ScanInfo(blockId, null, files[i], vol));
						}
						continue;
					}
					FilePath blockFile = files[i];
					long blockId_1 = Block.Filename2id(blockFile.GetName());
					FilePath metaFile = null;
					// Skip all the files that start with block name until
					// getting to the metafile for the block
					while (i + 1 < files.Length && files[i + 1].IsFile() && files[i + 1].GetName().StartsWith
						(blockFile.GetName()))
					{
						i++;
						if (IsBlockMetaFile(blockFile.GetName(), files[i].GetName()))
						{
							metaFile = files[i];
							break;
						}
					}
					VerifyFileLocation(blockFile.GetParentFile(), bpFinalizedDir, blockId_1);
					report.AddItem(new DirectoryScanner.ScanInfo(blockId_1, blockFile, metaFile, vol)
						);
				}
				return report;
			}

			/// <summary>
			/// Verify whether the actual directory location of block file has the
			/// expected directory path computed using its block ID.
			/// </summary>
			private void VerifyFileLocation(FilePath actualBlockDir, FilePath bpFinalizedDir, 
				long blockId)
			{
				FilePath blockDir = DatanodeUtil.IdToBlockDir(bpFinalizedDir, blockId);
				if (actualBlockDir.CompareTo(blockDir) != 0)
				{
					Log.Warn("Block: " + blockId + " has to be upgraded to block ID-based layout");
				}
			}
		}
	}
}
