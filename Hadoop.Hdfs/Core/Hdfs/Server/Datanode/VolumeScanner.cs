using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Cache;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>VolumeScanner scans a single volume.</summary>
	/// <remarks>
	/// VolumeScanner scans a single volume.  Each VolumeScanner has its own thread.<p/>
	/// They are all managed by the DataNode's BlockScanner.
	/// </remarks>
	public class VolumeScanner : Sharpen.Thread
	{
		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.VolumeScanner
			));

		/// <summary>Number of seconds in a minute.</summary>
		private const int SecondsPerMinute = 60;

		/// <summary>Number of minutes in an hour.</summary>
		private const int MinutesPerHour = 60;

		/// <summary>Name of the block iterator used by this scanner.</summary>
		private const string BlockIteratorName = "scanner";

		/// <summary>The configuration.</summary>
		private readonly BlockScanner.Conf conf;

		/// <summary>The DataNode this VolumEscanner is associated with.</summary>
		private readonly DataNode datanode;

		/// <summary>A reference to the volume that we're scanning.</summary>
		private readonly FsVolumeReference @ref;

		/// <summary>The volume that we're scanning.</summary>
		internal readonly FsVolumeSpi volume;

		/// <summary>
		/// The number of scanned bytes in each minute of the last hour.<p/>
		/// This array is managed as a circular buffer.
		/// </summary>
		/// <remarks>
		/// The number of scanned bytes in each minute of the last hour.<p/>
		/// This array is managed as a circular buffer.  We take the monotonic time and
		/// divide it up into one-minute periods.  Each entry in the array represents
		/// how many bytes were scanned during that period.
		/// </remarks>
		private readonly long[] scannedBytes = new long[MinutesPerHour];

		/// <summary>The sum of all the values of scannedBytes.</summary>
		private long scannedBytesSum = 0;

		/// <summary>The throttler to use with BlockSender objects.</summary>
		private readonly DataTransferThrottler throttler = new DataTransferThrottler(1);

		/// <summary>The null output stream to use with BlockSender objects.</summary>
		private readonly DataOutputStream nullStream = new DataOutputStream(new IOUtils.NullOutputStream
			());

		/// <summary>
		/// The block iterators associated with this VolumeScanner.<p/>
		/// Each block pool has its own BlockIterator.
		/// </summary>
		private readonly IList<FsVolumeSpi.BlockIterator> blockIters = new List<FsVolumeSpi.BlockIterator
			>();

		/// <summary>Blocks which are suspect.</summary>
		/// <remarks>
		/// Blocks which are suspect.
		/// The scanner prioritizes scanning these blocks.
		/// </remarks>
		private readonly LinkedHashSet<ExtendedBlock> suspectBlocks = new LinkedHashSet<ExtendedBlock
			>();

		/// <summary>Blocks which were suspect which we have scanned.</summary>
		/// <remarks>
		/// Blocks which were suspect which we have scanned.
		/// This is used to avoid scanning the same suspect block over and over.
		/// </remarks>
		private readonly Com.Google.Common.Cache.Cache<ExtendedBlock, bool> recentSuspectBlocks
			 = CacheBuilder.NewBuilder().MaximumSize(1000).ExpireAfterAccess(10, TimeUnit.Minutes
			).Build();

		/// <summary>The current block iterator, or null if there is none.</summary>
		private FsVolumeSpi.BlockIterator curBlockIter = null;

		/// <summary>
		/// True if the thread is stopping.<p/>
		/// Protected by this object's lock.
		/// </summary>
		private bool stopping = false;

		/// <summary>The monotonic minute that the volume scanner was started on.</summary>
		private long startMinute = 0;

		/// <summary>The current minute, in monotonic terms.</summary>
		private long curMinute = 0;

		/// <summary>Handles scan results.</summary>
		private readonly VolumeScanner.ScanResultHandler resultHandler;

		private readonly VolumeScanner.Statistics stats = new VolumeScanner.Statistics();

		internal class Statistics
		{
			internal long bytesScannedInPastHour = 0;

			internal long blocksScannedInCurrentPeriod = 0;

			internal long blocksScannedSinceRestart = 0;

			internal long scansSinceRestart = 0;

			internal long scanErrorsSinceRestart = 0;

			internal long nextBlockPoolScanStartMs = -1;

			internal long blockPoolPeriodEndsMs = -1;

			internal ExtendedBlock lastBlockScanned = null;

			internal bool eof = false;

			internal Statistics()
			{
			}

			internal Statistics(VolumeScanner.Statistics other)
			{
				this.bytesScannedInPastHour = other.bytesScannedInPastHour;
				this.blocksScannedInCurrentPeriod = other.blocksScannedInCurrentPeriod;
				this.blocksScannedSinceRestart = other.blocksScannedSinceRestart;
				this.scansSinceRestart = other.scansSinceRestart;
				this.scanErrorsSinceRestart = other.scanErrorsSinceRestart;
				this.nextBlockPoolScanStartMs = other.nextBlockPoolScanStartMs;
				this.blockPoolPeriodEndsMs = other.blockPoolPeriodEndsMs;
				this.lastBlockScanned = other.lastBlockScanned;
				this.eof = other.eof;
			}

			public override string ToString()
			{
				return new StringBuilder().Append("Statistics{").Append("bytesScannedInPastHour="
					).Append(bytesScannedInPastHour).Append(", blocksScannedInCurrentPeriod=").Append
					(blocksScannedInCurrentPeriod).Append(", blocksScannedSinceRestart=").Append(blocksScannedSinceRestart
					).Append(", scansSinceRestart=").Append(scansSinceRestart).Append(", scanErrorsSinceRestart="
					).Append(scanErrorsSinceRestart).Append(", nextBlockPoolScanStartMs=").Append(nextBlockPoolScanStartMs
					).Append(", blockPoolPeriodEndsMs=").Append(blockPoolPeriodEndsMs).Append(", lastBlockScanned="
					).Append(lastBlockScanned).Append(", eof=").Append(eof).Append("}").ToString();
			}
		}

		private static double PositiveMsToHours(long ms)
		{
			if (ms <= 0)
			{
				return 0;
			}
			else
			{
				return TimeUnit.Hours.Convert(ms, TimeUnit.Milliseconds);
			}
		}

		public virtual void PrintStats(StringBuilder p)
		{
			p.Append("Block scanner information for volume " + volume.GetStorageID() + " with base path "
				 + volume.GetBasePath() + "%n");
			lock (stats)
			{
				p.Append(string.Format("Bytes verified in last hour       : %57d%n", stats.bytesScannedInPastHour
					));
				p.Append(string.Format("Blocks scanned in current period  : %57d%n", stats.blocksScannedInCurrentPeriod
					));
				p.Append(string.Format("Blocks scanned since restart      : %57d%n", stats.blocksScannedSinceRestart
					));
				p.Append(string.Format("Block pool scans since restart    : %57d%n", stats.scansSinceRestart
					));
				p.Append(string.Format("Block scan errors since restart   : %57d%n", stats.scanErrorsSinceRestart
					));
				if (stats.nextBlockPoolScanStartMs > 0)
				{
					p.Append(string.Format("Hours until next block pool scan  : %57.3f%n", PositiveMsToHours
						(stats.nextBlockPoolScanStartMs - Time.MonotonicNow())));
				}
				if (stats.blockPoolPeriodEndsMs > 0)
				{
					p.Append(string.Format("Hours until possible pool rescan  : %57.3f%n", PositiveMsToHours
						(stats.blockPoolPeriodEndsMs - Time.Now())));
				}
				p.Append(string.Format("Last block scanned                : %57s%n", ((stats.lastBlockScanned
					 == null) ? "none" : stats.lastBlockScanned.ToString())));
				p.Append(string.Format("More blocks to scan in period     : %57s%n", !stats.eof));
				p.Append("%n");
			}
		}

		internal class ScanResultHandler
		{
			private VolumeScanner scanner;

			public virtual void Setup(VolumeScanner scanner)
			{
				Log.Trace("Starting VolumeScanner {}", scanner.volume.GetBasePath());
				this.scanner = scanner;
			}

			public virtual void Handle(ExtendedBlock block, IOException e)
			{
				FsVolumeSpi volume = scanner.volume;
				if (e == null)
				{
					Log.Trace("Successfully scanned {} on {}", block, volume.GetBasePath());
					return;
				}
				// If the block does not exist anymore, then it's not an error.
				if (!volume.GetDataset().Contains(block))
				{
					Log.Debug("Volume {}: block {} is no longer in the dataset.", volume.GetBasePath(
						), block);
					return;
				}
				// If the block exists, the exception may due to a race with write:
				// The BlockSender got an old block path in rbw. BlockReceiver removed
				// the rbw block from rbw to finalized but BlockSender tried to open the
				// file before BlockReceiver updated the VolumeMap. The state of the
				// block can be changed again now, so ignore this error here. If there
				// is a block really deleted by mistake, DirectoryScan should catch it.
				if (e is FileNotFoundException)
				{
					Log.Info("Volume {}: verification failed for {} because of " + "FileNotFoundException.  This may be due to a race with write."
						, volume.GetBasePath(), block);
					return;
				}
				Log.Warn("Reporting bad {} on {}", block, volume.GetBasePath());
				try
				{
					scanner.datanode.ReportBadBlocks(block);
				}
				catch (IOException)
				{
					// This is bad, but not bad enough to shut down the scanner.
					Log.Warn("Cannot report bad " + block.GetBlockId(), e);
				}
			}
		}

		internal VolumeScanner(BlockScanner.Conf conf, DataNode datanode, FsVolumeReference
			 @ref)
		{
			this.conf = conf;
			this.datanode = datanode;
			this.@ref = @ref;
			this.volume = @ref.GetVolume();
			VolumeScanner.ScanResultHandler handler;
			try
			{
				handler = System.Activator.CreateInstance(conf.resultHandler);
			}
			catch (Exception e)
			{
				Log.Error("unable to instantiate {}", conf.resultHandler, e);
				handler = new VolumeScanner.ScanResultHandler();
			}
			this.resultHandler = handler;
			SetName("VolumeScannerThread(" + volume.GetBasePath() + ")");
			SetDaemon(true);
		}

		private void SaveBlockIterator(FsVolumeSpi.BlockIterator iter)
		{
			try
			{
				iter.Save();
			}
			catch (IOException e)
			{
				Log.Warn("{}: error saving {}.", this, iter, e);
			}
		}

		private void ExpireOldScannedBytesRecords(long monotonicMs)
		{
			long newMinute = TimeUnit.Minutes.Convert(monotonicMs, TimeUnit.Milliseconds);
			if (curMinute == newMinute)
			{
				return;
			}
			// If a minute or more has gone past since we last updated the scannedBytes
			// array, zero out the slots corresponding to those minutes.
			for (long m = curMinute + 1; m <= newMinute; m++)
			{
				int slotIdx = (int)(m % MinutesPerHour);
				Log.Trace("{}: updateScannedBytes is zeroing out slotIdx {}.  " + "curMinute = {}; newMinute = {}"
					, this, slotIdx, curMinute, newMinute);
				scannedBytesSum -= scannedBytes[slotIdx];
				scannedBytes[slotIdx] = 0;
			}
			curMinute = newMinute;
		}

		/// <summary>
		/// Find a usable block iterator.<p/>
		/// We will consider available block iterators in order.
		/// </summary>
		/// <remarks>
		/// Find a usable block iterator.<p/>
		/// We will consider available block iterators in order.  This property is
		/// important so that we don't keep rescanning the same block pool id over
		/// and over, while other block pools stay unscanned.<p/>
		/// A block pool is always ready to scan if the iterator is not at EOF.  If
		/// the iterator is at EOF, the block pool will be ready to scan when
		/// conf.scanPeriodMs milliseconds have elapsed since the iterator was last
		/// rewound.<p/>
		/// </remarks>
		/// <returns>
		/// 0 if we found a usable block iterator; the
		/// length of time we should delay before
		/// checking again otherwise.
		/// </returns>
		private long FindNextUsableBlockIter()
		{
			lock (this)
			{
				int numBlockIters = blockIters.Count;
				if (numBlockIters == 0)
				{
					Log.Debug("{}: no block pools are registered.", this);
					return long.MaxValue;
				}
				int curIdx;
				if (curBlockIter == null)
				{
					curIdx = 0;
				}
				else
				{
					curIdx = blockIters.IndexOf(curBlockIter);
					Preconditions.CheckState(curIdx >= 0);
				}
				// Note that this has to be wall-clock time, not monotonic time.  This is
				// because the time saved in the cursor file is a wall-clock time.  We do
				// not want to save a monotonic time in the cursor file, because it resets
				// every time the machine reboots (on most platforms).
				long nowMs = Time.Now();
				long minTimeoutMs = long.MaxValue;
				for (int i = 0; i < numBlockIters; i++)
				{
					int idx = (curIdx + i + 1) % numBlockIters;
					FsVolumeSpi.BlockIterator iter = blockIters[idx];
					if (!iter.AtEnd())
					{
						Log.Info("Now scanning bpid {} on volume {}", iter.GetBlockPoolId(), volume.GetBasePath
							());
						curBlockIter = iter;
						return 0L;
					}
					long iterStartMs = iter.GetIterStartMs();
					long waitMs = (iterStartMs + conf.scanPeriodMs) - nowMs;
					if (waitMs <= 0)
					{
						iter.Rewind();
						Log.Info("Now rescanning bpid {} on volume {}, after more than " + "{} hour(s)", 
							iter.GetBlockPoolId(), volume.GetBasePath(), TimeUnit.Hours.Convert(conf.scanPeriodMs
							, TimeUnit.Milliseconds));
						curBlockIter = iter;
						return 0L;
					}
					minTimeoutMs = Math.Min(minTimeoutMs, waitMs);
				}
				Log.Info("{}: no suitable block pools found to scan.  Waiting {} ms.", this, minTimeoutMs
					);
				return minTimeoutMs;
			}
		}

		/// <summary>Scan a block.</summary>
		/// <param name="cblock">The block to scan.</param>
		/// <param name="bytesPerSec">The bytes per second to scan at.</param>
		/// <returns>
		/// The length of the block that was scanned, or
		/// -1 if the block could not be scanned.
		/// </returns>
		private long ScanBlock(ExtendedBlock cblock, long bytesPerSec)
		{
			// 'cblock' has a valid blockId and block pool id, but we don't yet know the
			// genstamp the block is supposed to have.  Ask the FsDatasetImpl for this
			// information.
			ExtendedBlock block = null;
			try
			{
				Block b = volume.GetDataset().GetStoredBlock(cblock.GetBlockPoolId(), cblock.GetBlockId
					());
				if (b == null)
				{
					Log.Info("FileNotFound while finding block {} on volume {}", cblock, volume.GetBasePath
						());
				}
				else
				{
					block = new ExtendedBlock(cblock.GetBlockPoolId(), b);
				}
			}
			catch (FileNotFoundException)
			{
				Log.Info("FileNotFoundException while finding block {} on volume {}", cblock, volume
					.GetBasePath());
			}
			catch (IOException)
			{
				Log.Warn("I/O error while finding block {} on volume {}", cblock, volume.GetBasePath
					());
			}
			if (block == null)
			{
				return -1;
			}
			// block not found.
			BlockSender blockSender = null;
			try
			{
				blockSender = new BlockSender(block, 0, -1, false, true, true, datanode, null, CachingStrategy
					.NewDropBehind());
				throttler.SetBandwidth(bytesPerSec);
				long bytesRead = blockSender.SendBlock(nullStream, null, throttler);
				resultHandler.Handle(block, null);
				return bytesRead;
			}
			catch (IOException e)
			{
				resultHandler.Handle(block, e);
			}
			finally
			{
				IOUtils.Cleanup(null, blockSender);
			}
			return -1;
		}

		[VisibleForTesting]
		internal static bool CalculateShouldScan(string storageId, long targetBytesPerSec
			, long scannedBytesSum, long startMinute, long curMinute)
		{
			long runMinutes = curMinute - startMinute;
			long effectiveBytesPerSec;
			if (runMinutes <= 0)
			{
				// avoid division by zero
				effectiveBytesPerSec = scannedBytesSum;
			}
			else
			{
				if (runMinutes > MinutesPerHour)
				{
					// we only keep an hour's worth of rate information
					runMinutes = MinutesPerHour;
				}
				effectiveBytesPerSec = scannedBytesSum / (SecondsPerMinute * runMinutes);
			}
			bool shouldScan = effectiveBytesPerSec <= targetBytesPerSec;
			Log.Trace("{}: calculateShouldScan: effectiveBytesPerSec = {}, and " + "targetBytesPerSec = {}.  startMinute = {}, curMinute = {}, "
				 + "shouldScan = {}", storageId, effectiveBytesPerSec, targetBytesPerSec, startMinute
				, curMinute, shouldScan);
			return shouldScan;
		}

		/// <summary>Run an iteration of the VolumeScanner loop.</summary>
		/// <param name="suspectBlock">
		/// A suspect block which we should scan, or null to
		/// scan the next regularly scheduled block.
		/// </param>
		/// <returns>
		/// The number of milliseconds to delay before running the loop
		/// again, or 0 to re-run the loop immediately.
		/// </returns>
		private long RunLoop(ExtendedBlock suspectBlock)
		{
			long bytesScanned = -1;
			bool scanError = false;
			ExtendedBlock block = null;
			try
			{
				long monotonicMs = Time.MonotonicNow();
				ExpireOldScannedBytesRecords(monotonicMs);
				if (!CalculateShouldScan(volume.GetStorageID(), conf.targetBytesPerSec, scannedBytesSum
					, startMinute, curMinute))
				{
					// If neededBytesPerSec is too low, then wait few seconds for some old
					// scannedBytes records to expire.
					return 30000L;
				}
				// Find a usable block pool to scan.
				if (suspectBlock != null)
				{
					block = suspectBlock;
				}
				else
				{
					if ((curBlockIter == null) || curBlockIter.AtEnd())
					{
						long timeout = FindNextUsableBlockIter();
						if (timeout > 0)
						{
							Log.Trace("{}: no block pools are ready to scan yet.  Waiting " + "{} ms.", this, 
								timeout);
							lock (stats)
							{
								stats.nextBlockPoolScanStartMs = Time.MonotonicNow() + timeout;
							}
							return timeout;
						}
						lock (stats)
						{
							stats.scansSinceRestart++;
							stats.blocksScannedInCurrentPeriod = 0;
							stats.nextBlockPoolScanStartMs = -1;
						}
						return 0L;
					}
					try
					{
						block = curBlockIter.NextBlock();
					}
					catch (IOException)
					{
						// There was an error listing the next block in the volume.  This is a
						// serious issue.
						Log.Warn("{}: nextBlock error on {}", this, curBlockIter);
						// On the next loop iteration, curBlockIter#eof will be set to true, and
						// we will pick a different block iterator.
						return 0L;
					}
					if (block == null)
					{
						// The BlockIterator is at EOF.
						Log.Info("{}: finished scanning block pool {}", this, curBlockIter.GetBlockPoolId
							());
						SaveBlockIterator(curBlockIter);
						return 0;
					}
				}
				if (curBlockIter != null)
				{
					long saveDelta = monotonicMs - curBlockIter.GetLastSavedMs();
					if (saveDelta >= conf.cursorSaveMs)
					{
						Log.Debug("{}: saving block iterator {} after {} ms.", this, curBlockIter, saveDelta
							);
						SaveBlockIterator(curBlockIter);
					}
				}
				bytesScanned = ScanBlock(block, conf.targetBytesPerSec);
				if (bytesScanned >= 0)
				{
					scannedBytesSum += bytesScanned;
					scannedBytes[(int)(curMinute % MinutesPerHour)] += bytesScanned;
				}
				else
				{
					scanError = true;
				}
				return 0L;
			}
			finally
			{
				lock (stats)
				{
					stats.bytesScannedInPastHour = scannedBytesSum;
					if (bytesScanned > 0)
					{
						stats.blocksScannedInCurrentPeriod++;
						stats.blocksScannedSinceRestart++;
					}
					if (scanError)
					{
						stats.scanErrorsSinceRestart++;
					}
					if (block != null)
					{
						stats.lastBlockScanned = block;
					}
					if (curBlockIter == null)
					{
						stats.eof = true;
						stats.blockPoolPeriodEndsMs = -1;
					}
					else
					{
						stats.eof = curBlockIter.AtEnd();
						stats.blockPoolPeriodEndsMs = curBlockIter.GetIterStartMs() + conf.scanPeriodMs;
					}
				}
			}
		}

		/// <summary>
		/// If there are elements in the suspectBlocks list, removes
		/// and returns the first one.
		/// </summary>
		/// <remarks>
		/// If there are elements in the suspectBlocks list, removes
		/// and returns the first one.  Otherwise, returns null.
		/// </remarks>
		private ExtendedBlock PopNextSuspectBlock()
		{
			lock (this)
			{
				IEnumerator<ExtendedBlock> iter = suspectBlocks.GetEnumerator();
				if (!iter.HasNext())
				{
					return null;
				}
				ExtendedBlock block = iter.Next();
				iter.Remove();
				return block;
			}
		}

		public override void Run()
		{
			// Record the minute on which the scanner started.
			this.startMinute = TimeUnit.Minutes.Convert(Time.MonotonicNow(), TimeUnit.Milliseconds
				);
			this.curMinute = startMinute;
			try
			{
				Log.Trace("{}: thread starting.", this);
				resultHandler.Setup(this);
				try
				{
					long timeout = 0;
					while (true)
					{
						ExtendedBlock suspectBlock = null;
						// Take the lock to check if we should stop, and access the
						// suspect block list.
						lock (this)
						{
							if (stopping)
							{
								break;
							}
							if (timeout > 0)
							{
								Sharpen.Runtime.Wait(this, timeout);
								if (stopping)
								{
									break;
								}
							}
							suspectBlock = PopNextSuspectBlock();
						}
						timeout = RunLoop(suspectBlock);
					}
				}
				catch (Exception)
				{
					// We are exiting because of an InterruptedException,
					// probably sent by VolumeScanner#shutdown.
					Log.Trace("{} exiting because of InterruptedException.", this);
				}
				catch (Exception e)
				{
					Log.Error("{} exiting because of exception ", this, e);
				}
				Log.Info("{} exiting.", this);
				// Save the current position of all block iterators and close them.
				foreach (FsVolumeSpi.BlockIterator iter in blockIters)
				{
					SaveBlockIterator(iter);
					IOUtils.Cleanup(null, iter);
				}
			}
			finally
			{
				// When the VolumeScanner exits, release the reference we were holding
				// on the volume.  This will allow the volume to be removed later.
				IOUtils.Cleanup(null, @ref);
			}
		}

		public override string ToString()
		{
			return "VolumeScanner(" + volume.GetBasePath() + ", " + volume.GetStorageID() + ")";
		}

		/// <summary>Shut down this scanner.</summary>
		public virtual void Shutdown()
		{
			lock (this)
			{
				stopping = true;
				Sharpen.Runtime.Notify(this);
				this.Interrupt();
			}
		}

		public virtual void MarkSuspectBlock(ExtendedBlock block)
		{
			lock (this)
			{
				if (stopping)
				{
					Log.Debug("{}: Not scheduling suspect block {} for " + "rescanning, because this volume scanner is stopping."
						, this, block);
					return;
				}
				bool recent = recentSuspectBlocks.GetIfPresent(block);
				if (recent != null)
				{
					Log.Debug("{}: Not scheduling suspect block {} for " + "rescanning, because we rescanned it recently."
						, this, block);
					return;
				}
				if (suspectBlocks.Contains(block))
				{
					Log.Debug("{}: suspect block {} is already queued for " + "rescanning.", this, block
						);
					return;
				}
				suspectBlocks.AddItem(block);
				recentSuspectBlocks.Put(block, true);
				Log.Debug("{}: Scheduling suspect block {} for rescanning.", this, block);
				Sharpen.Runtime.Notify(this);
			}
		}

		// wake scanner thread.
		/// <summary>Allow the scanner to scan the given block pool.</summary>
		/// <param name="bpid">The block pool id.</param>
		public virtual void EnableBlockPoolId(string bpid)
		{
			lock (this)
			{
				foreach (FsVolumeSpi.BlockIterator iter in blockIters)
				{
					if (iter.GetBlockPoolId().Equals(bpid))
					{
						Log.Warn("{}: already enabled scanning on block pool {}", this, bpid);
						return;
					}
				}
				FsVolumeSpi.BlockIterator iter_1 = null;
				try
				{
					// Load a block iterator for the next block pool on the volume.
					iter_1 = volume.LoadBlockIterator(bpid, BlockIteratorName);
					Log.Trace("{}: loaded block iterator for {}.", this, bpid);
				}
				catch (FileNotFoundException e)
				{
					Log.Debug("{}: failed to load block iterator: " + e.Message, this);
				}
				catch (IOException e)
				{
					Log.Warn("{}: failed to load block iterator.", this, e);
				}
				if (iter_1 == null)
				{
					iter_1 = volume.NewBlockIterator(bpid, BlockIteratorName);
					Log.Trace("{}: created new block iterator for {}.", this, bpid);
				}
				iter_1.SetMaxStalenessMs(conf.maxStalenessMs);
				blockIters.AddItem(iter_1);
				Sharpen.Runtime.Notify(this);
			}
		}

		/// <summary>Disallow the scanner from scanning the given block pool.</summary>
		/// <param name="bpid">The block pool id.</param>
		public virtual void DisableBlockPoolId(string bpid)
		{
			lock (this)
			{
				IEnumerator<FsVolumeSpi.BlockIterator> i = blockIters.GetEnumerator();
				while (i.HasNext())
				{
					FsVolumeSpi.BlockIterator iter = i.Next();
					if (iter.GetBlockPoolId().Equals(bpid))
					{
						Log.Trace("{}: disabling scanning on block pool {}", this, bpid);
						i.Remove();
						IOUtils.Cleanup(null, iter);
						if (curBlockIter == iter)
						{
							curBlockIter = null;
						}
						Sharpen.Runtime.Notify(this);
						return;
					}
				}
				Log.Warn("{}: can't remove block pool {}, because it was never " + "added.", this
					, bpid);
			}
		}

		[VisibleForTesting]
		internal virtual VolumeScanner.Statistics GetStatistics()
		{
			lock (stats)
			{
				return new VolumeScanner.Statistics(stats);
			}
		}
	}
}
