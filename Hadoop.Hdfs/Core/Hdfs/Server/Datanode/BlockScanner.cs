using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Util.Concurrent;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.IO;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class BlockScanner
	{
		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.BlockScanner
			));

		/// <summary>The DataNode that this scanner is associated with.</summary>
		private readonly DataNode datanode;

		/// <summary>Maps Storage IDs to VolumeScanner objects.</summary>
		private readonly SortedDictionary<string, VolumeScanner> scanners = new SortedDictionary
			<string, VolumeScanner>();

		/// <summary>The scanner configuration.</summary>
		private readonly BlockScanner.Conf conf;

		/// <summary>The cached scanner configuration.</summary>
		internal class Conf
		{
			[VisibleForTesting]
			internal const string InternalDfsDatanodeScanPeriodMs = "internal.dfs.datanode.scan.period.ms.key";

			[VisibleForTesting]
			internal const string InternalVolumeScannerScanResultHandler = "internal.volume.scanner.scan.result.handler";

			[VisibleForTesting]
			internal const string InternalDfsBlockScannerMaxStalenessMs = "internal.dfs.block.scanner.max_staleness.ms";

			[VisibleForTesting]
			internal static readonly long InternalDfsBlockScannerMaxStalenessMsDefault = TimeUnit
				.Milliseconds.Convert(15, TimeUnit.Minutes);

			[VisibleForTesting]
			internal const string InternalDfsBlockScannerCursorSaveIntervalMs = "dfs.block.scanner.cursor.save.interval.ms";

			[VisibleForTesting]
			internal static readonly long InternalDfsBlockScannerCursorSaveIntervalMsDefault = 
				TimeUnit.Milliseconds.Convert(10, TimeUnit.Minutes);

			internal static bool allowUnitTestSettings = false;

			internal readonly long targetBytesPerSec;

			internal readonly long maxStalenessMs;

			internal readonly long scanPeriodMs;

			internal readonly long cursorSaveMs;

			internal readonly Type resultHandler;

			// These are a few internal configuration keys used for unit tests.
			// They can't be set unless the static boolean allowUnitTestSettings has
			// been set to true.
			private static long GetUnitTestLong(Configuration conf, string key, long defVal)
			{
				if (allowUnitTestSettings)
				{
					return conf.GetLong(key, defVal);
				}
				else
				{
					return defVal;
				}
			}

			/// <summary>Determine the configured block scanner interval.</summary>
			/// <remarks>
			/// Determine the configured block scanner interval.
			/// For compatibility with prior releases of HDFS, if the
			/// configured value is zero then the scan period is
			/// set to 3 weeks.
			/// If the configured value is less than zero then the scanner
			/// is disabled.
			/// </remarks>
			/// <param name="conf">Configuration object.</param>
			/// <returns>block scan period in milliseconds.</returns>
			private static long GetConfiguredScanPeriodMs(Configuration conf)
			{
				long tempScanPeriodMs = GetUnitTestLong(conf, InternalDfsDatanodeScanPeriodMs, TimeUnit
					.Milliseconds.Convert(conf.GetLong(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, 
					DFSConfigKeys.DfsDatanodeScanPeriodHoursDefault), TimeUnit.Hours));
				if (tempScanPeriodMs == 0)
				{
					tempScanPeriodMs = TimeUnit.Milliseconds.Convert(DFSConfigKeys.DfsDatanodeScanPeriodHoursDefault
						, TimeUnit.Hours);
				}
				return tempScanPeriodMs;
			}

			internal Conf(Configuration conf)
			{
				this.targetBytesPerSec = Math.Max(0L, conf.GetLong(DFSConfigKeys.DfsBlockScannerVolumeBytesPerSecond
					, DFSConfigKeys.DfsBlockScannerVolumeBytesPerSecondDefault));
				this.maxStalenessMs = Math.Max(0L, GetUnitTestLong(conf, InternalDfsBlockScannerMaxStalenessMs
					, InternalDfsBlockScannerMaxStalenessMsDefault));
				this.scanPeriodMs = GetConfiguredScanPeriodMs(conf);
				this.cursorSaveMs = Math.Max(0L, GetUnitTestLong(conf, InternalDfsBlockScannerCursorSaveIntervalMs
					, InternalDfsBlockScannerCursorSaveIntervalMsDefault));
				if (allowUnitTestSettings)
				{
					this.resultHandler = (Type)conf.GetClass(InternalVolumeScannerScanResultHandler, 
						typeof(VolumeScanner.ScanResultHandler));
				}
				else
				{
					this.resultHandler = typeof(VolumeScanner.ScanResultHandler);
				}
			}
		}

		public BlockScanner(DataNode datanode, Configuration conf)
		{
			this.datanode = datanode;
			this.conf = new BlockScanner.Conf(conf);
			if (IsEnabled())
			{
				Log.Info("Initialized block scanner with targetBytesPerSec {}", this.conf.targetBytesPerSec
					);
			}
			else
			{
				Log.Info("Disabled block scanner.");
			}
		}

		/// <summary>
		/// Returns true if the block scanner is enabled.<p/>
		/// If the block scanner is disabled, no volume scanners will be created, and
		/// no threads will start.
		/// </summary>
		public virtual bool IsEnabled()
		{
			return (conf.scanPeriodMs > 0) && (conf.targetBytesPerSec > 0);
		}

		/// <summary>Set up a scanner for the given block pool and volume.</summary>
		/// <param name="ref">A reference to the volume.</param>
		public virtual void AddVolumeScanner(FsVolumeReference @ref)
		{
			lock (this)
			{
				bool success = false;
				try
				{
					FsVolumeSpi volume = @ref.GetVolume();
					if (!IsEnabled())
					{
						Log.Debug("Not adding volume scanner for {}, because the block " + "scanner is disabled."
							, volume.GetBasePath());
						return;
					}
					VolumeScanner scanner = scanners[volume.GetStorageID()];
					if (scanner != null)
					{
						Log.Error("Already have a scanner for volume {}.", volume.GetBasePath());
						return;
					}
					Log.Debug("Adding scanner for volume {} (StorageID {})", volume.GetBasePath(), volume
						.GetStorageID());
					scanner = new VolumeScanner(conf, datanode, @ref);
					scanner.Start();
					scanners[volume.GetStorageID()] = scanner;
					success = true;
				}
				finally
				{
					if (!success)
					{
						// If we didn't create a new VolumeScanner object, we don't
						// need this reference to the volume.
						IOUtils.Cleanup(null, @ref);
					}
				}
			}
		}

		/// <summary>
		/// Stops and removes a volume scanner.<p/>
		/// This function will block until the volume scanner has stopped.
		/// </summary>
		/// <param name="volume">The volume to remove.</param>
		public virtual void RemoveVolumeScanner(FsVolumeSpi volume)
		{
			lock (this)
			{
				if (!IsEnabled())
				{
					Log.Debug("Not removing volume scanner for {}, because the block " + "scanner is disabled."
						, volume.GetStorageID());
					return;
				}
				VolumeScanner scanner = scanners[volume.GetStorageID()];
				if (scanner == null)
				{
					Log.Warn("No scanner found to remove for volumeId {}", volume.GetStorageID());
					return;
				}
				Log.Info("Removing scanner for volume {} (StorageID {})", volume.GetBasePath(), volume
					.GetStorageID());
				scanner.Shutdown();
				Sharpen.Collections.Remove(scanners, volume.GetStorageID());
				Uninterruptibles.JoinUninterruptibly(scanner, 5, TimeUnit.Minutes);
			}
		}

		/// <summary>
		/// Stops and removes all volume scanners.<p/>
		/// This function will block until all the volume scanners have stopped.
		/// </summary>
		public virtual void RemoveAllVolumeScanners()
		{
			lock (this)
			{
				foreach (KeyValuePair<string, VolumeScanner> entry in scanners)
				{
					entry.Value.Shutdown();
				}
				foreach (KeyValuePair<string, VolumeScanner> entry_1 in scanners)
				{
					Uninterruptibles.JoinUninterruptibly(entry_1.Value, 5, TimeUnit.Minutes);
				}
				scanners.Clear();
			}
		}

		/// <summary>Enable scanning a given block pool id.</summary>
		/// <param name="bpid">The block pool id to enable scanning for.</param>
		internal virtual void EnableBlockPoolId(string bpid)
		{
			lock (this)
			{
				Preconditions.CheckNotNull(bpid);
				foreach (VolumeScanner scanner in scanners.Values)
				{
					scanner.EnableBlockPoolId(bpid);
				}
			}
		}

		/// <summary>Disable scanning a given block pool id.</summary>
		/// <param name="bpid">The block pool id to disable scanning for.</param>
		internal virtual void DisableBlockPoolId(string bpid)
		{
			lock (this)
			{
				Preconditions.CheckNotNull(bpid);
				foreach (VolumeScanner scanner in scanners.Values)
				{
					scanner.DisableBlockPoolId(bpid);
				}
			}
		}

		[VisibleForTesting]
		internal virtual VolumeScanner.Statistics GetVolumeStats(string volumeId)
		{
			lock (this)
			{
				VolumeScanner scanner = scanners[volumeId];
				if (scanner == null)
				{
					return null;
				}
				return scanner.GetStatistics();
			}
		}

		internal virtual void PrintStats(StringBuilder p)
		{
			lock (this)
			{
				// print out all bpids that we're scanning ?
				foreach (KeyValuePair<string, VolumeScanner> entry in scanners)
				{
					entry.Value.PrintStats(p);
				}
			}
		}

		/// <summary>
		/// Mark a block as "suspect."
		/// This means that we should try to rescan it soon.
		/// </summary>
		/// <remarks>
		/// Mark a block as "suspect."
		/// This means that we should try to rescan it soon.  Note that the
		/// VolumeScanner keeps a list of recently suspicious blocks, which
		/// it uses to avoid rescanning the same block over and over in a short
		/// time frame.
		/// </remarks>
		/// <param name="storageId">
		/// The ID of the storage where the block replica
		/// is being stored.
		/// </param>
		/// <param name="block">The block's ID and block pool id.</param>
		internal virtual void MarkSuspectBlock(string storageId, ExtendedBlock block)
		{
			lock (this)
			{
				if (!IsEnabled())
				{
					Log.Debug("Not scanning suspicious block {} on {}, because the block " + "scanner is disabled."
						, block, storageId);
					return;
				}
				VolumeScanner scanner = scanners[storageId];
				if (scanner == null)
				{
					// This could happen if the volume is in the process of being removed.
					// The removal process shuts down the VolumeScanner, but the volume
					// object stays around as long as there are references to it (which
					// should not be that long.)
					Log.Info("Not scanning suspicious block {} on {}, because there is no " + "volume scanner for that storageId."
						, block, storageId);
					return;
				}
				scanner.MarkSuspectBlock(block);
			}
		}

		[System.Serializable]
		public class Servlet : HttpServlet
		{
			private const long serialVersionUID = 1L;

			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest request, HttpServletResponse response
				)
			{
				response.SetContentType("text/plain");
				DataNode datanode = (DataNode)GetServletContext().GetAttribute("datanode");
				BlockScanner blockScanner = datanode.GetBlockScanner();
				StringBuilder buffer = new StringBuilder(8 * 1024);
				if (!blockScanner.IsEnabled())
				{
					Log.Warn("Periodic block scanner is not running");
					buffer.Append("Periodic block scanner is not running. " + "Please check the datanode log if this is unexpected."
						);
				}
				else
				{
					buffer.Append("Block Scanner Statistics\n\n");
					blockScanner.PrintStats(buffer);
				}
				string resp = buffer.ToString();
				Log.Trace("Returned Servlet info {}", resp);
				response.GetWriter().Write(resp);
			}
		}
	}
}
