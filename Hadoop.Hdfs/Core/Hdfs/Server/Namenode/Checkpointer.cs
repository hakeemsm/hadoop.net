using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// The Checkpointer is responsible for supporting periodic checkpoints
	/// of the HDFS metadata.
	/// </summary>
	/// <remarks>
	/// The Checkpointer is responsible for supporting periodic checkpoints
	/// of the HDFS metadata.
	/// The Checkpointer is a daemon that periodically wakes up
	/// up (determined by the schedule specified in the configuration),
	/// triggers a periodic checkpoint and then goes back to sleep.
	/// The start of a checkpoint is triggered by one of the two factors:
	/// (1) time or (2) the size of the edits file.
	/// </remarks>
	internal class Checkpointer : Daemon
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.Checkpointer
			).FullName);

		private readonly BackupNode backupNode;

		internal volatile bool shouldRun;

		private string infoBindAddress;

		private CheckpointConf checkpointConf;

		private readonly Configuration conf;

		private BackupImage GetFSImage()
		{
			return (BackupImage)backupNode.GetFSImage();
		}

		private NamenodeProtocol GetRemoteNamenodeProxy()
		{
			return backupNode.namenode;
		}

		/// <summary>Create a connection to the primary namenode.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal Checkpointer(Configuration conf, BackupNode bnNode)
		{
			this.conf = conf;
			this.backupNode = bnNode;
			try
			{
				Initialize(conf);
			}
			catch (IOException e)
			{
				Log.Warn("Checkpointer got exception", e);
				Shutdown();
				throw;
			}
		}

		/// <summary>Initialize checkpoint.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void Initialize(Configuration conf)
		{
			// Create connection to the namenode.
			shouldRun = true;
			// Initialize other scheduling parameters from the configuration
			checkpointConf = new CheckpointConf(conf);
			// Pull out exact http address for posting url to avoid ip aliasing issues
			string fullInfoAddr = conf.Get(DFSConfigKeys.DfsNamenodeBackupHttpAddressKey, DFSConfigKeys
				.DfsNamenodeBackupHttpAddressDefault);
			infoBindAddress = Sharpen.Runtime.Substring(fullInfoAddr, 0, fullInfoAddr.IndexOf
				(":"));
			Log.Info("Checkpoint Period : " + checkpointConf.GetPeriod() + " secs " + "(" + checkpointConf
				.GetPeriod() / 60 + " min)");
			Log.Info("Transactions count is  : " + checkpointConf.GetTxnCount() + ", to trigger checkpoint"
				);
		}

		/// <summary>Shut down the checkpointer.</summary>
		internal virtual void Shutdown()
		{
			shouldRun = false;
			backupNode.Stop();
		}

		//
		// The main work loop
		//
		public override void Run()
		{
			// Check the size of the edit log once every 5 minutes.
			long periodMSec = 5 * 60;
			// 5 minutes
			if (checkpointConf.GetPeriod() < periodMSec)
			{
				periodMSec = checkpointConf.GetPeriod();
			}
			periodMSec *= 1000;
			long lastCheckpointTime = 0;
			if (!backupNode.ShouldCheckpointAtStartup())
			{
				lastCheckpointTime = Time.MonotonicNow();
			}
			while (shouldRun)
			{
				try
				{
					long now = Time.MonotonicNow();
					bool shouldCheckpoint = false;
					if (now >= lastCheckpointTime + periodMSec)
					{
						shouldCheckpoint = true;
					}
					else
					{
						long txns = CountUncheckpointedTxns();
						if (txns >= checkpointConf.GetTxnCount())
						{
							shouldCheckpoint = true;
						}
					}
					if (shouldCheckpoint)
					{
						DoCheckpoint();
						lastCheckpointTime = now;
					}
				}
				catch (IOException e)
				{
					Log.Error("Exception in doCheckpoint: ", e);
				}
				catch (Exception e)
				{
					Log.Error("Throwable Exception in doCheckpoint: ", e);
					Shutdown();
					break;
				}
				try
				{
					Sharpen.Thread.Sleep(periodMSec);
				}
				catch (Exception)
				{
				}
			}
		}

		// do nothing
		/// <exception cref="System.IO.IOException"/>
		private long CountUncheckpointedTxns()
		{
			long curTxId = GetRemoteNamenodeProxy().GetTransactionID();
			long uncheckpointedTxns = curTxId - GetFSImage().GetStorage().GetMostRecentCheckpointTxId
				();
			System.Diagnostics.Debug.Assert(uncheckpointedTxns >= 0);
			return uncheckpointedTxns;
		}

		/// <summary>Create a new checkpoint</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void DoCheckpoint()
		{
			BackupImage bnImage = GetFSImage();
			NNStorage bnStorage = bnImage.GetStorage();
			long startTime = Time.MonotonicNow();
			bnImage.FreezeNamespaceAtNextRoll();
			NamenodeCommand cmd = GetRemoteNamenodeProxy().StartCheckpoint(backupNode.GetRegistration
				());
			CheckpointCommand cpCmd = null;
			switch (cmd.GetAction())
			{
				case NamenodeProtocol.ActShutdown:
				{
					Shutdown();
					throw new IOException("Name-node " + backupNode.nnRpcAddress + " requested shutdown."
						);
				}

				case NamenodeProtocol.ActCheckpoint:
				{
					cpCmd = (CheckpointCommand)cmd;
					break;
				}

				default:
				{
					throw new IOException("Unsupported NamenodeCommand: " + cmd.GetAction());
				}
			}
			bnImage.WaitUntilNamespaceFrozen();
			CheckpointSignature sig = cpCmd.GetSignature();
			// Make sure we're talking to the same NN!
			sig.ValidateStorageInfo(bnImage);
			long lastApplied = bnImage.GetLastAppliedTxId();
			Log.Debug("Doing checkpoint. Last applied: " + lastApplied);
			RemoteEditLogManifest manifest = GetRemoteNamenodeProxy().GetEditLogManifest(bnImage
				.GetLastAppliedTxId() + 1);
			bool needReloadImage = false;
			if (!manifest.GetLogs().IsEmpty())
			{
				RemoteEditLog firstRemoteLog = manifest.GetLogs()[0];
				// we don't have enough logs to roll forward using only logs. Need
				// to download and load the image.
				if (firstRemoteLog.GetStartTxId() > lastApplied + 1)
				{
					Log.Info("Unable to roll forward using only logs. Downloading " + "image with txid "
						 + sig.mostRecentCheckpointTxId);
					MD5Hash downloadedHash = TransferFsImage.DownloadImageToStorage(backupNode.nnHttpAddress
						, sig.mostRecentCheckpointTxId, bnStorage, true);
					bnImage.SaveDigestAndRenameCheckpointImage(NNStorage.NameNodeFile.Image, sig.mostRecentCheckpointTxId
						, downloadedHash);
					lastApplied = sig.mostRecentCheckpointTxId;
					needReloadImage = true;
				}
				if (firstRemoteLog.GetStartTxId() > lastApplied + 1)
				{
					throw new IOException("No logs to roll forward from " + lastApplied);
				}
				// get edits files
				foreach (RemoteEditLog log in manifest.GetLogs())
				{
					TransferFsImage.DownloadEditsToStorage(backupNode.nnHttpAddress, log, bnStorage);
				}
				if (needReloadImage)
				{
					Log.Info("Loading image with txid " + sig.mostRecentCheckpointTxId);
					FilePath file = bnStorage.FindImageFile(NNStorage.NameNodeFile.Image, sig.mostRecentCheckpointTxId
						);
					bnImage.ReloadFromImageFile(file, backupNode.GetNamesystem());
				}
				RollForwardByApplyingLogs(manifest, bnImage, backupNode.GetNamesystem());
			}
			long txid = bnImage.GetLastAppliedTxId();
			backupNode.namesystem.WriteLock();
			try
			{
				backupNode.namesystem.SetImageLoaded();
				if (backupNode.namesystem.GetBlocksTotal() > 0)
				{
					backupNode.namesystem.SetBlockTotal();
				}
				bnImage.SaveFSImageInAllDirs(backupNode.GetNamesystem(), txid);
				bnStorage.WriteAll();
			}
			finally
			{
				backupNode.namesystem.WriteUnlock();
			}
			if (cpCmd.NeedToReturnImage())
			{
				TransferFsImage.UploadImageFromStorage(backupNode.nnHttpAddress, conf, bnStorage, 
					NNStorage.NameNodeFile.Image, txid);
			}
			GetRemoteNamenodeProxy().EndCheckpoint(backupNode.GetRegistration(), sig);
			if (backupNode.GetRole() == HdfsServerConstants.NamenodeRole.Backup)
			{
				bnImage.ConvergeJournalSpool();
			}
			backupNode.SetRegistration();
			// keep registration up to date
			long imageSize = bnImage.GetStorage().GetFsImageName(txid).Length();
			Log.Info("Checkpoint completed in " + (Time.MonotonicNow() - startTime) / 1000 + 
				" seconds." + " New Image Size: " + imageSize);
		}

		private Uri GetImageListenAddress()
		{
			IPEndPoint httpSocAddr = backupNode.GetHttpAddress();
			int httpPort = httpSocAddr.Port;
			try
			{
				return new Uri(DFSUtil.GetHttpClientScheme(conf) + "://" + infoBindAddress + ":" 
					+ httpPort);
			}
			catch (UriFormatException e)
			{
				// Unreachable
				throw new RuntimeException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void RollForwardByApplyingLogs(RemoteEditLogManifest manifest, FSImage
			 dstImage, FSNamesystem dstNamesystem)
		{
			NNStorage dstStorage = dstImage.GetStorage();
			IList<EditLogInputStream> editsStreams = Lists.NewArrayList();
			foreach (RemoteEditLog log in manifest.GetLogs())
			{
				if (log.GetEndTxId() > dstImage.GetLastAppliedTxId())
				{
					FilePath f = dstStorage.FindFinalizedEditsFile(log.GetStartTxId(), log.GetEndTxId
						());
					editsStreams.AddItem(new EditLogFileInputStream(f, log.GetStartTxId(), log.GetEndTxId
						(), true));
				}
			}
			Log.Info("Checkpointer about to load edits from " + editsStreams.Count + " stream(s)."
				);
			dstImage.LoadEdits(editsStreams, dstNamesystem);
		}
	}
}
