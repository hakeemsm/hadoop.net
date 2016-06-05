using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>
	/// Tool which allows the standby node's storage directories to be bootstrapped
	/// by copying the latest namespace snapshot from the active namenode.
	/// </summary>
	/// <remarks>
	/// Tool which allows the standby node's storage directories to be bootstrapped
	/// by copying the latest namespace snapshot from the active namenode. This is
	/// used when first configuring an HA cluster.
	/// </remarks>
	public class BootstrapStandby : Tool, Configurable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(BootstrapStandby));

		private string nsId;

		private string nnId;

		private string otherNNId;

		private Uri otherHttpAddr;

		private IPEndPoint otherIpcAddr;

		private ICollection<URI> dirsToFormat;

		private IList<URI> editUrisToFormat;

		private IList<URI> sharedEditsUris;

		private Configuration conf;

		private bool force = false;

		private bool interactive = true;

		private bool skipSharedEditsCheck = false;

		internal const int ErrCodeFailedConnect = 2;

		internal const int ErrCodeInvalidVersion = 3;

		internal const int ErrCodeAlreadyFormatted = 5;

		internal const int ErrCodeLogsUnavailable = 6;

		// Exit/return codes.
		// Skip 4 - was used in previous versions, but no longer returned.
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			ParseArgs(args);
			ParseConfAndFindOtherNN();
			NameNode.CheckAllowFormat(conf);
			IPEndPoint myAddr = NameNode.GetAddress(conf);
			SecurityUtil.Login(conf, DFSConfigKeys.DfsNamenodeKeytabFileKey, DFSConfigKeys.DfsNamenodeKerberosPrincipalKey
				, myAddr.GetHostName());
			return SecurityUtil.DoAsLoginUserOrFatal(new _PrivilegedAction_110(this));
		}

		private sealed class _PrivilegedAction_110 : PrivilegedAction<int>
		{
			public _PrivilegedAction_110(BootstrapStandby _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public int Run()
			{
				try
				{
					return this._enclosing.DoRun();
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			}

			private readonly BootstrapStandby _enclosing;
		}

		private void ParseArgs(string[] args)
		{
			foreach (string arg in args)
			{
				if ("-force".Equals(arg))
				{
					force = true;
				}
				else
				{
					if ("-nonInteractive".Equals(arg))
					{
						interactive = false;
					}
					else
					{
						if ("-skipSharedEditsCheck".Equals(arg))
						{
							skipSharedEditsCheck = true;
						}
						else
						{
							PrintUsage();
							throw new HadoopIllegalArgumentException("Illegal argument: " + arg);
						}
					}
				}
			}
		}

		private void PrintUsage()
		{
			System.Console.Error.WriteLine("Usage: " + this.GetType().Name + " [-force] [-nonInteractive] [-skipSharedEditsCheck]"
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private NamenodeProtocol CreateNNProtocolProxy()
		{
			return NameNodeProxies.CreateNonHAProxy<NamenodeProtocol>(GetConf(), otherIpcAddr
				, UserGroupInformation.GetLoginUser(), true).GetProxy();
		}

		/// <exception cref="System.IO.IOException"/>
		private int DoRun()
		{
			NamenodeProtocol proxy = CreateNNProtocolProxy();
			NamespaceInfo nsInfo;
			bool isUpgradeFinalized;
			try
			{
				nsInfo = proxy.VersionRequest();
				isUpgradeFinalized = proxy.IsUpgradeFinalized();
			}
			catch (IOException ioe)
			{
				Log.Fatal("Unable to fetch namespace information from active NN at " + otherIpcAddr
					 + ": " + ioe.Message);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Full exception trace", ioe);
				}
				return ErrCodeFailedConnect;
			}
			if (!CheckLayoutVersion(nsInfo))
			{
				Log.Fatal("Layout version on remote node (" + nsInfo.GetLayoutVersion() + ") does not match "
					 + "this node's layout version (" + HdfsConstants.NamenodeLayoutVersion + ")");
				return ErrCodeInvalidVersion;
			}
			System.Console.Out.WriteLine("=====================================================\n"
				 + "About to bootstrap Standby ID " + nnId + " from:\n" + "           Nameservice ID: "
				 + nsId + "\n" + "        Other Namenode ID: " + otherNNId + "\n" + "  Other NN's HTTP address: "
				 + otherHttpAddr + "\n" + "  Other NN's IPC  address: " + otherIpcAddr + "\n" + 
				"             Namespace ID: " + nsInfo.GetNamespaceID() + "\n" + "            Block pool ID: "
				 + nsInfo.GetBlockPoolID() + "\n" + "               Cluster ID: " + nsInfo.GetClusterID
				() + "\n" + "           Layout version: " + nsInfo.GetLayoutVersion() + "\n" + "       isUpgradeFinalized: "
				 + isUpgradeFinalized + "\n" + "====================================================="
				);
			NNStorage storage = new NNStorage(conf, dirsToFormat, editUrisToFormat);
			if (!isUpgradeFinalized)
			{
				// the remote NameNode is in upgrade state, this NameNode should also
				// create the previous directory. First prepare the upgrade and rename
				// the current dir to previous.tmp.
				Log.Info("The active NameNode is in Upgrade. " + "Prepare the upgrade for the standby NameNode as well."
					);
				if (!DoPreUpgrade(storage, nsInfo))
				{
					return ErrCodeAlreadyFormatted;
				}
			}
			else
			{
				if (!Format(storage, nsInfo))
				{
					// prompt the user to format storage
					return ErrCodeAlreadyFormatted;
				}
			}
			// download the fsimage from active namenode
			int download = DownloadImage(storage, proxy);
			if (download != 0)
			{
				return download;
			}
			// finish the upgrade: rename previous.tmp to previous
			if (!isUpgradeFinalized)
			{
				DoUpgrade(storage);
			}
			return 0;
		}

		/// <summary>
		/// Iterate over all the storage directories, checking if it should be
		/// formatted.
		/// </summary>
		/// <remarks>
		/// Iterate over all the storage directories, checking if it should be
		/// formatted. Format the storage if necessary and allowed by the user.
		/// </remarks>
		/// <returns>True if formatting is processed</returns>
		/// <exception cref="System.IO.IOException"/>
		private bool Format(NNStorage storage, NamespaceInfo nsInfo)
		{
			// Check with the user before blowing away data.
			if (!Storage.ConfirmFormat(storage.DirIterable(null), force, interactive))
			{
				storage.Close();
				return false;
			}
			else
			{
				// Format the storage (writes VERSION file)
				storage.Format(nsInfo);
				return true;
			}
		}

		/// <summary>This is called when using bootstrapStandby for HA upgrade.</summary>
		/// <remarks>
		/// This is called when using bootstrapStandby for HA upgrade. The SBN should
		/// also create previous directory so that later when it starts, it understands
		/// that the cluster is in the upgrade state. This function renames the old
		/// current directory to previous.tmp.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private bool DoPreUpgrade(NNStorage storage, NamespaceInfo nsInfo)
		{
			bool isFormatted = false;
			IDictionary<Storage.StorageDirectory, Storage.StorageState> dataDirStates = new Dictionary
				<Storage.StorageDirectory, Storage.StorageState>();
			try
			{
				isFormatted = FSImage.RecoverStorageDirs(HdfsServerConstants.StartupOption.Upgrade
					, storage, dataDirStates);
				if (dataDirStates.Values.Contains(Storage.StorageState.NotFormatted))
				{
					// recoverStorageDirs returns true if there is a formatted directory
					isFormatted = false;
					System.Console.Error.WriteLine("The original storage directory is not formatted."
						);
				}
			}
			catch (InconsistentFSStateException e)
			{
				// if the storage is in a bad state,
				Log.Warn("The storage directory is in an inconsistent state", e);
			}
			finally
			{
				storage.UnlockAll();
			}
			// if there is InconsistentFSStateException or the storage is not formatted,
			// format the storage. Although this format is done through the new
			// software, since in HA setup the SBN is rolled back through
			// "-bootstrapStandby", we should still be fine.
			if (!isFormatted && !Format(storage, nsInfo))
			{
				return false;
			}
			// make sure there is no previous directory
			FSImage.CheckUpgrade(storage);
			// Do preUpgrade for each directory
			for (IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(false); it.HasNext
				(); )
			{
				Storage.StorageDirectory sd = it.Next();
				try
				{
					NNUpgradeUtil.RenameCurToTmp(sd);
				}
				catch (IOException e)
				{
					Log.Error("Failed to move aside pre-upgrade storage " + "in image directory " + sd
						.GetRoot(), e);
					throw;
				}
			}
			storage.SetStorageInfo(nsInfo);
			storage.SetBlockPoolID(nsInfo.GetBlockPoolID());
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoUpgrade(NNStorage storage)
		{
			for (IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(false); it.HasNext
				(); )
			{
				Storage.StorageDirectory sd = it.Next();
				NNUpgradeUtil.DoUpgrade(sd, storage);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int DownloadImage(NNStorage storage, NamenodeProtocol proxy)
		{
			// Load the newly formatted image, using all of the directories
			// (including shared edits)
			long imageTxId = proxy.GetMostRecentCheckpointTxId();
			long curTxId = proxy.GetTransactionID();
			FSImage image = new FSImage(conf);
			try
			{
				image.GetStorage().SetStorageInfo(storage);
				image.InitEditLog(HdfsServerConstants.StartupOption.Regular);
				System.Diagnostics.Debug.Assert(image.GetEditLog().IsOpenForRead(), "Expected edit log to be open for read"
					);
				// Ensure that we have enough edits already in the shared directory to
				// start up from the last checkpoint on the active.
				if (!skipSharedEditsCheck && !CheckLogsAvailableForRead(image, imageTxId, curTxId
					))
				{
					return ErrCodeLogsUnavailable;
				}
				image.GetStorage().WriteTransactionIdFileToStorage(curTxId);
				// Download that checkpoint into our storage directories.
				MD5Hash hash = TransferFsImage.DownloadImageToStorage(otherHttpAddr, imageTxId, storage
					, true);
				image.SaveDigestAndRenameCheckpointImage(NNStorage.NameNodeFile.Image, imageTxId, 
					hash);
			}
			catch (IOException ioe)
			{
				image.Close();
				throw;
			}
			return 0;
		}

		private bool CheckLogsAvailableForRead(FSImage image, long imageTxId, long curTxIdOnOtherNode
			)
		{
			if (imageTxId == curTxIdOnOtherNode)
			{
				// The other node hasn't written any logs since the last checkpoint.
				// This can be the case if the NN was freshly formatted as HA, and
				// then started in standby mode, so it has no edit logs at all.
				return true;
			}
			long firstTxIdInLogs = imageTxId + 1;
			System.Diagnostics.Debug.Assert(curTxIdOnOtherNode >= firstTxIdInLogs, "first=" +
				 firstTxIdInLogs + " onOtherNode=" + curTxIdOnOtherNode);
			try
			{
				ICollection<EditLogInputStream> streams = image.GetEditLog().SelectInputStreams(firstTxIdInLogs
					, curTxIdOnOtherNode, null, true);
				foreach (EditLogInputStream stream in streams)
				{
					IOUtils.CloseStream(stream);
				}
				return true;
			}
			catch (IOException e)
			{
				string msg = "Unable to read transaction ids " + firstTxIdInLogs + "-" + curTxIdOnOtherNode
					 + " from the configured shared edits storage " + Joiner.On(",").Join(sharedEditsUris
					) + ". " + "Please copy these logs into the shared edits storage " + "or call saveNamespace on the active node.\n"
					 + "Error: " + e.GetLocalizedMessage();
				if (Log.IsDebugEnabled())
				{
					Log.Fatal(msg, e);
				}
				else
				{
					Log.Fatal(msg);
				}
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private bool CheckLayoutVersion(NamespaceInfo nsInfo)
		{
			return (nsInfo.GetLayoutVersion() == HdfsConstants.NamenodeLayoutVersion);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ParseConfAndFindOtherNN()
		{
			Configuration conf = GetConf();
			nsId = DFSUtil.GetNamenodeNameServiceId(conf);
			if (!HAUtil.IsHAEnabled(conf, nsId))
			{
				throw new HadoopIllegalArgumentException("HA is not enabled for this namenode.");
			}
			nnId = HAUtil.GetNameNodeId(conf, nsId);
			NameNode.InitializeGenericKeys(conf, nsId, nnId);
			if (!HAUtil.UsesSharedEditsDir(conf))
			{
				throw new HadoopIllegalArgumentException("Shared edits storage is not enabled for this namenode."
					);
			}
			Configuration otherNode = HAUtil.GetConfForOtherNode(conf);
			otherNNId = HAUtil.GetNameNodeId(otherNode, nsId);
			otherIpcAddr = NameNode.GetServiceAddress(otherNode, true);
			Preconditions.CheckArgument(otherIpcAddr.Port != 0 && !otherIpcAddr.Address.IsAnyLocalAddress
				(), "Could not determine valid IPC address for other NameNode (%s)" + ", got: %s"
				, otherNNId, otherIpcAddr);
			string scheme = DFSUtil.GetHttpClientScheme(conf);
			otherHttpAddr = DFSUtil.GetInfoServerWithDefaultHost(otherIpcAddr.GetHostName(), 
				otherNode, scheme).ToURL();
			dirsToFormat = FSNamesystem.GetNamespaceDirs(conf);
			editUrisToFormat = FSNamesystem.GetNamespaceEditsDirs(conf, false);
			sharedEditsUris = FSNamesystem.GetSharedEditsDirs(conf);
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = DFSHAAdmin.AddSecurityConfiguration(conf);
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <exception cref="System.IO.IOException"/>
		public static int Run(string[] argv, Configuration conf)
		{
			BootstrapStandby bs = new BootstrapStandby();
			bs.SetConf(conf);
			try
			{
				return ToolRunner.Run(bs, argv);
			}
			catch (Exception e)
			{
				if (e is IOException)
				{
					throw (IOException)e;
				}
				else
				{
					throw new IOException(e);
				}
			}
		}
	}
}
