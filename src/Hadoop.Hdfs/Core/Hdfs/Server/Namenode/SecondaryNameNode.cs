using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Javax.Management;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Source;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>The Secondary NameNode is a helper to the primary NameNode.</summary>
	/// <remarks>
	/// The Secondary NameNode is a helper to the primary NameNode.
	/// The Secondary is responsible for supporting periodic checkpoints
	/// of the HDFS metadata. The current design allows only one Secondary
	/// NameNode per HDFs cluster.
	/// The Secondary NameNode is a daemon that periodically wakes
	/// up (determined by the schedule specified in the configuration),
	/// triggers a periodic checkpoint and then goes back to sleep.
	/// The Secondary NameNode uses the NamenodeProtocol to talk to the
	/// primary NameNode.
	/// </remarks>
	public class SecondaryNameNode : Runnable, SecondaryNameNodeInfoMXBean
	{
		static SecondaryNameNode()
		{
			HdfsConfiguration.Init();
		}

		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.SecondaryNameNode
			).FullName);

		private readonly long starttime = Time.Now();

		private volatile long lastCheckpointTime = 0;

		private Uri fsName;

		private SecondaryNameNode.CheckpointStorage checkpointImage;

		private NamenodeProtocol namenode;

		private Configuration conf;

		private IPEndPoint nameNodeAddr;

		private volatile bool shouldRun;

		private HttpServer2 infoServer;

		private ICollection<URI> checkpointDirs;

		private IList<URI> checkpointEditsDirs;

		private CheckpointConf checkpointConf;

		private FSNamesystem namesystem;

		private Sharpen.Thread checkpointThread;

		private ObjectName nameNodeStatusBeanName;

		private string legacyOivImageDir;

		public override string ToString()
		{
			return GetType().Name + " Status" + "\nName Node Address      : " + nameNodeAddr 
				+ "\nStart Time             : " + Sharpen.Extensions.CreateDate(starttime) + "\nLast Checkpoint        : "
				 + (lastCheckpointTime == 0 ? "--" : ((Time.MonotonicNow() - lastCheckpointTime)
				 / 1000)) + " seconds ago" + "\nCheckpoint Period      : " + checkpointConf.GetPeriod
				() + " seconds" + "\nCheckpoint Transactions: " + checkpointConf.GetTxnCount() +
				 "\nCheckpoint Dirs        : " + checkpointDirs + "\nCheckpoint Edits Dirs  : " 
				+ checkpointEditsDirs;
		}

		[VisibleForTesting]
		internal virtual FSImage GetFSImage()
		{
			return checkpointImage;
		}

		[VisibleForTesting]
		internal virtual int GetMergeErrorCount()
		{
			return checkpointImage.GetMergeErrorCount();
		}

		[VisibleForTesting]
		public virtual FSNamesystem GetFSNamesystem()
		{
			return namesystem;
		}

		[VisibleForTesting]
		internal virtual void SetFSImage(SecondaryNameNode.CheckpointStorage image)
		{
			this.checkpointImage = image;
		}

		[VisibleForTesting]
		internal virtual NamenodeProtocol GetNameNode()
		{
			return namenode;
		}

		[VisibleForTesting]
		internal virtual void SetNameNode(NamenodeProtocol namenode)
		{
			this.namenode = namenode;
		}

		/// <summary>Create a connection to the primary namenode.</summary>
		/// <exception cref="System.IO.IOException"/>
		public SecondaryNameNode(Configuration conf)
			: this(conf, new SecondaryNameNode.CommandLineOpts())
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public SecondaryNameNode(Configuration conf, SecondaryNameNode.CommandLineOpts commandLineOpts
			)
		{
			try
			{
				string nsId = DFSUtil.GetSecondaryNameServiceId(conf);
				if (HAUtil.IsHAEnabled(conf, nsId))
				{
					throw new IOException("Cannot use SecondaryNameNode in an HA cluster." + " The Standby Namenode will perform checkpointing."
						);
				}
				NameNode.InitializeGenericKeys(conf, nsId, null);
				Initialize(conf, commandLineOpts);
			}
			catch (IOException e)
			{
				Shutdown();
				throw;
			}
			catch (HadoopIllegalArgumentException e)
			{
				Shutdown();
				throw;
			}
		}

		public static IPEndPoint GetHttpAddress(Configuration conf)
		{
			return NetUtils.CreateSocketAddr(conf.GetTrimmed(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey
				, DFSConfigKeys.DfsNamenodeSecondaryHttpAddressDefault));
		}

		/// <summary>Initialize SecondaryNameNode.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void Initialize(Configuration conf, SecondaryNameNode.CommandLineOpts commandLineOpts
			)
		{
			IPEndPoint infoSocAddr = GetHttpAddress(conf);
			string infoBindAddress = infoSocAddr.GetHostName();
			UserGroupInformation.SetConfiguration(conf);
			if (UserGroupInformation.IsSecurityEnabled())
			{
				SecurityUtil.Login(conf, DFSConfigKeys.DfsSecondaryNamenodeKeytabFileKey, DFSConfigKeys
					.DfsSecondaryNamenodeKerberosPrincipalKey, infoBindAddress);
			}
			// initiate Java VM metrics
			DefaultMetricsSystem.Initialize("SecondaryNameNode");
			JvmMetrics.Create("SecondaryNameNode", conf.Get(DFSConfigKeys.DfsMetricsSessionIdKey
				), DefaultMetricsSystem.Instance());
			// Create connection to the namenode.
			shouldRun = true;
			nameNodeAddr = NameNode.GetServiceAddress(conf, true);
			this.conf = conf;
			this.namenode = NameNodeProxies.CreateNonHAProxy<NamenodeProtocol>(conf, nameNodeAddr
				, UserGroupInformation.GetCurrentUser(), true).GetProxy();
			// initialize checkpoint directories
			fsName = GetInfoServer();
			checkpointDirs = FSImage.GetCheckpointDirs(conf, "/tmp/hadoop/dfs/namesecondary");
			checkpointEditsDirs = FSImage.GetCheckpointEditsDirs(conf, "/tmp/hadoop/dfs/namesecondary"
				);
			checkpointImage = new SecondaryNameNode.CheckpointStorage(conf, checkpointDirs, checkpointEditsDirs
				);
			checkpointImage.RecoverCreate(commandLineOpts.ShouldFormat());
			checkpointImage.DeleteTempEdits();
			namesystem = new FSNamesystem(conf, checkpointImage, true);
			// Disable quota checks
			namesystem.dir.DisableQuotaChecks();
			// Initialize other scheduling parameters from the configuration
			checkpointConf = new CheckpointConf(conf);
			IPEndPoint httpAddr = infoSocAddr;
			string httpsAddrString = conf.GetTrimmed(DFSConfigKeys.DfsNamenodeSecondaryHttpsAddressKey
				, DFSConfigKeys.DfsNamenodeSecondaryHttpsAddressDefault);
			IPEndPoint httpsAddr = NetUtils.CreateSocketAddr(httpsAddrString);
			HttpServer2.Builder builder = DFSUtil.HttpServerTemplateForNNAndJN(conf, httpAddr
				, httpsAddr, "secondary", DFSConfigKeys.DfsSecondaryNamenodeKerberosInternalSpnegoPrincipalKey
				, DFSConfigKeys.DfsSecondaryNamenodeKeytabFileKey);
			nameNodeStatusBeanName = MBeans.Register("SecondaryNameNode", "SecondaryNameNodeInfo"
				, this);
			infoServer = builder.Build();
			infoServer.SetAttribute("secondary.name.node", this);
			infoServer.SetAttribute("name.system.image", checkpointImage);
			infoServer.SetAttribute(JspHelper.CurrentConf, conf);
			infoServer.AddInternalServlet("imagetransfer", ImageServlet.PathSpec, typeof(ImageServlet
				), true);
			infoServer.Start();
			Log.Info("Web server init done");
			HttpConfig.Policy policy = DFSUtil.GetHttpPolicy(conf);
			int connIdx = 0;
			if (policy.IsHttpEnabled())
			{
				IPEndPoint httpAddress = infoServer.GetConnectorAddress(connIdx++);
				conf.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, NetUtils.GetHostPortString
					(httpAddress));
			}
			if (policy.IsHttpsEnabled())
			{
				IPEndPoint httpsAddress = infoServer.GetConnectorAddress(connIdx);
				conf.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpsAddressKey, NetUtils.GetHostPortString
					(httpsAddress));
			}
			legacyOivImageDir = conf.Get(DFSConfigKeys.DfsNamenodeLegacyOivImageDirKey);
			Log.Info("Checkpoint Period   :" + checkpointConf.GetPeriod() + " secs " + "(" + 
				checkpointConf.GetPeriod() / 60 + " min)");
			Log.Info("Log Size Trigger    :" + checkpointConf.GetTxnCount() + " txns");
		}

		/// <summary>Wait for the service to finish.</summary>
		/// <remarks>
		/// Wait for the service to finish.
		/// (Normally, it runs forever.)
		/// </remarks>
		private void Join()
		{
			try
			{
				infoServer.Join();
			}
			catch (Exception ie)
			{
				Log.Debug("Exception ", ie);
			}
		}

		/// <summary>Shut down this instance of the datanode.</summary>
		/// <remarks>
		/// Shut down this instance of the datanode.
		/// Returns only after shutdown is complete.
		/// </remarks>
		public virtual void Shutdown()
		{
			shouldRun = false;
			if (checkpointThread != null)
			{
				checkpointThread.Interrupt();
				try
				{
					checkpointThread.Join(10000);
				}
				catch (Exception)
				{
					Log.Info("Interrupted waiting to join on checkpointer thread");
					Sharpen.Thread.CurrentThread().Interrupt();
				}
			}
			// maintain status
			try
			{
				if (infoServer != null)
				{
					infoServer.Stop();
					infoServer = null;
				}
			}
			catch (Exception e)
			{
				Log.Warn("Exception shutting down SecondaryNameNode", e);
			}
			if (nameNodeStatusBeanName != null)
			{
				MBeans.Unregister(nameNodeStatusBeanName);
				nameNodeStatusBeanName = null;
			}
			try
			{
				if (checkpointImage != null)
				{
					checkpointImage.Close();
					checkpointImage = null;
				}
			}
			catch (IOException e)
			{
				Log.Warn("Exception while closing CheckpointStorage", e);
			}
			if (namesystem != null)
			{
				namesystem.Shutdown();
				namesystem = null;
			}
		}

		public virtual void Run()
		{
			SecurityUtil.DoAsLoginUserOrFatal(new _PrivilegedAction_358(this));
		}

		private sealed class _PrivilegedAction_358 : PrivilegedAction<object>
		{
			public _PrivilegedAction_358(SecondaryNameNode _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public object Run()
			{
				this._enclosing.DoWork();
				return null;
			}

			private readonly SecondaryNameNode _enclosing;
		}

		//
		// The main work loop
		//
		public virtual void DoWork()
		{
			//
			// Poll the Namenode (once every checkpointCheckPeriod seconds) to find the
			// number of transactions in the edit log that haven't yet been checkpointed.
			//
			long period = checkpointConf.GetCheckPeriod();
			int maxRetries = checkpointConf.GetMaxRetriesOnMergeError();
			while (shouldRun)
			{
				try
				{
					Sharpen.Thread.Sleep(1000 * period);
				}
				catch (Exception)
				{
				}
				// do nothing
				if (!shouldRun)
				{
					break;
				}
				try
				{
					// We may have lost our ticket since last checkpoint, log in again, just in case
					if (UserGroupInformation.IsSecurityEnabled())
					{
						UserGroupInformation.GetCurrentUser().CheckTGTAndReloginFromKeytab();
					}
					long now = Time.MonotonicNow();
					if (ShouldCheckpointBasedOnCount() || now >= lastCheckpointTime + 1000 * checkpointConf
						.GetPeriod())
					{
						DoCheckpoint();
						lastCheckpointTime = now;
					}
				}
				catch (IOException e)
				{
					Log.Error("Exception in doCheckpoint", e);
					Sharpen.Runtime.PrintStackTrace(e);
					// Prevent a huge number of edits from being created due to
					// unrecoverable conditions and endless retries.
					if (checkpointImage.GetMergeErrorCount() > maxRetries)
					{
						Log.Fatal("Merging failed " + checkpointImage.GetMergeErrorCount() + " times.");
						ExitUtil.Terminate(1);
					}
				}
				catch (Exception e)
				{
					Log.Fatal("Throwable Exception in doCheckpoint", e);
					Sharpen.Runtime.PrintStackTrace(e);
					ExitUtil.Terminate(1, e);
				}
			}
		}

		/// <summary>
		/// Download <code>fsimage</code> and <code>edits</code>
		/// files from the name-node.
		/// </summary>
		/// <returns>true if a new image has been downloaded and needs to be loaded</returns>
		/// <exception cref="System.IO.IOException"/>
		internal static bool DownloadCheckpointFiles(Uri nnHostPort, FSImage dstImage, CheckpointSignature
			 sig, RemoteEditLogManifest manifest)
		{
			// Sanity check manifest - these could happen if, eg, someone on the
			// NN side accidentally rmed the storage directories
			if (manifest.GetLogs().IsEmpty())
			{
				throw new IOException("Found no edit logs to download on NN since txid " + sig.mostRecentCheckpointTxId
					);
			}
			long expectedTxId = sig.mostRecentCheckpointTxId + 1;
			if (manifest.GetLogs()[0].GetStartTxId() != expectedTxId)
			{
				throw new IOException("Bad edit log manifest (expected txid = " + expectedTxId + 
					": " + manifest);
			}
			try
			{
				bool b = UserGroupInformation.GetCurrentUser().DoAs(new _PrivilegedExceptionAction_444
					(dstImage, sig, nnHostPort, manifest));
				// get fsimage
				// get edits file
				// true if we haven't loaded all the transactions represented by the
				// downloaded fsimage.
				return b;
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}

		private sealed class _PrivilegedExceptionAction_444 : PrivilegedExceptionAction<bool
			>
		{
			public _PrivilegedExceptionAction_444(FSImage dstImage, CheckpointSignature sig, 
				Uri nnHostPort, RemoteEditLogManifest manifest)
			{
				this.dstImage = dstImage;
				this.sig = sig;
				this.nnHostPort = nnHostPort;
				this.manifest = manifest;
			}

			/// <exception cref="System.Exception"/>
			public bool Run()
			{
				dstImage.GetStorage().cTime = sig.cTime;
				if (sig.mostRecentCheckpointTxId == dstImage.GetStorage().GetMostRecentCheckpointTxId
					())
				{
					Org.Apache.Hadoop.Hdfs.Server.Namenode.SecondaryNameNode.Log.Info("Image has not changed. Will not download image."
						);
				}
				else
				{
					Org.Apache.Hadoop.Hdfs.Server.Namenode.SecondaryNameNode.Log.Info("Image has changed. Downloading updated image from NN."
						);
					MD5Hash downloadedHash = TransferFsImage.DownloadImageToStorage(nnHostPort, sig.mostRecentCheckpointTxId
						, dstImage.GetStorage(), true);
					dstImage.SaveDigestAndRenameCheckpointImage(NNStorage.NameNodeFile.Image, sig.mostRecentCheckpointTxId
						, downloadedHash);
				}
				foreach (RemoteEditLog log in manifest.GetLogs())
				{
					TransferFsImage.DownloadEditsToStorage(nnHostPort, log, dstImage.GetStorage());
				}
				return dstImage.GetLastAppliedTxId() < sig.mostRecentCheckpointTxId;
			}

			private readonly FSImage dstImage;

			private readonly CheckpointSignature sig;

			private readonly Uri nnHostPort;

			private readonly RemoteEditLogManifest manifest;
		}

		internal virtual IPEndPoint GetNameNodeAddress()
		{
			return nameNodeAddr;
		}

		/// <summary>Returns the Jetty server that the Namenode is listening on.</summary>
		/// <exception cref="System.IO.IOException"/>
		private Uri GetInfoServer()
		{
			URI fsName = FileSystem.GetDefaultUri(conf);
			if (!Sharpen.Runtime.EqualsIgnoreCase(HdfsConstants.HdfsUriScheme, fsName.GetScheme
				()))
			{
				throw new IOException("This is not a DFS");
			}
			string scheme = DFSUtil.GetHttpClientScheme(conf);
			URI address = DFSUtil.GetInfoServerWithDefaultHost(fsName.GetHost(), conf, scheme
				);
			Log.Debug("Will connect to NameNode at " + address);
			return address.ToURL();
		}

		/// <summary>Create a new checkpoint</summary>
		/// <returns>if the image is fetched from primary or not</returns>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public virtual bool DoCheckpoint()
		{
			checkpointImage.EnsureCurrentDirExists();
			NNStorage dstStorage = checkpointImage.GetStorage();
			// Tell the namenode to start logging transactions in a new edit file
			// Returns a token that would be used to upload the merged image.
			CheckpointSignature sig = namenode.RollEditLog();
			bool loadImage = false;
			bool isFreshCheckpointer = (checkpointImage.GetNamespaceID() == 0);
			bool isSameCluster = (dstStorage.VersionSupportsFederation(NameNodeLayoutVersion.
				Features) && sig.IsSameCluster(checkpointImage)) || (!dstStorage.VersionSupportsFederation
				(NameNodeLayoutVersion.Features) && sig.NamespaceIdMatches(checkpointImage));
			if (isFreshCheckpointer || (isSameCluster && !sig.StorageVersionMatches(checkpointImage
				.GetStorage())))
			{
				// if we're a fresh 2NN, or if we're on the same cluster and our storage
				// needs an upgrade, just take the storage info from the server.
				dstStorage.SetStorageInfo(sig);
				dstStorage.SetClusterID(sig.GetClusterID());
				dstStorage.SetBlockPoolID(sig.GetBlockpoolID());
				loadImage = true;
			}
			sig.ValidateStorageInfo(checkpointImage);
			// error simulation code for junit test
			CheckpointFaultInjector.GetInstance().AfterSecondaryCallsRollEditLog();
			RemoteEditLogManifest manifest = namenode.GetEditLogManifest(sig.mostRecentCheckpointTxId
				 + 1);
			// Fetch fsimage and edits. Reload the image if previous merge failed.
			loadImage |= DownloadCheckpointFiles(fsName, checkpointImage, sig, manifest) | checkpointImage
				.HasMergeError();
			try
			{
				DoMerge(sig, manifest, loadImage, checkpointImage, namesystem);
			}
			catch (IOException ioe)
			{
				// A merge error occurred. The in-memory file system state may be
				// inconsistent, so the image and edits need to be reloaded.
				checkpointImage.SetMergeError();
				throw;
			}
			// Clear any error since merge was successful.
			checkpointImage.ClearMergeError();
			//
			// Upload the new image into the NameNode. Then tell the Namenode
			// to make this new uploaded image as the most current image.
			//
			long txid = checkpointImage.GetLastAppliedTxId();
			TransferFsImage.UploadImageFromStorage(fsName, conf, dstStorage, NNStorage.NameNodeFile
				.Image, txid);
			// error simulation code for junit test
			CheckpointFaultInjector.GetInstance().AfterSecondaryUploadsNewImage();
			Log.Warn("Checkpoint done. New Image Size: " + dstStorage.GetFsImageName(txid).Length
				());
			if (legacyOivImageDir != null && !legacyOivImageDir.IsEmpty())
			{
				try
				{
					checkpointImage.SaveLegacyOIVImage(namesystem, legacyOivImageDir, new Canceler());
				}
				catch (IOException e)
				{
					Log.Warn("Failed to write legacy OIV image: ", e);
				}
			}
			return loadImage;
		}

		/// <param name="opts">The parameters passed to this program.</param>
		/// <exception>
		/// Exception
		/// if the filesystem does not exist.
		/// </exception>
		/// <returns>0 on success, non zero on error.</returns>
		/// <exception cref="System.Exception"/>
		private int ProcessStartupCommand(SecondaryNameNode.CommandLineOpts opts)
		{
			if (opts.GetCommand() == null)
			{
				return 0;
			}
			string cmd = StringUtils.ToLowerCase(opts.GetCommand().ToString());
			int exitCode = 0;
			try
			{
				switch (opts.GetCommand())
				{
					case SecondaryNameNode.CommandLineOpts.Command.Checkpoint:
					{
						long count = CountUncheckpointedTxns();
						if (count > checkpointConf.GetTxnCount() || opts.ShouldForceCheckpoint())
						{
							DoCheckpoint();
						}
						else
						{
							System.Console.Error.WriteLine("EditLog size " + count + " transactions is " + "smaller than configured checkpoint "
								 + "interval " + checkpointConf.GetTxnCount() + " transactions.");
							System.Console.Error.WriteLine("Skipping checkpoint.");
						}
						break;
					}

					case SecondaryNameNode.CommandLineOpts.Command.Geteditsize:
					{
						long uncheckpointed = CountUncheckpointedTxns();
						System.Console.Out.WriteLine("NameNode has " + uncheckpointed + " uncheckpointed transactions"
							);
						break;
					}

					default:
					{
						throw new Exception("bad command enum: " + opts.GetCommand());
					}
				}
			}
			catch (RemoteException e)
			{
				//
				// This is a error returned by hadoop server. Print
				// out the first line of the error mesage, ignore the stack trace.
				exitCode = 1;
				try
				{
					string[] content;
					content = e.GetLocalizedMessage().Split("\n");
					Log.Error(cmd + ": " + content[0]);
				}
				catch (Exception ex)
				{
					Log.Error(cmd + ": " + ex.GetLocalizedMessage());
				}
			}
			catch (IOException e)
			{
				//
				// IO exception encountered locally.
				//
				exitCode = 1;
				Log.Error(cmd + ": " + e.GetLocalizedMessage());
			}
			finally
			{
			}
			// Does the RPC connection need to be closed?
			return exitCode;
		}

		/// <exception cref="System.IO.IOException"/>
		private long CountUncheckpointedTxns()
		{
			long curTxId = namenode.GetTransactionID();
			long uncheckpointedTxns = curTxId - checkpointImage.GetStorage().GetMostRecentCheckpointTxId
				();
			System.Diagnostics.Debug.Assert(uncheckpointedTxns >= 0);
			return uncheckpointedTxns;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual bool ShouldCheckpointBasedOnCount()
		{
			return CountUncheckpointedTxns() >= checkpointConf.GetTxnCount();
		}

		/// <summary>main() has some simple utility methods.</summary>
		/// <param name="argv">Command line parameters.</param>
		/// <exception>
		/// Exception
		/// if the filesystem does not exist.
		/// </exception>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			SecondaryNameNode.CommandLineOpts opts = Org.Apache.Hadoop.Hdfs.Server.Namenode.SecondaryNameNode
				.ParseArgs(argv);
			if (opts == null)
			{
				Log.Fatal("Failed to parse options");
				ExitUtil.Terminate(1);
			}
			else
			{
				if (opts.ShouldPrintHelp())
				{
					opts.Usage();
					System.Environment.Exit(0);
				}
			}
			StringUtils.StartupShutdownMessage(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.SecondaryNameNode
				), argv, Log);
			Configuration tconf = new HdfsConfiguration();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.SecondaryNameNode secondary = null;
			try
			{
				secondary = new Org.Apache.Hadoop.Hdfs.Server.Namenode.SecondaryNameNode(tconf, opts
					);
			}
			catch (IOException ioe)
			{
				Log.Fatal("Failed to start secondary namenode", ioe);
				ExitUtil.Terminate(1);
			}
			if (opts != null && opts.GetCommand() != null)
			{
				int ret = secondary.ProcessStartupCommand(opts);
				ExitUtil.Terminate(ret);
			}
			if (secondary != null)
			{
				secondary.StartCheckpointThread();
				secondary.Join();
			}
		}

		public virtual void StartCheckpointThread()
		{
			Preconditions.CheckState(checkpointThread == null, "Should not already have a thread"
				);
			Preconditions.CheckState(shouldRun, "shouldRun should be true");
			checkpointThread = new Daemon(this);
			checkpointThread.Start();
		}

		public virtual string GetHostAndPort()
		{
			// SecondaryNameNodeInfoMXXBean
			return NetUtils.GetHostPortString(nameNodeAddr);
		}

		public virtual long GetStartTime()
		{
			// SecondaryNameNodeInfoMXXBean
			return starttime;
		}

		public virtual long GetLastCheckpointTime()
		{
			// SecondaryNameNodeInfoMXXBean
			return lastCheckpointTime;
		}

		public virtual string[] GetCheckpointDirectories()
		{
			// SecondaryNameNodeInfoMXXBean
			AList<string> r = Lists.NewArrayListWithCapacity(checkpointDirs.Count);
			foreach (URI d in checkpointDirs)
			{
				r.AddItem(d.ToString());
			}
			return Sharpen.Collections.ToArray(r, new string[r.Count]);
		}

		public virtual string[] GetCheckpointEditlogDirectories()
		{
			// SecondaryNameNodeInfoMXXBean
			AList<string> r = Lists.NewArrayListWithCapacity(checkpointEditsDirs.Count);
			foreach (URI d in checkpointEditsDirs)
			{
				r.AddItem(d.ToString());
			}
			return Sharpen.Collections.ToArray(r, new string[r.Count]);
		}

		public virtual string GetCompileInfo()
		{
			// VersionInfoMXBean
			return VersionInfo.GetDate() + " by " + VersionInfo.GetUser() + " from " + VersionInfo
				.GetBranch();
		}

		public virtual string GetSoftwareVersion()
		{
			// VersionInfoMXBean
			return VersionInfo.GetVersion();
		}

		/// <summary>Container for parsed command-line options.</summary>
		internal class CommandLineOpts
		{
			private readonly Options options = new Options();

			private readonly Option geteditsizeOpt;

			private readonly Option checkpointOpt;

			private readonly Option formatOpt;

			private readonly Option helpOpt;

			internal SecondaryNameNode.CommandLineOpts.Command cmd;

			internal enum Command
			{
				Geteditsize,
				Checkpoint
			}

			private bool shouldForce;

			private bool shouldFormat;

			private bool shouldPrintHelp;

			internal CommandLineOpts()
			{
				geteditsizeOpt = new Option("geteditsize", "return the number of uncheckpointed transactions on the NameNode"
					);
				checkpointOpt = OptionBuilder.Create("checkpoint");
				formatOpt = new Option("format", "format the local storage during startup");
				helpOpt = new Option("h", "help", false, "get help information");
				options.AddOption(geteditsizeOpt);
				options.AddOption(checkpointOpt);
				options.AddOption(formatOpt);
				options.AddOption(helpOpt);
			}

			public virtual bool ShouldFormat()
			{
				return shouldFormat;
			}

			public virtual bool ShouldPrintHelp()
			{
				return shouldPrintHelp;
			}

			/// <exception cref="Org.Apache.Commons.Cli.ParseException"/>
			public virtual void Parse(params string[] argv)
			{
				CommandLineParser parser = new PosixParser();
				CommandLine cmdLine = parser.Parse(options, argv);
				if (cmdLine.HasOption(helpOpt.GetOpt()) || cmdLine.HasOption(helpOpt.GetLongOpt()
					))
				{
					shouldPrintHelp = true;
					return;
				}
				bool hasGetEdit = cmdLine.HasOption(geteditsizeOpt.GetOpt());
				bool hasCheckpoint = cmdLine.HasOption(checkpointOpt.GetOpt());
				if (hasGetEdit && hasCheckpoint)
				{
					throw new ParseException("May not pass both " + geteditsizeOpt.GetOpt() + " and "
						 + checkpointOpt.GetOpt());
				}
				if (hasGetEdit)
				{
					cmd = SecondaryNameNode.CommandLineOpts.Command.Geteditsize;
				}
				else
				{
					if (hasCheckpoint)
					{
						cmd = SecondaryNameNode.CommandLineOpts.Command.Checkpoint;
						string arg = cmdLine.GetOptionValue(checkpointOpt.GetOpt());
						if ("force".Equals(arg))
						{
							shouldForce = true;
						}
						else
						{
							if (arg != null)
							{
								throw new ParseException("-checkpoint may only take 'force' as an " + "argument");
							}
						}
					}
				}
				if (cmdLine.HasOption(formatOpt.GetOpt()))
				{
					shouldFormat = true;
				}
			}

			public virtual SecondaryNameNode.CommandLineOpts.Command GetCommand()
			{
				return cmd;
			}

			public virtual bool ShouldForceCheckpoint()
			{
				return shouldForce;
			}

			internal virtual void Usage()
			{
				string header = "The Secondary NameNode is a helper " + "to the primary NameNode. The Secondary is responsible "
					 + "for supporting periodic checkpoints of the HDFS metadata. " + "The current design allows only one Secondary NameNode "
					 + "per HDFS cluster.";
				HelpFormatter formatter = new HelpFormatter();
				formatter.PrintHelp("secondarynamenode", header, options, string.Empty, false);
			}
		}

		private static SecondaryNameNode.CommandLineOpts ParseArgs(string[] argv)
		{
			SecondaryNameNode.CommandLineOpts opts = new SecondaryNameNode.CommandLineOpts();
			try
			{
				opts.Parse(argv);
			}
			catch (ParseException pe)
			{
				Log.Error(pe.Message);
				opts.Usage();
				return null;
			}
			return opts;
		}

		internal class CheckpointStorage : FSImage
		{
			private int mergeErrorCount;

			private class CheckpointLogPurger : LogsPurgeable
			{
				private readonly NNStorage storage;

				private readonly NNStorageRetentionManager.StoragePurger purger = new NNStorageRetentionManager.DeletionStoragePurger
					();

				public CheckpointLogPurger(NNStorage storage)
				{
					this.storage = storage;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void PurgeLogsOlderThan(long minTxIdToKeep)
				{
					IEnumerator<Storage.StorageDirectory> iter = storage.DirIterator();
					while (iter.HasNext())
					{
						Storage.StorageDirectory dir = iter.Next();
						IList<FileJournalManager.EditLogFile> editFiles = FileJournalManager.MatchEditLogs
							(dir.GetCurrentDir());
						foreach (FileJournalManager.EditLogFile f in editFiles)
						{
							if (f.GetLastTxId() < minTxIdToKeep)
							{
								purger.PurgeLog(f);
							}
						}
					}
				}

				public virtual void SelectInputStreams(ICollection<EditLogInputStream> streams, long
					 fromTxId, bool inProgressOk)
				{
					IEnumerator<Storage.StorageDirectory> iter = storage.DirIterator();
					while (iter.HasNext())
					{
						Storage.StorageDirectory dir = iter.Next();
						IList<FileJournalManager.EditLogFile> editFiles;
						try
						{
							editFiles = FileJournalManager.MatchEditLogs(dir.GetCurrentDir());
						}
						catch (IOException ioe)
						{
							throw new RuntimeException(ioe);
						}
						FileJournalManager.AddStreamsToCollectionFromFiles(editFiles, streams, fromTxId, 
							inProgressOk);
					}
				}
			}

			/// <summary>Construct a checkpoint image.</summary>
			/// <param name="conf">Node configuration.</param>
			/// <param name="imageDirs">URIs of storage for image.</param>
			/// <param name="editsDirs">URIs of storage for edit logs.</param>
			/// <exception cref="System.IO.IOException">If storage cannot be access.</exception>
			internal CheckpointStorage(Configuration conf, ICollection<URI> imageDirs, IList<
				URI> editsDirs)
				: base(conf, imageDirs, editsDirs)
			{
				// the 2NN never writes edits -- it only downloads them. So
				// we shouldn't have any editLog instance. Setting to null
				// makes sure we don't accidentally depend on it.
				editLog = null;
				mergeErrorCount = 0;
				// Replace the archival manager with one that can actually work on the
				// 2NN's edits storage.
				this.archivalManager = new NNStorageRetentionManager(conf, storage, new SecondaryNameNode.CheckpointStorage.CheckpointLogPurger
					(storage));
			}

			/// <summary>Analyze checkpoint directories.</summary>
			/// <remarks>
			/// Analyze checkpoint directories.
			/// Create directories if they do not exist.
			/// Recover from an unsuccessful checkpoint if necessary.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void RecoverCreate(bool format)
			{
				storage.AttemptRestoreRemovedStorage();
				storage.UnlockAll();
				for (IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(); it.HasNext
					(); )
				{
					Storage.StorageDirectory sd = it.Next();
					bool isAccessible = true;
					try
					{
						// create directories if don't exist yet
						if (!sd.GetRoot().Mkdirs())
						{
						}
					}
					catch (SecurityException)
					{
						// do nothing, directory is already created
						isAccessible = false;
					}
					if (!isAccessible)
					{
						throw new InconsistentFSStateException(sd.GetRoot(), "cannot access checkpoint directory."
							);
					}
					if (format)
					{
						// Don't confirm, since this is just the secondary namenode.
						Log.Info("Formatting storage directory " + sd);
						sd.ClearDirectory();
					}
					Storage.StorageState curState;
					try
					{
						curState = sd.AnalyzeStorage(HdfsServerConstants.StartupOption.Regular, storage);
						switch (curState)
						{
							case Storage.StorageState.NonExistent:
							{
								// sd is locked but not opened
								// fail if any of the configured checkpoint dirs are inaccessible 
								throw new InconsistentFSStateException(sd.GetRoot(), "checkpoint directory does not exist or is not accessible."
									);
							}

							case Storage.StorageState.NotFormatted:
							{
								break;
							}

							case Storage.StorageState.Normal:
							{
								// it's ok since initially there is no current and VERSION
								// Read the VERSION file. This verifies that:
								// (a) the VERSION file for each of the directories is the same,
								// and (b) when we connect to a NN, we can verify that the remote
								// node matches the same namespace that we ran on previously.
								storage.ReadProperties(sd);
								break;
							}

							default:
							{
								// recovery is possible
								sd.DoRecover(curState);
								break;
							}
						}
					}
					catch (IOException ioe)
					{
						sd.Unlock();
						throw;
					}
				}
			}

			internal virtual bool HasMergeError()
			{
				return (mergeErrorCount > 0);
			}

			internal virtual int GetMergeErrorCount()
			{
				return mergeErrorCount;
			}

			internal virtual void SetMergeError()
			{
				mergeErrorCount++;
			}

			internal virtual void ClearMergeError()
			{
				mergeErrorCount = 0;
			}

			/// <summary>
			/// Ensure that the current/ directory exists in all storage
			/// directories
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void EnsureCurrentDirExists()
			{
				for (IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(); it.HasNext
					(); )
				{
					Storage.StorageDirectory sd = it.Next();
					FilePath curDir = sd.GetCurrentDir();
					if (!curDir.Exists() && !curDir.Mkdirs())
					{
						throw new IOException("Could not create directory " + curDir);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void DeleteTempEdits()
			{
				FilenameFilter filter = new _FilenameFilter_1021();
				IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(NNStorage.NameNodeDirType
					.Edits);
				for (; it.HasNext(); )
				{
					Storage.StorageDirectory dir = it.Next();
					FilePath[] tempEdits = dir.GetCurrentDir().ListFiles(filter);
					if (tempEdits != null)
					{
						foreach (FilePath t in tempEdits)
						{
							bool success = t.Delete();
							if (!success)
							{
								Log.Warn("Failed to delete temporary edits file: " + t.GetAbsolutePath());
							}
						}
					}
				}
			}

			private sealed class _FilenameFilter_1021 : FilenameFilter
			{
				public _FilenameFilter_1021()
				{
				}

				public bool Accept(FilePath dir, string name)
				{
					return name.Matches(NNStorage.NameNodeFile.EditsTmp.GetName() + "_(\\d+)-(\\d+)_(\\d+)"
						);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void DoMerge(CheckpointSignature sig, RemoteEditLogManifest manifest
			, bool loadImage, FSImage dstImage, FSNamesystem dstNamesystem)
		{
			NNStorage dstStorage = dstImage.GetStorage();
			dstStorage.SetStorageInfo(sig);
			if (loadImage)
			{
				FilePath file = dstStorage.FindImageFile(NNStorage.NameNodeFile.Image, sig.mostRecentCheckpointTxId
					);
				if (file == null)
				{
					throw new IOException("Couldn't find image file at txid " + sig.mostRecentCheckpointTxId
						 + " even though it should have " + "just been downloaded");
				}
				dstNamesystem.WriteLock();
				try
				{
					dstImage.ReloadFromImageFile(file, dstNamesystem);
				}
				finally
				{
					dstNamesystem.WriteUnlock();
				}
				dstNamesystem.ImageLoadComplete();
			}
			// error simulation code for junit test
			CheckpointFaultInjector.GetInstance().DuringMerge();
			Checkpointer.RollForwardByApplyingLogs(manifest, dstImage, dstNamesystem);
			// The following has the side effect of purging old fsimages/edit logs.
			dstImage.SaveFSImageInAllDirs(dstNamesystem, dstImage.GetLastAppliedTxId());
			dstStorage.WriteAll();
		}
	}
}
