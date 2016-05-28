using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Javax.Management;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Qjournal.Client;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Source;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Mortbay.Util.Ajax;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Server
{
	/// <summary>
	/// The JournalNode is a daemon which allows namenodes using
	/// the QuorumJournalManager to log and retrieve edits stored
	/// remotely.
	/// </summary>
	/// <remarks>
	/// The JournalNode is a daemon which allows namenodes using
	/// the QuorumJournalManager to log and retrieve edits stored
	/// remotely. It is a thin wrapper around a local edit log
	/// directory with the addition of facilities to participate
	/// in the quorum protocol.
	/// </remarks>
	public class JournalNode : Tool, Configurable, JournalNodeMXBean
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(JournalNode));

		private Configuration conf;

		private JournalNodeRpcServer rpcServer;

		private JournalNodeHttpServer httpServer;

		private readonly IDictionary<string, Journal> journalsById = Maps.NewHashMap();

		private ObjectName journalNodeInfoBeanName;

		private string httpServerURI;

		private FilePath localDir;

		static JournalNode()
		{
			HdfsConfiguration.Init();
		}

		/// <summary>When stopped, the daemon will exit with this code.</summary>
		private int resultCode = 0;

		/// <exception cref="System.IO.IOException"/>
		internal virtual Journal GetOrCreateJournal(string jid, HdfsServerConstants.StartupOption
			 startOpt)
		{
			lock (this)
			{
				QuorumJournalManager.CheckJournalId(jid);
				Journal journal = journalsById[jid];
				if (journal == null)
				{
					FilePath logDir = GetLogDir(jid);
					Log.Info("Initializing journal in directory " + logDir);
					journal = new Journal(conf, logDir, jid, startOpt, new JournalNode.ErrorReporter(
						this));
					journalsById[jid] = journal;
				}
				return journal;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public virtual Journal GetOrCreateJournal(string jid)
		{
			return GetOrCreateJournal(jid, HdfsServerConstants.StartupOption.Regular);
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
			this.localDir = new FilePath(conf.Get(DFSConfigKeys.DfsJournalnodeEditsDirKey, DFSConfigKeys
				.DfsJournalnodeEditsDirDefault).Trim());
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ValidateAndCreateJournalDir(FilePath dir)
		{
			if (!dir.IsAbsolute())
			{
				throw new ArgumentException("Journal dir '" + dir + "' should be an absolute path"
					);
			}
			DiskChecker.CheckDir(dir);
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Start();
			return Join();
		}

		/// <summary>Start listening for edits via RPC.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Start()
		{
			Preconditions.CheckState(!IsStarted(), "JN already running");
			ValidateAndCreateJournalDir(localDir);
			DefaultMetricsSystem.Initialize("JournalNode");
			JvmMetrics.Create("JournalNode", conf.Get(DFSConfigKeys.DfsMetricsSessionIdKey), 
				DefaultMetricsSystem.Instance());
			IPEndPoint socAddr = JournalNodeRpcServer.GetAddress(conf);
			SecurityUtil.Login(conf, DFSConfigKeys.DfsJournalnodeKeytabFileKey, DFSConfigKeys
				.DfsJournalnodeKerberosPrincipalKey, socAddr.GetHostName());
			RegisterJNMXBean();
			httpServer = new JournalNodeHttpServer(conf, this);
			httpServer.Start();
			httpServerURI = httpServer.GetServerURI().ToString();
			rpcServer = new JournalNodeRpcServer(conf, this);
			rpcServer.Start();
		}

		public virtual bool IsStarted()
		{
			return rpcServer != null;
		}

		/// <returns>the address the IPC server is bound to</returns>
		public virtual IPEndPoint GetBoundIpcAddress()
		{
			return rpcServer.GetAddress();
		}

		[Obsolete]
		public virtual IPEndPoint GetBoundHttpAddress()
		{
			return httpServer.GetAddress();
		}

		public virtual string GetHttpServerURI()
		{
			return httpServerURI;
		}

		/// <summary>Stop the daemon with the given status code</summary>
		/// <param name="rc">
		/// the status code with which to exit (non-zero
		/// should indicate an error)
		/// </param>
		public virtual void Stop(int rc)
		{
			this.resultCode = rc;
			if (rpcServer != null)
			{
				rpcServer.Stop();
			}
			if (httpServer != null)
			{
				try
				{
					httpServer.Stop();
				}
				catch (IOException ioe)
				{
					Log.Warn("Unable to stop HTTP server for " + this, ioe);
				}
			}
			foreach (Journal j in journalsById.Values)
			{
				IOUtils.Cleanup(Log, j);
			}
			if (journalNodeInfoBeanName != null)
			{
				MBeans.Unregister(journalNodeInfoBeanName);
				journalNodeInfoBeanName = null;
			}
		}

		/// <summary>Wait for the daemon to exit.</summary>
		/// <returns>the result code (non-zero if error)</returns>
		/// <exception cref="System.Exception"/>
		internal virtual int Join()
		{
			if (rpcServer != null)
			{
				rpcServer.Join();
			}
			return resultCode;
		}

		/// <exception cref="System.Exception"/>
		public virtual void StopAndJoin(int rc)
		{
			Stop(rc);
			Join();
		}

		/// <summary>
		/// Return the directory inside our configured storage
		/// dir which corresponds to a given journal.
		/// </summary>
		/// <param name="jid">the journal identifier</param>
		/// <returns>the file, which may or may not exist yet</returns>
		private FilePath GetLogDir(string jid)
		{
			string dir = conf.Get(DFSConfigKeys.DfsJournalnodeEditsDirKey, DFSConfigKeys.DfsJournalnodeEditsDirDefault
				);
			Preconditions.CheckArgument(jid != null && !jid.IsEmpty(), "bad journal identifier: %s"
				, jid);
			System.Diagnostics.Debug.Assert(jid != null);
			return new FilePath(new FilePath(dir), jid);
		}

		public virtual string GetJournalsStatus()
		{
			// JournalNodeMXBean
			// jid:{Formatted:True/False}
			IDictionary<string, IDictionary<string, string>> status = new Dictionary<string, 
				IDictionary<string, string>>();
			lock (this)
			{
				foreach (KeyValuePair<string, Journal> entry in journalsById)
				{
					IDictionary<string, string> jMap = new Dictionary<string, string>();
					jMap["Formatted"] = bool.ToString(entry.Value.IsFormatted());
					status[entry.Key] = jMap;
				}
			}
			// It is possible that some journals have been formatted before, while the 
			// corresponding journals are not in journalsById yet (because of restarting
			// JN, e.g.). For simplicity, let's just assume a journal is formatted if
			// there is a directory for it. We can also call analyzeStorage method for
			// these directories if necessary.
			// Also note that we do not need to check localDir here since
			// validateAndCreateJournalDir has been called before we register the
			// MXBean.
			FilePath[] journalDirs = localDir.ListFiles(new _FileFilter_261());
			foreach (FilePath journalDir in journalDirs)
			{
				string jid = journalDir.GetName();
				if (!status.Contains(jid))
				{
					IDictionary<string, string> jMap = new Dictionary<string, string>();
					jMap["Formatted"] = "true";
					status[jid] = jMap;
				}
			}
			return JSON.ToString(status);
		}

		private sealed class _FileFilter_261 : FileFilter
		{
			public _FileFilter_261()
			{
			}

			public bool Accept(FilePath file)
			{
				return file.IsDirectory();
			}
		}

		/// <summary>Register JournalNodeMXBean</summary>
		private void RegisterJNMXBean()
		{
			journalNodeInfoBeanName = MBeans.Register("JournalNode", "JournalNodeInfo", this);
		}

		private class ErrorReporter : StorageErrorReporter
		{
			public virtual void ReportErrorOnFile(FilePath f)
			{
				JournalNode.Log.Fatal("Error reported on file " + f + "... exiting", new Exception
					());
				this._enclosing.Stop(1);
			}

			internal ErrorReporter(JournalNode _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly JournalNode _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			StringUtils.StartupShutdownMessage(typeof(JournalNode), args, Log);
			System.Environment.Exit(ToolRunner.Run(new JournalNode(), args));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DiscardSegments(string journalId, long startTxId)
		{
			GetOrCreateJournal(journalId).DiscardSegments(startTxId);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoPreUpgrade(string journalId)
		{
			GetOrCreateJournal(journalId).DoPreUpgrade();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoUpgrade(string journalId, StorageInfo sInfo)
		{
			GetOrCreateJournal(journalId).DoUpgrade(sInfo);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoFinalize(string journalId)
		{
			GetOrCreateJournal(journalId).DoFinalize();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool CanRollBack(string journalId, StorageInfo storage, StorageInfo
			 prevStorage, int targetLayoutVersion)
		{
			return GetOrCreateJournal(journalId, HdfsServerConstants.StartupOption.Rollback).
				CanRollBack(storage, prevStorage, targetLayoutVersion);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoRollback(string journalId)
		{
			GetOrCreateJournal(journalId, HdfsServerConstants.StartupOption.Rollback).DoRollback
				();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetJournalCTime(string journalId)
		{
			return GetOrCreateJournal(journalId).GetJournalCTime();
		}
	}
}
