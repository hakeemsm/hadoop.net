using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class ContainerLocalizer
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.ContainerLocalizer
			));

		public const string Filecache = "filecache";

		public const string Appcache = "appcache";

		public const string Usercache = "usercache";

		public const string Outputdir = "output";

		public const string TokenFileNameFmt = "%s.tokens";

		public const string Workdir = "work";

		private const string AppcacheCtxtFmt = "%s.app.cache.dirs";

		private const string UsercacheCtxtFmt = "%s.user.cache.dirs";

		private static readonly FsPermission FilecachePerms = new FsPermission((short)0x1c8
			);

		private readonly string user;

		private readonly string appId;

		private readonly IList<Path> localDirs;

		private readonly string localizerId;

		private readonly FileContext lfs;

		private readonly Configuration conf;

		private readonly RecordFactory recordFactory;

		private readonly IDictionary<LocalResource, Future<Path>> pendingResources;

		private readonly string appCacheDirContextName;

		/// <exception cref="System.IO.IOException"/>
		public ContainerLocalizer(FileContext lfs, string user, string appId, string localizerId
			, IList<Path> localDirs, RecordFactory recordFactory)
		{
			if (null == user)
			{
				throw new IOException("Cannot initialize for null user");
			}
			if (null == localizerId)
			{
				throw new IOException("Cannot initialize for null containerId");
			}
			this.lfs = lfs;
			this.user = user;
			this.appId = appId;
			this.localDirs = localDirs;
			this.localizerId = localizerId;
			this.recordFactory = recordFactory;
			this.conf = new Configuration();
			this.appCacheDirContextName = string.Format(AppcacheCtxtFmt, appId);
			this.pendingResources = new Dictionary<LocalResource, Future<Path>>();
		}

		internal virtual LocalizationProtocol GetProxy(IPEndPoint nmAddr)
		{
			YarnRPC rpc = YarnRPC.Create(conf);
			return (LocalizationProtocol)rpc.GetProxy(typeof(LocalizationProtocol), nmAddr, conf
				);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual int RunLocalization(IPEndPoint nmAddr)
		{
			// load credentials
			InitDirs(conf, user, appId, lfs, localDirs);
			Credentials creds = new Credentials();
			DataInputStream credFile = null;
			try
			{
				// assume credentials in cwd
				// TODO: Fix
				Path tokenPath = new Path(string.Format(TokenFileNameFmt, localizerId));
				credFile = lfs.Open(tokenPath);
				creds.ReadTokenStorageStream(credFile);
				// Explicitly deleting token file.
				lfs.Delete(tokenPath, false);
			}
			finally
			{
				if (credFile != null)
				{
					credFile.Close();
				}
			}
			// create localizer context
			UserGroupInformation remoteUser = UserGroupInformation.CreateRemoteUser(user);
			remoteUser.AddToken(creds.GetToken(LocalizerTokenIdentifier.Kind));
			LocalizationProtocol nodeManager = remoteUser.DoAs(new _PrivilegedAction_151(this
				, nmAddr));
			// create user context
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(user);
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in creds.GetAllTokens
				())
			{
				ugi.AddToken(token);
			}
			ExecutorService exec = null;
			try
			{
				exec = CreateDownloadThreadPool();
				CompletionService<Path> ecs = CreateCompletionService(exec);
				LocalizeFiles(nodeManager, ecs, ugi);
				return 0;
			}
			catch (Exception e)
			{
				// Print traces to stdout so that they can be logged by the NM address
				// space.
				Sharpen.Runtime.PrintStackTrace(e, System.Console.Out);
				return -1;
			}
			finally
			{
				try
				{
					if (exec != null)
					{
						exec.ShutdownNow();
					}
					LocalDirAllocator.RemoveContext(appCacheDirContextName);
				}
				finally
				{
					CloseFileSystems(ugi);
				}
			}
		}

		private sealed class _PrivilegedAction_151 : PrivilegedAction<LocalizationProtocol
			>
		{
			public _PrivilegedAction_151(ContainerLocalizer _enclosing, IPEndPoint nmAddr)
			{
				this._enclosing = _enclosing;
				this.nmAddr = nmAddr;
			}

			public LocalizationProtocol Run()
			{
				return this._enclosing.GetProxy(nmAddr);
			}

			private readonly ContainerLocalizer _enclosing;

			private readonly IPEndPoint nmAddr;
		}

		internal virtual ExecutorService CreateDownloadThreadPool()
		{
			return Executors.NewSingleThreadExecutor(new ThreadFactoryBuilder().SetNameFormat
				("ContainerLocalizer Downloader").Build());
		}

		internal virtual CompletionService<Path> CreateCompletionService(ExecutorService 
			exec)
		{
			return new ExecutorCompletionService<Path>(exec);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual Callable<Path> Download(Path path, LocalResource rsrc, UserGroupInformation
			 ugi)
		{
			DiskChecker.CheckDir(new FilePath(path.ToUri().GetRawPath()));
			return new FSDownload(lfs, ugi, conf, path, rsrc);
		}

		internal static long GetEstimatedSize(LocalResource rsrc)
		{
			if (rsrc.GetSize() < 0)
			{
				return -1;
			}
			switch (rsrc.GetType())
			{
				case LocalResourceType.Archive:
				case LocalResourceType.Pattern:
				{
					return 5 * rsrc.GetSize();
				}

				case LocalResourceType.File:
				default:
				{
					return rsrc.GetSize();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		internal virtual void Sleep(int duration)
		{
			TimeUnit.Seconds.Sleep(duration);
		}

		protected internal virtual void CloseFileSystems(UserGroupInformation ugi)
		{
			try
			{
				FileSystem.CloseAllForUGI(ugi);
			}
			catch (IOException e)
			{
				Log.Warn("Failed to close filesystems: ", e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void LocalizeFiles(LocalizationProtocol nodemanager, CompletionService
			<Path> cs, UserGroupInformation ugi)
		{
			while (true)
			{
				try
				{
					LocalizerStatus status = CreateStatus();
					LocalizerHeartbeatResponse response = nodemanager.Heartbeat(status);
					switch (response.GetLocalizerAction())
					{
						case LocalizerAction.Live:
						{
							IList<ResourceLocalizationSpec> newRsrcs = response.GetResourceSpecs();
							foreach (ResourceLocalizationSpec newRsrc in newRsrcs)
							{
								if (!pendingResources.Contains(newRsrc.GetResource()))
								{
									pendingResources[newRsrc.GetResource()] = cs.Submit(Download(new Path(newRsrc.GetDestinationDirectory
										().GetFile()), newRsrc.GetResource(), ugi));
								}
							}
							break;
						}

						case LocalizerAction.Die:
						{
							// killall running localizations
							foreach (Future<Path> pending in pendingResources.Values)
							{
								pending.Cancel(true);
							}
							status = CreateStatus();
							// ignore response
							try
							{
								nodemanager.Heartbeat(status);
							}
							catch (YarnException)
							{
							}
							return;
						}
					}
					cs.Poll(1000, TimeUnit.Milliseconds);
				}
				catch (Exception)
				{
					return;
				}
				catch (YarnException)
				{
					// TODO cleanup
					return;
				}
			}
		}

		/// <summary>Create the payload for the HeartBeat.</summary>
		/// <remarks>
		/// Create the payload for the HeartBeat. Mainly the list of
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords.LocalResourceStatus
		/// 	"/>
		/// es
		/// </remarks>
		/// <returns>
		/// a
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords.LocalizerStatus
		/// 	"/>
		/// that can be sent via heartbeat.
		/// </returns>
		/// <exception cref="System.Exception"/>
		private LocalizerStatus CreateStatus()
		{
			IList<LocalResourceStatus> currentResources = new AList<LocalResourceStatus>();
			// TODO: Synchronization??
			for (IEnumerator<LocalResource> i = pendingResources.Keys.GetEnumerator(); i.HasNext
				(); )
			{
				LocalResource rsrc = i.Next();
				LocalResourceStatus stat = recordFactory.NewRecordInstance<LocalResourceStatus>();
				stat.SetResource(rsrc);
				Future<Path> fPath = pendingResources[rsrc];
				if (fPath.IsDone())
				{
					try
					{
						Path localPath = fPath.Get();
						stat.SetLocalPath(ConverterUtils.GetYarnUrlFromPath(localPath));
						stat.SetLocalSize(FileUtil.GetDU(new FilePath(localPath.GetParent().ToUri())));
						stat.SetStatus(ResourceStatusType.FetchSuccess);
					}
					catch (ExecutionException e)
					{
						stat.SetStatus(ResourceStatusType.FetchFailure);
						stat.SetException(SerializedException.NewInstance(e.InnerException));
					}
					catch (CancellationException e)
					{
						stat.SetStatus(ResourceStatusType.FetchFailure);
						stat.SetException(SerializedException.NewInstance(e));
					}
					// TODO shouldn't remove until ACK
					i.Remove();
				}
				else
				{
					stat.SetStatus(ResourceStatusType.FetchPending);
				}
				currentResources.AddItem(stat);
			}
			LocalizerStatus status = recordFactory.NewRecordInstance<LocalizerStatus>();
			status.SetLocalizerId(localizerId);
			status.AddAllResources(currentResources);
			return status;
		}

		/// <summary>
		/// Adds the ContainerLocalizer arguments for a @{link ShellCommandExecutor},
		/// as expected by ContainerLocalizer.main
		/// </summary>
		/// <param name="command">the current ShellCommandExecutor command line</param>
		/// <param name="user">localization user</param>
		/// <param name="appId">localized app id</param>
		/// <param name="locId">localizer id</param>
		/// <param name="nmAddr">nodemanager address</param>
		/// <param name="localDirs">list of local dirs</param>
		public static void BuildMainArgs(IList<string> command, string user, string appId
			, string locId, IPEndPoint nmAddr, IList<string> localDirs)
		{
			command.AddItem(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.ContainerLocalizer
				).FullName);
			command.AddItem(user);
			command.AddItem(appId);
			command.AddItem(locId);
			command.AddItem(nmAddr.GetHostName());
			command.AddItem(Sharpen.Extensions.ToString(nmAddr.Port));
			foreach (string dir in localDirs)
			{
				command.AddItem(dir);
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			Sharpen.Thread.SetDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler
				());
			// usage: $0 user appId locId host port app_log_dir user_dir [user_dir]*
			// let $x = $x/usercache for $local.dir
			// MKDIR $x/$user/appcache/$appid
			// MKDIR $x/$user/appcache/$appid/output
			// MKDIR $x/$user/appcache/$appid/filecache
			// LOAD $x/$user/appcache/$appid/appTokens
			try
			{
				string user = argv[0];
				string appId = argv[1];
				string locId = argv[2];
				IPEndPoint nmAddr = new IPEndPoint(argv[3], System.Convert.ToInt32(argv[4]));
				string[] sLocaldirs = Arrays.CopyOfRange(argv, 5, argv.Length);
				AList<Path> localDirs = new AList<Path>(sLocaldirs.Length);
				foreach (string sLocaldir in sLocaldirs)
				{
					localDirs.AddItem(new Path(sLocaldir));
				}
				string uid = UserGroupInformation.GetCurrentUser().GetShortUserName();
				if (!user.Equals(uid))
				{
					// TODO: fail localization
					Log.Warn("Localization running as " + uid + " not " + user);
				}
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.ContainerLocalizer
					 localizer = new Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.ContainerLocalizer
					(FileContext.GetLocalFSFileContext(), user, appId, locId, localDirs, RecordFactoryProvider
					.GetRecordFactory(null));
				int nRet = localizer.RunLocalization(nmAddr);
				if (Log.IsDebugEnabled())
				{
					Log.Debug(string.Format("nRet: %d", nRet));
				}
				System.Environment.Exit(nRet);
			}
			catch (Exception e)
			{
				// Print error to stdout so that LCE can use it.
				Sharpen.Runtime.PrintStackTrace(e, System.Console.Out);
				Log.Error("Exception in main:", e);
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void InitDirs(Configuration conf, string user, string appId, FileContext
			 lfs, IList<Path> localDirs)
		{
			if (null == localDirs || 0 == localDirs.Count)
			{
				throw new IOException("Cannot initialize without local dirs");
			}
			string[] appsFileCacheDirs = new string[localDirs.Count];
			string[] usersFileCacheDirs = new string[localDirs.Count];
			for (int i = 0; i < n; ++i)
			{
				// $x/usercache/$user
				Path @base = lfs.MakeQualified(new Path(new Path(localDirs[i], Usercache), user));
				// $x/usercache/$user/filecache
				Path userFileCacheDir = new Path(@base, Filecache);
				usersFileCacheDirs[i] = userFileCacheDir.ToString();
				CreateDir(lfs, userFileCacheDir, FilecachePerms, false);
				// $x/usercache/$user/appcache/$appId
				Path appBase = new Path(@base, new Path(Appcache, appId));
				// $x/usercache/$user/appcache/$appId/filecache
				Path appFileCacheDir = new Path(appBase, Filecache);
				appsFileCacheDirs[i] = appFileCacheDir.ToString();
				CreateDir(lfs, appFileCacheDir, FilecachePerms, false);
			}
			conf.SetStrings(string.Format(AppcacheCtxtFmt, appId), appsFileCacheDirs);
			conf.SetStrings(string.Format(UsercacheCtxtFmt, user), usersFileCacheDirs);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateDir(FileContext lfs, Path dirPath, FsPermission perms, 
			bool createParent)
		{
			lfs.Mkdir(dirPath, perms, createParent);
			if (!perms.Equals(perms.ApplyUMask(lfs.GetUMask())))
			{
				lfs.SetPermission(dirPath, perms);
			}
		}
	}
}
