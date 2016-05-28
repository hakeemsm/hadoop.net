using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Lib.Server;
using Org.Apache.Hadoop.Lib.Service;
using Org.Apache.Hadoop.Lib.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Service.Hadoop
{
	public class FileSystemAccessService : BaseService, FileSystemAccess
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Lib.Service.Hadoop.FileSystemAccessService
			));

		public const string Prefix = "hadoop";

		private const string InstrumentationGroup = "hadoop";

		public const string AuthenticationType = "authentication.type";

		public const string KerberosKeytab = "authentication.kerberos.keytab";

		public const string KerberosPrincipal = "authentication.kerberos.principal";

		public const string FsCachePurgeFrequency = "filesystem.cache.purge.frequency";

		public const string FsCachePurgeTimeout = "filesystem.cache.purge.timeout";

		public const string NameNodeWhitelist = "name.node.whitelist";

		public const string HadoopConfDir = "config.dir";

		private static readonly string[] HadoopConfFiles = new string[] { "core-site.xml"
			, "hdfs-site.xml" };

		private const string FileSystemServiceCreated = "FileSystemAccessService.created";

		private class CachedFileSystem
		{
			private FileSystem fs;

			private long lastUse;

			private long timeout;

			private int count;

			public CachedFileSystem(long timeout)
			{
				this.timeout = timeout;
				lastUse = -1;
				count = 0;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual FileSystem GetFileSytem(Configuration conf)
			{
				lock (this)
				{
					if (fs == null)
					{
						fs = FileSystem.Get(conf);
					}
					lastUse = -1;
					count++;
					return fs;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void Release()
			{
				lock (this)
				{
					count--;
					if (count == 0)
					{
						if (timeout == 0)
						{
							fs.Close();
							fs = null;
							lastUse = -1;
						}
						else
						{
							lastUse = Runtime.CurrentTimeMillis();
						}
					}
				}
			}

			// to avoid race conditions in the map cache adding removing entries
			// an entry in the cache remains forever, it just closes/opens filesystems
			// based on their utilization. Worse case scenario, the penalty we'll
			// pay is that the amount of entries in the cache will be the total
			// number of users in HDFS (which seems a resonable overhead).
			/// <exception cref="System.IO.IOException"/>
			internal virtual bool PurgeIfIdle()
			{
				lock (this)
				{
					bool ret = false;
					if (count == 0 && lastUse != -1 && (Runtime.CurrentTimeMillis() - lastUse) > timeout)
					{
						fs.Close();
						fs = null;
						lastUse = -1;
						ret = true;
					}
					return ret;
				}
			}
		}

		public FileSystemAccessService()
			: base(Prefix)
		{
		}

		private ICollection<string> nameNodeWhitelist;

		internal Configuration serviceHadoopConf;

		private AtomicInteger unmanagedFileSystems = new AtomicInteger();

		private ConcurrentHashMap<string, FileSystemAccessService.CachedFileSystem> fsCache
			 = new ConcurrentHashMap<string, FileSystemAccessService.CachedFileSystem>();

		private long purgeTimeout;

		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
		protected internal override void Init()
		{
			Log.Info("Using FileSystemAccess JARs version [{}]", VersionInfo.GetVersion());
			string security = GetServiceConfig().Get(AuthenticationType, "simple").Trim();
			if (security.Equals("kerberos"))
			{
				string defaultName = GetServer().GetName();
				string keytab = Runtime.GetProperty("user.home") + "/" + defaultName + ".keytab";
				keytab = GetServiceConfig().Get(KerberosKeytab, keytab).Trim();
				if (keytab.Length == 0)
				{
					throw new ServiceException(FileSystemAccessException.ERROR.H01, KerberosKeytab);
				}
				string principal = defaultName + "/localhost@LOCALHOST";
				principal = GetServiceConfig().Get(KerberosPrincipal, principal).Trim();
				if (principal.Length == 0)
				{
					throw new ServiceException(FileSystemAccessException.ERROR.H01, KerberosPrincipal
						);
				}
				Configuration conf = new Configuration();
				conf.Set("hadoop.security.authentication", "kerberos");
				UserGroupInformation.SetConfiguration(conf);
				try
				{
					UserGroupInformation.LoginUserFromKeytab(principal, keytab);
				}
				catch (IOException ex)
				{
					throw new ServiceException(FileSystemAccessException.ERROR.H02, ex.Message, ex);
				}
				Log.Info("Using FileSystemAccess Kerberos authentication, principal [{}] keytab [{}]"
					, principal, keytab);
			}
			else
			{
				if (security.Equals("simple"))
				{
					Configuration conf = new Configuration();
					conf.Set("hadoop.security.authentication", "simple");
					UserGroupInformation.SetConfiguration(conf);
					Log.Info("Using FileSystemAccess simple/pseudo authentication, principal [{}]", Runtime
						.GetProperty("user.name"));
				}
				else
				{
					throw new ServiceException(FileSystemAccessException.ERROR.H09, security);
				}
			}
			string hadoopConfDirProp = GetServiceConfig().Get(HadoopConfDir, GetServer().GetConfigDir
				());
			FilePath hadoopConfDir = new FilePath(hadoopConfDirProp).GetAbsoluteFile();
			if (!hadoopConfDir.Exists())
			{
				hadoopConfDir = new FilePath(GetServer().GetConfigDir()).GetAbsoluteFile();
			}
			if (!hadoopConfDir.Exists())
			{
				throw new ServiceException(FileSystemAccessException.ERROR.H10, hadoopConfDir);
			}
			try
			{
				serviceHadoopConf = LoadHadoopConf(hadoopConfDir);
			}
			catch (IOException ex)
			{
				throw new ServiceException(FileSystemAccessException.ERROR.H11, ex.ToString(), ex
					);
			}
			Log.Debug("FileSystemAccess FileSystem configuration:");
			foreach (DictionaryEntry entry in serviceHadoopConf)
			{
				Log.Debug("  {} = {}", entry.Key, entry.Value);
			}
			SetRequiredServiceHadoopConf(serviceHadoopConf);
			nameNodeWhitelist = ToLowerCase(GetServiceConfig().GetTrimmedStringCollection(NameNodeWhitelist
				));
		}

		/// <exception cref="System.IO.IOException"/>
		private Configuration LoadHadoopConf(FilePath dir)
		{
			Configuration hadoopConf = new Configuration(false);
			foreach (string file in HadoopConfFiles)
			{
				FilePath f = new FilePath(dir, file);
				if (f.Exists())
				{
					hadoopConf.AddResource(new Path(f.GetAbsolutePath()));
				}
			}
			return hadoopConf;
		}

		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
		public override void PostInit()
		{
			base.PostInit();
			Instrumentation instrumentation = GetServer().Get<Instrumentation>();
			instrumentation.AddVariable(InstrumentationGroup, "unmanaged.fs", new _Variable_217
				(this));
			instrumentation.AddSampler(InstrumentationGroup, "unmanaged.fs", 60, new _Variable_223
				(this));
			Scheduler scheduler = GetServer().Get<Scheduler>();
			int purgeInterval = GetServiceConfig().GetInt(FsCachePurgeFrequency, 60);
			purgeTimeout = GetServiceConfig().GetLong(FsCachePurgeTimeout, 60);
			purgeTimeout = (purgeTimeout > 0) ? purgeTimeout : 0;
			if (purgeTimeout > 0)
			{
				scheduler.Schedule(new FileSystemAccessService.FileSystemCachePurger(this), purgeInterval
					, purgeInterval, TimeUnit.Seconds);
			}
		}

		private sealed class _Variable_217 : Instrumentation.Variable<int>
		{
			public _Variable_217(FileSystemAccessService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public int GetValue()
			{
				return this._enclosing.unmanagedFileSystems.Get();
			}

			private readonly FileSystemAccessService _enclosing;
		}

		private sealed class _Variable_223 : Instrumentation.Variable<long>
		{
			public _Variable_223(FileSystemAccessService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public long GetValue()
			{
				return (long)this._enclosing.unmanagedFileSystems.Get();
			}

			private readonly FileSystemAccessService _enclosing;
		}

		private class FileSystemCachePurger : Runnable
		{
			public virtual void Run()
			{
				int count = 0;
				foreach (FileSystemAccessService.CachedFileSystem cacheFs in this._enclosing.fsCache
					.Values)
				{
					try
					{
						count += cacheFs.PurgeIfIdle() ? 1 : 0;
					}
					catch (Exception ex)
					{
						FileSystemAccessService.Log.Warn("Error while purging filesystem, " + ex.ToString
							(), ex);
					}
				}
				FileSystemAccessService.Log.Debug("Purged [{}} filesystem instances", count);
			}

			internal FileSystemCachePurger(FileSystemAccessService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly FileSystemAccessService _enclosing;
		}

		private ICollection<string> ToLowerCase(ICollection<string> collection)
		{
			ICollection<string> set = new HashSet<string>();
			foreach (string value in collection)
			{
				set.AddItem(StringUtils.ToLowerCase(value));
			}
			return set;
		}

		public override Type GetInterface()
		{
			return typeof(FileSystemAccess);
		}

		public override Type[] GetServiceDependencies()
		{
			return new Type[] { typeof(Instrumentation), typeof(Scheduler) };
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual UserGroupInformation GetUGI(string user)
		{
			return UserGroupInformation.CreateProxyUser(user, UserGroupInformation.GetLoginUser
				());
		}

		protected internal virtual void SetRequiredServiceHadoopConf(Configuration conf)
		{
			conf.Set("fs.hdfs.impl.disable.cache", "true");
		}

		private const string HttpfsFsUser = "httpfs.fs.user";

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual FileSystem CreateFileSystem(Configuration namenodeConf
			)
		{
			string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
			FileSystemAccessService.CachedFileSystem newCachedFS = new FileSystemAccessService.CachedFileSystem
				(purgeTimeout);
			FileSystemAccessService.CachedFileSystem cachedFS = fsCache.PutIfAbsent(user, newCachedFS
				);
			if (cachedFS == null)
			{
				cachedFS = newCachedFS;
			}
			Configuration conf = new Configuration(namenodeConf);
			conf.Set(HttpfsFsUser, user);
			return cachedFS.GetFileSytem(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CloseFileSystem(FileSystem fs)
		{
			if (fsCache.Contains(fs.GetConf().Get(HttpfsFsUser)))
			{
				fsCache[fs.GetConf().Get(HttpfsFsUser)].Release();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccessException"/>
		protected internal virtual void ValidateNamenode(string namenode)
		{
			if (nameNodeWhitelist.Count > 0 && !nameNodeWhitelist.Contains("*"))
			{
				if (!nameNodeWhitelist.Contains(StringUtils.ToLowerCase(namenode)))
				{
					throw new FileSystemAccessException(FileSystemAccessException.ERROR.H05, namenode
						, "not in whitelist");
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccessException"/>
		protected internal virtual void CheckNameNodeHealth(FileSystem fileSystem)
		{
		}

		/// <exception cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccessException"/>
		public override T Execute<T>(string user, Configuration conf, FileSystemAccess.FileSystemExecutor
			<T> executor)
		{
			Check.NotEmpty(user, "user");
			Check.NotNull(conf, "conf");
			Check.NotNull(executor, "executor");
			if (!conf.GetBoolean(FileSystemServiceCreated, false))
			{
				throw new FileSystemAccessException(FileSystemAccessException.ERROR.H04);
			}
			if (conf.Get(CommonConfigurationKeysPublic.FsDefaultNameKey) == null || conf.GetTrimmed
				(CommonConfigurationKeysPublic.FsDefaultNameKey).Length == 0)
			{
				throw new FileSystemAccessException(FileSystemAccessException.ERROR.H06, CommonConfigurationKeysPublic
					.FsDefaultNameKey);
			}
			try
			{
				ValidateNamenode(new URI(conf.Get(CommonConfigurationKeysPublic.FsDefaultNameKey)
					).GetAuthority());
				UserGroupInformation ugi = GetUGI(user);
				return ugi.DoAs(new _PrivilegedExceptionAction_333(this, conf, executor));
			}
			catch (FileSystemAccessException ex)
			{
				throw;
			}
			catch (Exception ex)
			{
				throw new FileSystemAccessException(FileSystemAccessException.ERROR.H03, ex);
			}
		}

		private sealed class _PrivilegedExceptionAction_333 : PrivilegedExceptionAction<T
			>
		{
			public _PrivilegedExceptionAction_333(FileSystemAccessService _enclosing, Configuration
				 conf, FileSystemAccess.FileSystemExecutor<T> executor)
			{
				this._enclosing = _enclosing;
				this.conf = conf;
				this.executor = executor;
			}

			/// <exception cref="System.Exception"/>
			public T Run()
			{
				FileSystem fs = this._enclosing.CreateFileSystem(conf);
				Instrumentation instrumentation = this._enclosing.GetServer().Get<Instrumentation
					>();
				Instrumentation.Cron cron = instrumentation.CreateCron();
				try
				{
					this._enclosing.CheckNameNodeHealth(fs);
					cron.Start();
					return executor.Execute(fs);
				}
				finally
				{
					cron.Stop();
					instrumentation.AddCron(FileSystemAccessService.InstrumentationGroup, executor.GetType
						().Name, cron);
					this._enclosing.CloseFileSystem(fs);
				}
			}

			private readonly FileSystemAccessService _enclosing;

			private readonly Configuration conf;

			private readonly FileSystemAccess.FileSystemExecutor<T> executor;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccessException"/>
		public virtual FileSystem CreateFileSystemInternal(string user, Configuration conf
			)
		{
			Check.NotEmpty(user, "user");
			Check.NotNull(conf, "conf");
			if (!conf.GetBoolean(FileSystemServiceCreated, false))
			{
				throw new FileSystemAccessException(FileSystemAccessException.ERROR.H04);
			}
			try
			{
				ValidateNamenode(new URI(conf.Get(CommonConfigurationKeysPublic.FsDefaultNameKey)
					).GetAuthority());
				UserGroupInformation ugi = GetUGI(user);
				return ugi.DoAs(new _PrivilegedExceptionAction_368(this, conf));
			}
			catch (IOException ex)
			{
				throw;
			}
			catch (FileSystemAccessException ex)
			{
				throw;
			}
			catch (Exception ex)
			{
				throw new FileSystemAccessException(FileSystemAccessException.ERROR.H08, ex.Message
					, ex);
			}
		}

		private sealed class _PrivilegedExceptionAction_368 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_368(FileSystemAccessService _enclosing, Configuration
				 conf)
			{
				this._enclosing = _enclosing;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return this._enclosing.CreateFileSystem(conf);
			}

			private readonly FileSystemAccessService _enclosing;

			private readonly Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccessException"/>
		public override FileSystem CreateFileSystem(string user, Configuration conf)
		{
			unmanagedFileSystems.IncrementAndGet();
			return CreateFileSystemInternal(user, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReleaseFileSystem(FileSystem fs)
		{
			unmanagedFileSystems.DecrementAndGet();
			CloseFileSystem(fs);
		}

		public override Configuration GetFileSystemConfiguration()
		{
			Configuration conf = new Configuration(true);
			ConfigurationUtils.Copy(serviceHadoopConf, conf);
			conf.SetBoolean(FileSystemServiceCreated, true);
			// Force-clear server-side umask to make HttpFS match WebHDFS behavior
			conf.Set(FsPermission.UmaskLabel, "000");
			return conf;
		}
	}
}
