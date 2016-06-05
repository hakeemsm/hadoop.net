using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	/// <summary>
	/// This class provides a way to interact with history files in a thread safe
	/// manor.
	/// </summary>
	public class HistoryFileManager : AbstractService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.HS.HistoryFileManager
			));

		private static readonly Log SummaryLog = LogFactory.GetLog(typeof(JobSummary));

		private enum HistoryInfoState
		{
			InIntermediate,
			InDone,
			Deleted,
			MoveFailed
		}

		private static string DoneBeforeSerialTail = JobHistoryUtils.DoneSubdirsBeforeSerialTail
			();

		/// <summary>
		/// Maps between a serial number (generated based on jobId) and the timestamp
		/// component(s) to which it belongs.
		/// </summary>
		/// <remarks>
		/// Maps between a serial number (generated based on jobId) and the timestamp
		/// component(s) to which it belongs. Facilitates jobId based searches. If a
		/// jobId is not found in this list - it will not be found.
		/// </remarks>
		private class SerialNumberIndex
		{
			private SortedDictionary<string, ICollection<string>> cache;

			private int maxSize;

			public SerialNumberIndex(int maxSize)
			{
				this.cache = new SortedDictionary<string, ICollection<string>>();
				this.maxSize = maxSize;
			}

			public virtual void Add(string serialPart, string timestampPart)
			{
				lock (this)
				{
					if (!cache.Contains(serialPart))
					{
						cache[serialPart] = new HashSet<string>();
						if (cache.Count > maxSize)
						{
							string key = cache.FirstKey();
							Log.Error("Dropping " + key + " from the SerialNumberIndex. We will no " + "longer be able to see jobs that are in that serial index for "
								 + cache[key]);
							Sharpen.Collections.Remove(cache, key);
						}
					}
					ICollection<string> datePartSet = cache[serialPart];
					datePartSet.AddItem(timestampPart);
				}
			}

			public virtual void Remove(string serialPart, string timeStampPart)
			{
				lock (this)
				{
					if (cache.Contains(serialPart))
					{
						ICollection<string> set = cache[serialPart];
						set.Remove(timeStampPart);
						if (set.IsEmpty())
						{
							Sharpen.Collections.Remove(cache, serialPart);
						}
					}
				}
			}

			public virtual ICollection<string> Get(string serialPart)
			{
				lock (this)
				{
					ICollection<string> found = cache[serialPart];
					if (found != null)
					{
						return new HashSet<string>(found);
					}
					return null;
				}
			}
		}

		/// <summary>
		/// Wrapper around
		/// <see cref="Sharpen.ConcurrentSkipListMap{K, V}"/>
		/// that maintains size along
		/// side for O(1) size() implementation for use in JobListCache.
		/// Note: The size is not updated atomically with changes additions/removals.
		/// This race can lead to size() returning an incorrect size at times.
		/// </summary>
		internal class JobIdHistoryFileInfoMap
		{
			private ConcurrentSkipListMap<JobId, HistoryFileManager.HistoryFileInfo> cache;

			private AtomicInteger mapSize;

			internal JobIdHistoryFileInfoMap()
			{
				cache = new ConcurrentSkipListMap<JobId, HistoryFileManager.HistoryFileInfo>();
				mapSize = new AtomicInteger();
			}

			public virtual HistoryFileManager.HistoryFileInfo PutIfAbsent(JobId key, HistoryFileManager.HistoryFileInfo
				 value)
			{
				HistoryFileManager.HistoryFileInfo ret = cache.PutIfAbsent(key, value);
				if (ret == null)
				{
					mapSize.IncrementAndGet();
				}
				return ret;
			}

			public virtual HistoryFileManager.HistoryFileInfo Remove(JobId key)
			{
				HistoryFileManager.HistoryFileInfo ret = Sharpen.Collections.Remove(cache, key);
				if (ret != null)
				{
					mapSize.DecrementAndGet();
				}
				return ret;
			}

			/// <summary>Returns the recorded size of the internal map.</summary>
			/// <remarks>
			/// Returns the recorded size of the internal map. Note that this could be out
			/// of sync with the actual size of the map
			/// </remarks>
			/// <returns>"recorded" size</returns>
			public virtual int Size()
			{
				return mapSize.Get();
			}

			public virtual HistoryFileManager.HistoryFileInfo Get(JobId key)
			{
				return cache[key];
			}

			public virtual NavigableSet<JobId> NavigableKeySet()
			{
				return cache.NavigableKeySet();
			}

			public virtual ICollection<HistoryFileManager.HistoryFileInfo> Values()
			{
				return cache.Values;
			}
		}

		internal class JobListCache
		{
			private HistoryFileManager.JobIdHistoryFileInfoMap cache;

			private int maxSize;

			private long maxAge;

			public JobListCache(int maxSize, long maxAge)
			{
				this.maxSize = maxSize;
				this.maxAge = maxAge;
				this.cache = new HistoryFileManager.JobIdHistoryFileInfoMap();
			}

			public virtual HistoryFileManager.HistoryFileInfo AddIfAbsent(HistoryFileManager.HistoryFileInfo
				 fileInfo)
			{
				JobId jobId = fileInfo.GetJobId();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Adding " + jobId + " to job list cache with " + fileInfo.GetJobIndexInfo
						());
				}
				HistoryFileManager.HistoryFileInfo old = cache.PutIfAbsent(jobId, fileInfo);
				if (cache.Size() > maxSize)
				{
					//There is a race here, where more then one thread could be trying to
					// remove entries.  This could result in too many entries being removed
					// from the cache.  This is considered OK as the size of the cache
					// should be rather large, and we would rather have performance over
					// keeping the cache size exactly at the maximum.
					IEnumerator<JobId> keys = cache.NavigableKeySet().GetEnumerator();
					long cutoff = Runtime.CurrentTimeMillis() - maxAge;
					while (cache.Size() > maxSize && keys.HasNext())
					{
						JobId key = keys.Next();
						HistoryFileManager.HistoryFileInfo firstValue = cache.Get(key);
						if (firstValue != null)
						{
							lock (firstValue)
							{
								if (firstValue.IsMovePending())
								{
									if (firstValue.DidMoveFail() && firstValue.jobIndexInfo.GetFinishTime() <= cutoff)
									{
										cache.Remove(key);
										//Now lets try to delete it
										try
										{
											firstValue.Delete();
										}
										catch (IOException e)
										{
											Log.Error("Error while trying to delete history files" + " that could not be moved to done."
												, e);
										}
									}
									else
									{
										Log.Warn("Waiting to remove " + key + " from JobListCache because it is not in done yet."
											);
									}
								}
								else
								{
									cache.Remove(key);
								}
							}
						}
					}
				}
				return old;
			}

			public virtual void Delete(HistoryFileManager.HistoryFileInfo fileInfo)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Removing from cache " + fileInfo);
				}
				cache.Remove(fileInfo.GetJobId());
			}

			public virtual ICollection<HistoryFileManager.HistoryFileInfo> Values()
			{
				return new AList<HistoryFileManager.HistoryFileInfo>(cache.Values());
			}

			public virtual HistoryFileManager.HistoryFileInfo Get(JobId jobId)
			{
				return cache.Get(jobId);
			}

			public virtual bool IsFull()
			{
				return cache.Size() >= maxSize;
			}
		}

		/// <summary>This class represents a user dir in the intermediate done directory.</summary>
		/// <remarks>
		/// This class represents a user dir in the intermediate done directory.  This
		/// is mostly for locking purposes.
		/// </remarks>
		private class UserLogDir
		{
			internal long modTime = 0;

			public virtual void ScanIfNeeded(FileStatus fs)
			{
				lock (this)
				{
					long newModTime = fs.GetModificationTime();
					if (this.modTime != newModTime)
					{
						Path p = fs.GetPath();
						try
						{
							this._enclosing.ScanIntermediateDirectory(p);
							//If scanning fails, we will scan again.  We assume the failure is
							// temporary.
							this.modTime = newModTime;
						}
						catch (IOException e)
						{
							HistoryFileManager.Log.Error("Error while trying to scan the directory " + p, e);
						}
					}
					else
					{
						if (HistoryFileManager.Log.IsDebugEnabled())
						{
							HistoryFileManager.Log.Debug("Scan not needed of " + fs.GetPath());
						}
					}
				}
			}

			internal UserLogDir(HistoryFileManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly HistoryFileManager _enclosing;
		}

		public class HistoryFileInfo
		{
			private Path historyFile;

			private Path confFile;

			private Path summaryFile;

			private JobIndexInfo jobIndexInfo;

			private HistoryFileManager.HistoryInfoState state;

			[VisibleForTesting]
			protected internal HistoryFileInfo(HistoryFileManager _enclosing, Path historyFile
				, Path confFile, Path summaryFile, JobIndexInfo jobIndexInfo, bool isInDone)
			{
				this._enclosing = _enclosing;
				this.historyFile = historyFile;
				this.confFile = confFile;
				this.summaryFile = summaryFile;
				this.jobIndexInfo = jobIndexInfo;
				this.state = isInDone ? HistoryFileManager.HistoryInfoState.InDone : HistoryFileManager.HistoryInfoState
					.InIntermediate;
			}

			[VisibleForTesting]
			internal virtual bool IsMovePending()
			{
				lock (this)
				{
					return this.state == HistoryFileManager.HistoryInfoState.InIntermediate || this.state
						 == HistoryFileManager.HistoryInfoState.MoveFailed;
				}
			}

			[VisibleForTesting]
			internal virtual bool DidMoveFail()
			{
				lock (this)
				{
					return this.state == HistoryFileManager.HistoryInfoState.MoveFailed;
				}
			}

			/// <returns>true if the files backed by this were deleted.</returns>
			public virtual bool IsDeleted()
			{
				lock (this)
				{
					return this.state == HistoryFileManager.HistoryInfoState.Deleted;
				}
			}

			public override string ToString()
			{
				return "HistoryFileInfo jobID " + this.GetJobId() + " historyFile = " + this.historyFile;
			}

			/// <exception cref="System.IO.IOException"/>
			[VisibleForTesting]
			internal virtual void MoveToDone()
			{
				lock (this)
				{
					if (HistoryFileManager.Log.IsDebugEnabled())
					{
						HistoryFileManager.Log.Debug("moveToDone: " + this.historyFile);
					}
					if (!this.IsMovePending())
					{
						// It was either deleted or is already in done. Either way do nothing
						if (HistoryFileManager.Log.IsDebugEnabled())
						{
							HistoryFileManager.Log.Debug("Move no longer pending");
						}
						return;
					}
					try
					{
						long completeTime = this.jobIndexInfo.GetFinishTime();
						if (completeTime == 0)
						{
							completeTime = Runtime.CurrentTimeMillis();
						}
						JobId jobId = this.jobIndexInfo.GetJobId();
						IList<Path> paths = new AList<Path>(2);
						if (this.historyFile == null)
						{
							HistoryFileManager.Log.Info("No file for job-history with " + jobId + " found in cache!"
								);
						}
						else
						{
							paths.AddItem(this.historyFile);
						}
						if (this.confFile == null)
						{
							HistoryFileManager.Log.Info("No file for jobConf with " + jobId + " found in cache!"
								);
						}
						else
						{
							paths.AddItem(this.confFile);
						}
						if (this.summaryFile == null || !this._enclosing.intermediateDoneDirFc.Util().Exists
							(this.summaryFile))
						{
							HistoryFileManager.Log.Info("No summary file for job: " + jobId);
						}
						else
						{
							string jobSummaryString = this._enclosing.GetJobSummary(this._enclosing.intermediateDoneDirFc
								, this.summaryFile);
							HistoryFileManager.SummaryLog.Info(jobSummaryString);
							HistoryFileManager.Log.Info("Deleting JobSummary file: [" + this.summaryFile + "]"
								);
							this._enclosing.intermediateDoneDirFc.Delete(this.summaryFile, false);
							this.summaryFile = null;
						}
						Path targetDir = this._enclosing.CanonicalHistoryLogPath(jobId, completeTime);
						this._enclosing.AddDirectoryToSerialNumberIndex(targetDir);
						this._enclosing.MakeDoneSubdir(targetDir);
						if (this.historyFile != null)
						{
							Path toPath = this._enclosing.doneDirFc.MakeQualified(new Path(targetDir, this.historyFile
								.GetName()));
							if (!toPath.Equals(this.historyFile))
							{
								this._enclosing.MoveToDoneNow(this.historyFile, toPath);
								this.historyFile = toPath;
							}
						}
						if (this.confFile != null)
						{
							Path toPath = this._enclosing.doneDirFc.MakeQualified(new Path(targetDir, this.confFile
								.GetName()));
							if (!toPath.Equals(this.confFile))
							{
								this._enclosing.MoveToDoneNow(this.confFile, toPath);
								this.confFile = toPath;
							}
						}
						this.state = HistoryFileManager.HistoryInfoState.InDone;
					}
					catch (Exception t)
					{
						HistoryFileManager.Log.Error("Error while trying to move a job to done", t);
						this.state = HistoryFileManager.HistoryInfoState.MoveFailed;
					}
				}
			}

			/// <summary>
			/// Parse a job from the JobHistoryFile, if the underlying file is not going
			/// to be deleted.
			/// </summary>
			/// <returns>the Job or null if the underlying file was deleted.</returns>
			/// <exception cref="System.IO.IOException">if there is an error trying to read the file.
			/// 	</exception>
			public virtual Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job LoadJob()
			{
				lock (this)
				{
					return new CompletedJob(this._enclosing.conf, this.jobIndexInfo.GetJobId(), this.
						historyFile, false, this.jobIndexInfo.GetUser(), this, this._enclosing.aclsMgr);
				}
			}

			/// <summary>Return the history file.</summary>
			/// <remarks>Return the history file.  This should only be used for testing.</remarks>
			/// <returns>the history file.</returns>
			internal virtual Path GetHistoryFile()
			{
				lock (this)
				{
					return this.historyFile;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal virtual void Delete()
			{
				lock (this)
				{
					if (HistoryFileManager.Log.IsDebugEnabled())
					{
						HistoryFileManager.Log.Debug("deleting " + this.historyFile + " and " + this.confFile
							);
					}
					this.state = HistoryFileManager.HistoryInfoState.Deleted;
					this._enclosing.doneDirFc.Delete(this._enclosing.doneDirFc.MakeQualified(this.historyFile
						), false);
					this._enclosing.doneDirFc.Delete(this._enclosing.doneDirFc.MakeQualified(this.confFile
						), false);
				}
			}

			public virtual JobIndexInfo GetJobIndexInfo()
			{
				return this.jobIndexInfo;
			}

			public virtual JobId GetJobId()
			{
				return this.jobIndexInfo.GetJobId();
			}

			public virtual Path GetConfFile()
			{
				lock (this)
				{
					return this.confFile;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual Configuration LoadConfFile()
			{
				lock (this)
				{
					FileContext fc = FileContext.GetFileContext(this.confFile.ToUri(), this._enclosing
						.conf);
					Configuration jobConf = new Configuration(false);
					jobConf.AddResource(fc.Open(this.confFile), this.confFile.ToString());
					return jobConf;
				}
			}

			private readonly HistoryFileManager _enclosing;
		}

		private HistoryFileManager.SerialNumberIndex serialNumberIndex = null;

		protected internal HistoryFileManager.JobListCache jobListCache = null;

		private readonly ICollection<Path> existingDoneSubdirs = Sharpen.Collections.SynchronizedSet
			(new HashSet<Path>());

		/// <summary>
		/// Maintains a mapping between intermediate user directories and the last
		/// known modification time.
		/// </summary>
		private ConcurrentMap<string, HistoryFileManager.UserLogDir> userDirModificationTimeMap
			 = new ConcurrentHashMap<string, HistoryFileManager.UserLogDir>();

		private JobACLsManager aclsMgr;

		[VisibleForTesting]
		internal Configuration conf;

		private string serialNumberFormat;

		private Path doneDirPrefixPath = null;

		private FileContext doneDirFc;

		private Path intermediateDoneDirPath = null;

		private FileContext intermediateDoneDirFc;

		[VisibleForTesting]
		protected internal ThreadPoolExecutor moveToDoneExecutor = null;

		private long maxHistoryAge = 0;

		public HistoryFileManager()
			: base(typeof(HistoryFileManager).FullName)
		{
		}

		// Maintains a list of known done subdirectories.
		// folder for completed jobs
		// done Dir FileContext
		// Intermediate Done Dir Path
		// Intermediate Done Dir
		// FileContext
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.conf = conf;
			int serialNumberLowDigits = 3;
			serialNumberFormat = ("%0" + (JobHistoryUtils.SerialNumberDirectoryDigits + serialNumberLowDigits
				) + "d");
			long maxFSWaitTime = conf.GetLong(JHAdminConfig.MrHistoryMaxStartWaitTime, JHAdminConfig
				.DefaultMrHistoryMaxStartWaitTime);
			CreateHistoryDirs(new SystemClock(), 10 * 1000, maxFSWaitTime);
			this.aclsMgr = new JobACLsManager(conf);
			maxHistoryAge = conf.GetLong(JHAdminConfig.MrHistoryMaxAgeMs, JHAdminConfig.DefaultMrHistoryMaxAge
				);
			jobListCache = CreateJobListCache();
			serialNumberIndex = new HistoryFileManager.SerialNumberIndex(conf.GetInt(JHAdminConfig
				.MrHistoryDatestringCacheSize, JHAdminConfig.DefaultMrHistoryDatestringCacheSize
				));
			int numMoveThreads = conf.GetInt(JHAdminConfig.MrHistoryMoveThreadCount, JHAdminConfig
				.DefaultMrHistoryMoveThreadCount);
			ThreadFactory tf = new ThreadFactoryBuilder().SetNameFormat("MoveIntermediateToDone Thread #%d"
				).Build();
			moveToDoneExecutor = new ThreadPoolExecutor(numMoveThreads, numMoveThreads, 1, TimeUnit
				.Hours, new LinkedBlockingQueue<Runnable>(), tf);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void CreateHistoryDirs(Clock clock, long intervalCheckMillis, long
			 timeOutMillis)
		{
			long start = clock.GetTime();
			bool done = false;
			int counter = 0;
			while (!done && ((timeOutMillis == -1) || (clock.GetTime() - start < timeOutMillis
				)))
			{
				done = TryCreatingHistoryDirs(counter++ % 3 == 0);
				// log every 3 attempts, 30sec
				try
				{
					Sharpen.Thread.Sleep(intervalCheckMillis);
				}
				catch (Exception ex)
				{
					throw new YarnRuntimeException(ex);
				}
			}
			if (!done)
			{
				throw new YarnRuntimeException("Timed out '" + timeOutMillis + "ms' waiting for FileSystem to become available"
					);
			}
		}

		/// <summary>
		/// DistributedFileSystem returns a RemoteException with a message stating
		/// SafeModeException in it.
		/// </summary>
		/// <remarks>
		/// DistributedFileSystem returns a RemoteException with a message stating
		/// SafeModeException in it. So this is only way to check it is because of
		/// being in safe mode.
		/// </remarks>
		private bool IsBecauseSafeMode(Exception ex)
		{
			return ex.ToString().Contains("SafeModeException");
		}

		/// <summary>
		/// Returns TRUE if the history dirs were created, FALSE if they could not
		/// be created because the FileSystem is not reachable or in safe mode and
		/// throws and exception otherwise.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual bool TryCreatingHistoryDirs(bool logWait)
		{
			bool succeeded = true;
			string doneDirPrefix = JobHistoryUtils.GetConfiguredHistoryServerDoneDirPrefix(conf
				);
			try
			{
				doneDirPrefixPath = FileContext.GetFileContext(conf).MakeQualified(new Path(doneDirPrefix
					));
				doneDirFc = FileContext.GetFileContext(doneDirPrefixPath.ToUri(), conf);
				doneDirFc.SetUMask(JobHistoryUtils.HistoryDoneDirUmask);
				Mkdir(doneDirFc, doneDirPrefixPath, new FsPermission(JobHistoryUtils.HistoryDoneDirPermission
					));
			}
			catch (ConnectException)
			{
				if (logWait)
				{
					Log.Info("Waiting for FileSystem at " + doneDirPrefixPath.ToUri().GetAuthority() 
						+ "to be available");
				}
				succeeded = false;
			}
			catch (IOException e)
			{
				if (IsBecauseSafeMode(e))
				{
					succeeded = false;
					if (logWait)
					{
						Log.Info("Waiting for FileSystem at " + doneDirPrefixPath.ToUri().GetAuthority() 
							+ "to be out of safe mode");
					}
				}
				else
				{
					throw new YarnRuntimeException("Error creating done directory: [" + doneDirPrefixPath
						 + "]", e);
				}
			}
			if (succeeded)
			{
				string intermediateDoneDirPrefix = JobHistoryUtils.GetConfiguredHistoryIntermediateDoneDirPrefix
					(conf);
				try
				{
					intermediateDoneDirPath = FileContext.GetFileContext(conf).MakeQualified(new Path
						(intermediateDoneDirPrefix));
					intermediateDoneDirFc = FileContext.GetFileContext(intermediateDoneDirPath.ToUri(
						), conf);
					Mkdir(intermediateDoneDirFc, intermediateDoneDirPath, new FsPermission(JobHistoryUtils
						.HistoryIntermediateDoneDirPermissions.ToShort()));
				}
				catch (ConnectException)
				{
					succeeded = false;
					if (logWait)
					{
						Log.Info("Waiting for FileSystem at " + intermediateDoneDirPath.ToUri().GetAuthority
							() + "to be available");
					}
				}
				catch (IOException e)
				{
					if (IsBecauseSafeMode(e))
					{
						succeeded = false;
						if (logWait)
						{
							Log.Info("Waiting for FileSystem at " + intermediateDoneDirPath.ToUri().GetAuthority
								() + "to be out of safe mode");
						}
					}
					else
					{
						throw new YarnRuntimeException("Error creating intermediate done directory: [" + 
							intermediateDoneDirPath + "]", e);
					}
				}
			}
			return succeeded;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			ShutdownThreadsHelper.ShutdownExecutorService(moveToDoneExecutor);
			base.ServiceStop();
		}

		protected internal virtual HistoryFileManager.JobListCache CreateJobListCache()
		{
			return new HistoryFileManager.JobListCache(conf.GetInt(JHAdminConfig.MrHistoryJoblistCacheSize
				, JHAdminConfig.DefaultMrHistoryJoblistCacheSize), maxHistoryAge);
		}

		/// <exception cref="System.IO.IOException"/>
		private void Mkdir(FileContext fc, Path path, FsPermission fsp)
		{
			if (!fc.Util().Exists(path))
			{
				try
				{
					fc.Mkdir(path, fsp, true);
					FileStatus fsStatus = fc.GetFileStatus(path);
					Log.Info("Perms after creating " + fsStatus.GetPermission().ToShort() + ", Expected: "
						 + fsp.ToShort());
					if (fsStatus.GetPermission().ToShort() != fsp.ToShort())
					{
						Log.Info("Explicitly setting permissions to : " + fsp.ToShort() + ", " + fsp);
						fc.SetPermission(path, fsp);
					}
				}
				catch (FileAlreadyExistsException)
				{
					Log.Info("Directory: [" + path + "] already exists.");
				}
			}
		}

		/// <summary>Populates index data structures.</summary>
		/// <remarks>
		/// Populates index data structures. Should only be called at initialization
		/// times.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void InitExisting()
		{
			Log.Info("Initializing Existing Jobs...");
			IList<FileStatus> timestampedDirList = FindTimestampedDirectories();
			// Sort first just so insertion is in a consistent order
			timestampedDirList.Sort();
			foreach (FileStatus fs in timestampedDirList)
			{
				// TODO Could verify the correct format for these directories.
				AddDirectoryToSerialNumberIndex(fs.GetPath());
			}
			for (int i = timestampedDirList.Count - 1; i >= 0 && !jobListCache.IsFull(); i--)
			{
				FileStatus fs_1 = timestampedDirList[i];
				AddDirectoryToJobListCache(fs_1.GetPath());
			}
		}

		private void RemoveDirectoryFromSerialNumberIndex(Path serialDirPath)
		{
			string serialPart = serialDirPath.GetName();
			string timeStampPart = JobHistoryUtils.GetTimestampPartFromPath(serialDirPath.ToString
				());
			if (timeStampPart == null)
			{
				Log.Warn("Could not find timestamp portion from path: " + serialDirPath.ToString(
					) + ". Continuing with next");
				return;
			}
			if (serialPart == null)
			{
				Log.Warn("Could not find serial portion from path: " + serialDirPath.ToString() +
					 ". Continuing with next");
				return;
			}
			serialNumberIndex.Remove(serialPart, timeStampPart);
		}

		private void AddDirectoryToSerialNumberIndex(Path serialDirPath)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Adding " + serialDirPath + " to serial index");
			}
			string serialPart = serialDirPath.GetName();
			string timestampPart = JobHistoryUtils.GetTimestampPartFromPath(serialDirPath.ToString
				());
			if (timestampPart == null)
			{
				Log.Warn("Could not find timestamp portion from path: " + serialDirPath + ". Continuing with next"
					);
				return;
			}
			if (serialPart == null)
			{
				Log.Warn("Could not find serial portion from path: " + serialDirPath.ToString() +
					 ". Continuing with next");
			}
			else
			{
				serialNumberIndex.Add(serialPart, timestampPart);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void AddDirectoryToJobListCache(Path path)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Adding " + path + " to job list cache.");
			}
			IList<FileStatus> historyFileList = ScanDirectoryForHistoryFiles(path, doneDirFc);
			foreach (FileStatus fs in historyFileList)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Adding in history for " + fs.GetPath());
				}
				JobIndexInfo jobIndexInfo = FileNameIndexUtils.GetIndexInfo(fs.GetPath().GetName(
					));
				string confFileName = JobHistoryUtils.GetIntermediateConfFileName(jobIndexInfo.GetJobId
					());
				string summaryFileName = JobHistoryUtils.GetIntermediateSummaryFileName(jobIndexInfo
					.GetJobId());
				HistoryFileManager.HistoryFileInfo fileInfo = new HistoryFileManager.HistoryFileInfo
					(this, fs.GetPath(), new Path(fs.GetPath().GetParent(), confFileName), new Path(
					fs.GetPath().GetParent(), summaryFileName), jobIndexInfo, true);
				jobListCache.AddIfAbsent(fileInfo);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal static IList<FileStatus> ScanDirectory(Path path, FileContext 
			fc, PathFilter pathFilter)
		{
			path = fc.MakeQualified(path);
			IList<FileStatus> jhStatusList = new AList<FileStatus>();
			try
			{
				RemoteIterator<FileStatus> fileStatusIter = fc.ListStatus(path);
				while (fileStatusIter.HasNext())
				{
					FileStatus fileStatus = fileStatusIter.Next();
					Path filePath = fileStatus.GetPath();
					if (fileStatus.IsFile() && pathFilter.Accept(filePath))
					{
						jhStatusList.AddItem(fileStatus);
					}
				}
			}
			catch (FileNotFoundException fe)
			{
				Log.Error("Error while scanning directory " + path, fe);
			}
			return jhStatusList;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual IList<FileStatus> ScanDirectoryForHistoryFiles(Path path
			, FileContext fc)
		{
			return ScanDirectory(path, fc, JobHistoryUtils.GetHistoryFileFilter());
		}

		/// <summary>
		/// Finds all history directories with a timestamp component by scanning the
		/// filesystem.
		/// </summary>
		/// <remarks>
		/// Finds all history directories with a timestamp component by scanning the
		/// filesystem. Used when the JobHistory server is started.
		/// </remarks>
		/// <returns>list of history directories</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual IList<FileStatus> FindTimestampedDirectories()
		{
			IList<FileStatus> fsList = JobHistoryUtils.LocalGlobber(doneDirFc, doneDirPrefixPath
				, DoneBeforeSerialTail);
			return fsList;
		}

		/// <summary>Scans the intermediate directory to find user directories.</summary>
		/// <remarks>
		/// Scans the intermediate directory to find user directories. Scans these for
		/// history files if the modification time for the directory has changed. Once
		/// it finds history files it starts the process of moving them to the done
		/// directory.
		/// </remarks>
		/// <exception cref="System.IO.IOException">if there was a error while scanning</exception>
		internal virtual void ScanIntermediateDirectory()
		{
			// TODO it would be great to limit how often this happens, except in the
			// case where we are looking for a particular job.
			IList<FileStatus> userDirList = JobHistoryUtils.LocalGlobber(intermediateDoneDirFc
				, intermediateDoneDirPath, string.Empty);
			Log.Debug("Scanning intermediate dirs");
			foreach (FileStatus userDir in userDirList)
			{
				string name = userDir.GetPath().GetName();
				HistoryFileManager.UserLogDir dir = userDirModificationTimeMap[name];
				if (dir == null)
				{
					dir = new HistoryFileManager.UserLogDir(this);
					HistoryFileManager.UserLogDir old = userDirModificationTimeMap.PutIfAbsent(name, 
						dir);
					if (old != null)
					{
						dir = old;
					}
				}
				dir.ScanIfNeeded(userDir);
			}
		}

		/// <summary>Scans the specified path and populates the intermediate cache.</summary>
		/// <param name="absPath"/>
		/// <exception cref="System.IO.IOException"/>
		private void ScanIntermediateDirectory(Path absPath)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Scanning intermediate dir " + absPath);
			}
			IList<FileStatus> fileStatusList = ScanDirectoryForHistoryFiles(absPath, intermediateDoneDirFc
				);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Found " + fileStatusList.Count + " files");
			}
			foreach (FileStatus fs in fileStatusList)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("scanning file: " + fs.GetPath());
				}
				JobIndexInfo jobIndexInfo = FileNameIndexUtils.GetIndexInfo(fs.GetPath().GetName(
					));
				string confFileName = JobHistoryUtils.GetIntermediateConfFileName(jobIndexInfo.GetJobId
					());
				string summaryFileName = JobHistoryUtils.GetIntermediateSummaryFileName(jobIndexInfo
					.GetJobId());
				HistoryFileManager.HistoryFileInfo fileInfo = new HistoryFileManager.HistoryFileInfo
					(this, fs.GetPath(), new Path(fs.GetPath().GetParent(), confFileName), new Path(
					fs.GetPath().GetParent(), summaryFileName), jobIndexInfo, false);
				HistoryFileManager.HistoryFileInfo old = jobListCache.AddIfAbsent(fileInfo);
				if (old == null || old.DidMoveFail())
				{
					HistoryFileManager.HistoryFileInfo found = (old == null) ? fileInfo : old;
					long cutoff = Runtime.CurrentTimeMillis() - maxHistoryAge;
					if (found.GetJobIndexInfo().GetFinishTime() <= cutoff)
					{
						try
						{
							found.Delete();
						}
						catch (IOException e)
						{
							Log.Warn("Error cleaning up a HistoryFile that is out of date.", e);
						}
					}
					else
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Scheduling move to done of " + found);
						}
						moveToDoneExecutor.Execute(new _Runnable_855(found));
					}
				}
				else
				{
					if (!old.IsMovePending())
					{
						//This is a duplicate so just delete it
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Duplicate: deleting");
						}
						fileInfo.Delete();
					}
				}
			}
		}

		private sealed class _Runnable_855 : Runnable
		{
			public _Runnable_855(HistoryFileManager.HistoryFileInfo found)
			{
				this.found = found;
			}

			public void Run()
			{
				try
				{
					found.MoveToDone();
				}
				catch (IOException e)
				{
					HistoryFileManager.Log.Info("Failed to process fileInfo for job: " + found.GetJobId
						(), e);
				}
			}

			private readonly HistoryFileManager.HistoryFileInfo found;
		}

		/// <summary>Searches the job history file FileStatus list for the specified JobId.</summary>
		/// <param name="fileStatusList">fileStatus list of Job History Files.</param>
		/// <param name="jobId">The JobId to find.</param>
		/// <returns>A FileInfo object for the jobId, null if not found.</returns>
		/// <exception cref="System.IO.IOException"/>
		private HistoryFileManager.HistoryFileInfo GetJobFileInfo(IList<FileStatus> fileStatusList
			, JobId jobId)
		{
			foreach (FileStatus fs in fileStatusList)
			{
				JobIndexInfo jobIndexInfo = FileNameIndexUtils.GetIndexInfo(fs.GetPath().GetName(
					));
				if (jobIndexInfo.GetJobId().Equals(jobId))
				{
					string confFileName = JobHistoryUtils.GetIntermediateConfFileName(jobIndexInfo.GetJobId
						());
					string summaryFileName = JobHistoryUtils.GetIntermediateSummaryFileName(jobIndexInfo
						.GetJobId());
					HistoryFileManager.HistoryFileInfo fileInfo = new HistoryFileManager.HistoryFileInfo
						(this, fs.GetPath(), new Path(fs.GetPath().GetParent(), confFileName), new Path(
						fs.GetPath().GetParent(), summaryFileName), jobIndexInfo, true);
					return fileInfo;
				}
			}
			return null;
		}

		/// <summary>
		/// Scans old directories known by the idToDateString map for the specified
		/// jobId.
		/// </summary>
		/// <remarks>
		/// Scans old directories known by the idToDateString map for the specified
		/// jobId. If the number of directories is higher than the supported size of
		/// the idToDateString cache, the jobId will not be found.
		/// </remarks>
		/// <param name="jobId">the jobId.</param>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		private HistoryFileManager.HistoryFileInfo ScanOldDirsForJob(JobId jobId)
		{
			string boxedSerialNumber = JobHistoryUtils.SerialNumberDirectoryComponent(jobId, 
				serialNumberFormat);
			ICollection<string> dateStringSet = serialNumberIndex.Get(boxedSerialNumber);
			if (dateStringSet == null)
			{
				return null;
			}
			foreach (string timestampPart in dateStringSet)
			{
				Path logDir = CanonicalHistoryLogPath(jobId, timestampPart);
				IList<FileStatus> fileStatusList = ScanDirectoryForHistoryFiles(logDir, doneDirFc
					);
				HistoryFileManager.HistoryFileInfo fileInfo = GetJobFileInfo(fileStatusList, jobId
					);
				if (fileInfo != null)
				{
					return fileInfo;
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ICollection<HistoryFileManager.HistoryFileInfo> GetAllFileInfo()
		{
			ScanIntermediateDirectory();
			return jobListCache.Values();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual HistoryFileManager.HistoryFileInfo GetFileInfo(JobId jobId)
		{
			// FileInfo available in cache.
			HistoryFileManager.HistoryFileInfo fileInfo = jobListCache.Get(jobId);
			if (fileInfo != null)
			{
				return fileInfo;
			}
			// OK so scan the intermediate to be sure we did not lose it that way
			ScanIntermediateDirectory();
			fileInfo = jobListCache.Get(jobId);
			if (fileInfo != null)
			{
				return fileInfo;
			}
			// Intermediate directory does not contain job. Search through older ones.
			fileInfo = ScanOldDirsForJob(jobId);
			if (fileInfo != null)
			{
				return fileInfo;
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		private void MoveToDoneNow(Path src, Path target)
		{
			Log.Info("Moving " + src.ToString() + " to " + target.ToString());
			intermediateDoneDirFc.Rename(src, target, Options.Rename.None);
		}

		/// <exception cref="System.IO.IOException"/>
		private string GetJobSummary(FileContext fc, Path path)
		{
			Path qPath = fc.MakeQualified(path);
			FSDataInputStream @in = null;
			string jobSummaryString = null;
			try
			{
				@in = fc.Open(qPath);
				jobSummaryString = @in.ReadUTF();
			}
			finally
			{
				if (@in != null)
				{
					@in.Close();
				}
			}
			return jobSummaryString;
		}

		/// <exception cref="System.IO.IOException"/>
		private void MakeDoneSubdir(Path path)
		{
			try
			{
				doneDirFc.GetFileStatus(path);
				existingDoneSubdirs.AddItem(path);
			}
			catch (FileNotFoundException)
			{
				try
				{
					FsPermission fsp = new FsPermission(JobHistoryUtils.HistoryDoneDirPermission);
					doneDirFc.Mkdir(path, fsp, true);
					FileStatus fsStatus = doneDirFc.GetFileStatus(path);
					Log.Info("Perms after creating " + fsStatus.GetPermission().ToShort() + ", Expected: "
						 + fsp.ToShort());
					if (fsStatus.GetPermission().ToShort() != fsp.ToShort())
					{
						Log.Info("Explicitly setting permissions to : " + fsp.ToShort() + ", " + fsp);
						doneDirFc.SetPermission(path, fsp);
					}
					existingDoneSubdirs.AddItem(path);
				}
				catch (FileAlreadyExistsException)
				{
				}
			}
		}

		// Nothing to do.
		private Path CanonicalHistoryLogPath(JobId id, string timestampComponent)
		{
			return new Path(doneDirPrefixPath, JobHistoryUtils.HistoryLogSubdirectory(id, timestampComponent
				, serialNumberFormat));
		}

		private Path CanonicalHistoryLogPath(JobId id, long millisecondTime)
		{
			string timestampComponent = JobHistoryUtils.TimestampDirectoryComponent(millisecondTime
				);
			return new Path(doneDirPrefixPath, JobHistoryUtils.HistoryLogSubdirectory(id, timestampComponent
				, serialNumberFormat));
		}

		private long GetEffectiveTimestamp(long finishTime, FileStatus fileStatus)
		{
			if (finishTime == 0)
			{
				return fileStatus.GetModificationTime();
			}
			return finishTime;
		}

		/// <exception cref="System.IO.IOException"/>
		private void DeleteJobFromDone(HistoryFileManager.HistoryFileInfo fileInfo)
		{
			jobListCache.Delete(fileInfo);
			fileInfo.Delete();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual IList<FileStatus> GetHistoryDirsForCleaning(long cutoff)
		{
			return JobHistoryUtils.GetHistoryDirsForCleaning(doneDirFc, doneDirPrefixPath, cutoff
				);
		}

		/// <summary>Clean up older history files.</summary>
		/// <exception cref="System.IO.IOException">on any error trying to remove the entries.
		/// 	</exception>
		internal virtual void Clean()
		{
			long cutoff = Runtime.CurrentTimeMillis() - maxHistoryAge;
			bool halted = false;
			IList<FileStatus> serialDirList = GetHistoryDirsForCleaning(cutoff);
			// Sort in ascending order. Relies on YYYY/MM/DD/Serial
			serialDirList.Sort();
			foreach (FileStatus serialDir in serialDirList)
			{
				IList<FileStatus> historyFileList = ScanDirectoryForHistoryFiles(serialDir.GetPath
					(), doneDirFc);
				foreach (FileStatus historyFile in historyFileList)
				{
					JobIndexInfo jobIndexInfo = FileNameIndexUtils.GetIndexInfo(historyFile.GetPath()
						.GetName());
					long effectiveTimestamp = GetEffectiveTimestamp(jobIndexInfo.GetFinishTime(), historyFile
						);
					if (effectiveTimestamp <= cutoff)
					{
						HistoryFileManager.HistoryFileInfo fileInfo = this.jobListCache.Get(jobIndexInfo.
							GetJobId());
						if (fileInfo == null)
						{
							string confFileName = JobHistoryUtils.GetIntermediateConfFileName(jobIndexInfo.GetJobId
								());
							fileInfo = new HistoryFileManager.HistoryFileInfo(this, historyFile.GetPath(), new 
								Path(historyFile.GetPath().GetParent(), confFileName), null, jobIndexInfo, true);
						}
						DeleteJobFromDone(fileInfo);
					}
					else
					{
						halted = true;
						break;
					}
				}
				if (!halted)
				{
					DeleteDir(serialDir);
					RemoveDirectoryFromSerialNumberIndex(serialDir.GetPath());
					existingDoneSubdirs.Remove(serialDir.GetPath());
				}
				else
				{
					break;
				}
			}
		}

		// Don't scan any more directories.
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual bool DeleteDir(FileStatus serialDir)
		{
			return doneDirFc.Delete(doneDirFc.MakeQualified(serialDir.GetPath()), true);
		}

		[VisibleForTesting]
		protected internal virtual void SetMaxHistoryAge(long newValue)
		{
			maxHistoryAge = newValue;
		}
	}
}
