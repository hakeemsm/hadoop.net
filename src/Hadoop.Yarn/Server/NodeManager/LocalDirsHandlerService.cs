using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	/// <summary>
	/// The class which provides functionality of checking the health of the local
	/// directories of a node.
	/// </summary>
	/// <remarks>
	/// The class which provides functionality of checking the health of the local
	/// directories of a node. This specifically manages nodemanager-local-dirs and
	/// nodemanager-log-dirs by periodically checking their health.
	/// </remarks>
	public class LocalDirsHandlerService : AbstractService
	{
		private static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.LocalDirsHandlerService
			));

		/// <summary>Timer used to schedule disk health monitoring code execution</summary>
		private Timer dirsHandlerScheduler;

		private long diskHealthCheckInterval;

		private bool isDiskHealthCheckerEnabled;

		/// <summary>
		/// Minimum fraction of disks to be healthy for the node to be healthy in
		/// terms of disks.
		/// </summary>
		/// <remarks>
		/// Minimum fraction of disks to be healthy for the node to be healthy in
		/// terms of disks. This applies to nm-local-dirs and nm-log-dirs.
		/// </remarks>
		private float minNeededHealthyDisksFactor;

		private LocalDirsHandlerService.MonitoringTimerTask monitoringTimerTask;

		/// <summary>Local dirs to store localized files in</summary>
		private DirectoryCollection localDirs = null;

		/// <summary>storage for container logs</summary>
		private DirectoryCollection logDirs = null;

		/// <summary>
		/// Everybody should go through this LocalDirAllocator object for read/write
		/// of any local path corresponding to
		/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.NmLocalDirs"/>
		/// instead of creating his/her own LocalDirAllocator objects
		/// </summary>
		private LocalDirAllocator localDirsAllocator;

		/// <summary>
		/// Everybody should go through this LocalDirAllocator object for read/write
		/// of any local path corresponding to
		/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.NmLogDirs"/>
		/// instead of creating his/her own LocalDirAllocator objects
		/// </summary>
		private LocalDirAllocator logDirsAllocator;

		/// <summary>when disk health checking code was last run</summary>
		private long lastDisksCheckTime;

		private static string FileScheme = "file";

		/// <summary>
		/// Class which is used by the
		/// <see cref="Sharpen.Timer"/>
		/// class to periodically execute the
		/// disks' health checker code.
		/// </summary>
		private sealed class MonitoringTimerTask : TimerTask
		{
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnRuntimeException"/>
			public MonitoringTimerTask(LocalDirsHandlerService _enclosing, Configuration conf
				)
			{
				this._enclosing = _enclosing;
				float maxUsableSpacePercentagePerDisk = conf.GetFloat(YarnConfiguration.NmMaxPerDiskUtilizationPercentage
					, YarnConfiguration.DefaultNmMaxPerDiskUtilizationPercentage);
				long minFreeSpacePerDiskMB = conf.GetLong(YarnConfiguration.NmMinPerDiskFreeSpaceMb
					, YarnConfiguration.DefaultNmMinPerDiskFreeSpaceMb);
				this._enclosing.localDirs = new DirectoryCollection(LocalDirsHandlerService.ValidatePaths
					(conf.GetTrimmedStrings(YarnConfiguration.NmLocalDirs)), maxUsableSpacePercentagePerDisk
					, minFreeSpacePerDiskMB);
				this._enclosing.logDirs = new DirectoryCollection(LocalDirsHandlerService.ValidatePaths
					(conf.GetTrimmedStrings(YarnConfiguration.NmLogDirs)), maxUsableSpacePercentagePerDisk
					, minFreeSpacePerDiskMB);
				this._enclosing.localDirsAllocator = new LocalDirAllocator(YarnConfiguration.NmLocalDirs
					);
				this._enclosing.logDirsAllocator = new LocalDirAllocator(YarnConfiguration.NmLogDirs
					);
			}

			public override void Run()
			{
				this._enclosing.CheckDirs();
			}

			private readonly LocalDirsHandlerService _enclosing;
		}

		public LocalDirsHandlerService()
			: base(typeof(LocalDirsHandlerService).FullName)
		{
		}

		/// <summary>Method which initializes the timertask and its interval time.</summary>
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration config)
		{
			// Clone the configuration as we may do modifications to dirs-list
			Configuration conf = new Configuration(config);
			diskHealthCheckInterval = conf.GetLong(YarnConfiguration.NmDiskHealthCheckIntervalMs
				, YarnConfiguration.DefaultNmDiskHealthCheckIntervalMs);
			monitoringTimerTask = new LocalDirsHandlerService.MonitoringTimerTask(this, conf);
			isDiskHealthCheckerEnabled = conf.GetBoolean(YarnConfiguration.NmDiskHealthCheckEnable
				, true);
			minNeededHealthyDisksFactor = conf.GetFloat(YarnConfiguration.NmMinHealthyDisksFraction
				, YarnConfiguration.DefaultNmMinHealthyDisksFraction);
			lastDisksCheckTime = Runtime.CurrentTimeMillis();
			base.ServiceInit(conf);
			FileContext localFs;
			try
			{
				localFs = FileContext.GetLocalFSFileContext(config);
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException("Unable to get the local filesystem", e);
			}
			FsPermission perm = new FsPermission((short)0x1ed);
			bool createSucceeded = localDirs.CreateNonExistentDirs(localFs, perm);
			createSucceeded &= logDirs.CreateNonExistentDirs(localFs, perm);
			if (!createSucceeded)
			{
				UpdateDirsAfterTest();
			}
			// Check the disk health immediately to weed out bad directories
			// before other init code attempts to use them.
			CheckDirs();
		}

		/// <summary>Method used to start the disk health monitoring, if enabled.</summary>
		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			if (isDiskHealthCheckerEnabled)
			{
				dirsHandlerScheduler = new Timer("DiskHealthMonitor-Timer", true);
				dirsHandlerScheduler.ScheduleAtFixedRate(monitoringTimerTask, diskHealthCheckInterval
					, diskHealthCheckInterval);
			}
			base.ServiceStart();
		}

		/// <summary>Method used to terminate the disk health monitoring service.</summary>
		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (dirsHandlerScheduler != null)
			{
				dirsHandlerScheduler.Cancel();
			}
			base.ServiceStop();
		}

		/// <returns>the good/valid local directories based on disks' health</returns>
		public virtual IList<string> GetLocalDirs()
		{
			return localDirs.GetGoodDirs();
		}

		/// <returns>the good/valid log directories based on disks' health</returns>
		public virtual IList<string> GetLogDirs()
		{
			return logDirs.GetGoodDirs();
		}

		/// <returns>the local directories which have no disk space</returns>
		public virtual IList<string> GetDiskFullLocalDirs()
		{
			return localDirs.GetFullDirs();
		}

		/// <returns>the log directories that have no disk space</returns>
		public virtual IList<string> GetDiskFullLogDirs()
		{
			return logDirs.GetFullDirs();
		}

		/// <summary>
		/// Function to get the local dirs which should be considered for reading
		/// existing files on disk.
		/// </summary>
		/// <remarks>
		/// Function to get the local dirs which should be considered for reading
		/// existing files on disk. Contains the good local dirs and the local dirs
		/// that have reached the disk space limit
		/// </remarks>
		/// <returns>the local dirs which should be considered for reading</returns>
		public virtual IList<string> GetLocalDirsForRead()
		{
			return DirectoryCollection.Concat(localDirs.GetGoodDirs(), localDirs.GetFullDirs(
				));
		}

		/// <summary>
		/// Function to get the local dirs which should be considered when cleaning up
		/// resources.
		/// </summary>
		/// <remarks>
		/// Function to get the local dirs which should be considered when cleaning up
		/// resources. Contains the good local dirs and the local dirs that have reached
		/// the disk space limit
		/// </remarks>
		/// <returns>the local dirs which should be considered for cleaning up</returns>
		public virtual IList<string> GetLocalDirsForCleanup()
		{
			return DirectoryCollection.Concat(localDirs.GetGoodDirs(), localDirs.GetFullDirs(
				));
		}

		/// <summary>
		/// Function to get the log dirs which should be considered for reading
		/// existing files on disk.
		/// </summary>
		/// <remarks>
		/// Function to get the log dirs which should be considered for reading
		/// existing files on disk. Contains the good log dirs and the log dirs that
		/// have reached the disk space limit
		/// </remarks>
		/// <returns>the log dirs which should be considered for reading</returns>
		public virtual IList<string> GetLogDirsForRead()
		{
			return DirectoryCollection.Concat(logDirs.GetGoodDirs(), logDirs.GetFullDirs());
		}

		/// <summary>
		/// Function to get the log dirs which should be considered when cleaning up
		/// resources.
		/// </summary>
		/// <remarks>
		/// Function to get the log dirs which should be considered when cleaning up
		/// resources. Contains the good log dirs and the log dirs that have reached
		/// the disk space limit
		/// </remarks>
		/// <returns>the log dirs which should be considered for cleaning up</returns>
		public virtual IList<string> GetLogDirsForCleanup()
		{
			return DirectoryCollection.Concat(logDirs.GetGoodDirs(), logDirs.GetFullDirs());
		}

		/// <summary>Function to generate a report on the state of the disks.</summary>
		/// <param name="listGoodDirs">
		/// flag to determine whether the report should report the state of
		/// good dirs or failed dirs
		/// </param>
		/// <returns>the health report of nm-local-dirs and nm-log-dirs</returns>
		public virtual string GetDisksHealthReport(bool listGoodDirs)
		{
			if (!isDiskHealthCheckerEnabled)
			{
				return string.Empty;
			}
			StringBuilder report = new StringBuilder();
			IList<string> failedLocalDirsList = localDirs.GetFailedDirs();
			IList<string> failedLogDirsList = logDirs.GetFailedDirs();
			IList<string> goodLocalDirsList = localDirs.GetGoodDirs();
			IList<string> goodLogDirsList = logDirs.GetGoodDirs();
			int numLocalDirs = goodLocalDirsList.Count + failedLocalDirsList.Count;
			int numLogDirs = goodLogDirsList.Count + failedLogDirsList.Count;
			if (!listGoodDirs)
			{
				if (!failedLocalDirsList.IsEmpty())
				{
					report.Append(failedLocalDirsList.Count + "/" + numLocalDirs + " local-dirs are bad: "
						 + StringUtils.Join(",", failedLocalDirsList) + "; ");
				}
				if (!failedLogDirsList.IsEmpty())
				{
					report.Append(failedLogDirsList.Count + "/" + numLogDirs + " log-dirs are bad: " 
						+ StringUtils.Join(",", failedLogDirsList));
				}
			}
			else
			{
				report.Append(goodLocalDirsList.Count + "/" + numLocalDirs + " local-dirs are good: "
					 + StringUtils.Join(",", goodLocalDirsList) + "; ");
				report.Append(goodLogDirsList.Count + "/" + numLogDirs + " log-dirs are good: " +
					 StringUtils.Join(",", goodLogDirsList));
			}
			return report.ToString();
		}

		/// <summary>
		/// The minimum fraction of number of disks needed to be healthy for a node to
		/// be considered healthy in terms of disks is configured using
		/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.NmMinHealthyDisksFraction
		/// 	"/>
		/// , with a default
		/// value of
		/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.DefaultNmMinHealthyDisksFraction
		/// 	"/>
		/// .
		/// </summary>
		/// <returns>
		/// <em>false</em> if either (a) more than the allowed percentage of
		/// nm-local-dirs failed or (b) more than the allowed percentage of
		/// nm-log-dirs failed.
		/// </returns>
		public virtual bool AreDisksHealthy()
		{
			if (!isDiskHealthCheckerEnabled)
			{
				return true;
			}
			int goodDirs = GetLocalDirs().Count;
			int failedDirs = localDirs.GetFailedDirs().Count;
			int totalConfiguredDirs = goodDirs + failedDirs;
			if (goodDirs / (float)totalConfiguredDirs < minNeededHealthyDisksFactor)
			{
				return false;
			}
			// Not enough healthy local-dirs
			goodDirs = GetLogDirs().Count;
			failedDirs = logDirs.GetFailedDirs().Count;
			totalConfiguredDirs = goodDirs + failedDirs;
			if (goodDirs / (float)totalConfiguredDirs < minNeededHealthyDisksFactor)
			{
				return false;
			}
			// Not enough healthy log-dirs
			return true;
		}

		public virtual long GetLastDisksCheckTime()
		{
			return lastDisksCheckTime;
		}

		/// <summary>
		/// Set good local dirs and good log dirs in the configuration so that the
		/// LocalDirAllocator objects will use this updated configuration only.
		/// </summary>
		private void UpdateDirsAfterTest()
		{
			Configuration conf = GetConfig();
			IList<string> localDirs = GetLocalDirs();
			conf.SetStrings(YarnConfiguration.NmLocalDirs, Sharpen.Collections.ToArray(localDirs
				, new string[localDirs.Count]));
			IList<string> logDirs = GetLogDirs();
			conf.SetStrings(YarnConfiguration.NmLogDirs, Sharpen.Collections.ToArray(logDirs, 
				new string[logDirs.Count]));
			if (!AreDisksHealthy())
			{
				// Just log.
				Log.Error("Most of the disks failed. " + GetDisksHealthReport(false));
			}
		}

		private void LogDiskStatus(bool newDiskFailure, bool diskTurnedGood)
		{
			if (newDiskFailure)
			{
				string report = GetDisksHealthReport(false);
				Log.Info("Disk(s) failed: " + report);
			}
			if (diskTurnedGood)
			{
				string report = GetDisksHealthReport(true);
				Log.Info("Disk(s) turned good: " + report);
			}
		}

		private void CheckDirs()
		{
			bool disksStatusChange = false;
			ICollection<string> failedLocalDirsPreCheck = new HashSet<string>(localDirs.GetFailedDirs
				());
			ICollection<string> failedLogDirsPreCheck = new HashSet<string>(logDirs.GetFailedDirs
				());
			if (localDirs.CheckDirs())
			{
				disksStatusChange = true;
			}
			if (logDirs.CheckDirs())
			{
				disksStatusChange = true;
			}
			ICollection<string> failedLocalDirsPostCheck = new HashSet<string>(localDirs.GetFailedDirs
				());
			ICollection<string> failedLogDirsPostCheck = new HashSet<string>(logDirs.GetFailedDirs
				());
			bool disksFailed = false;
			bool disksTurnedGood = false;
			disksFailed = DisksTurnedBad(failedLocalDirsPreCheck, failedLocalDirsPostCheck);
			disksTurnedGood = DisksTurnedGood(failedLocalDirsPreCheck, failedLocalDirsPostCheck
				);
			// skip check if we have new failed or good local dirs since we're going to
			// log anyway
			if (!disksFailed)
			{
				disksFailed = DisksTurnedBad(failedLogDirsPreCheck, failedLogDirsPostCheck);
			}
			if (!disksTurnedGood)
			{
				disksTurnedGood = DisksTurnedGood(failedLogDirsPreCheck, failedLogDirsPostCheck);
			}
			LogDiskStatus(disksFailed, disksTurnedGood);
			if (disksStatusChange)
			{
				UpdateDirsAfterTest();
			}
			lastDisksCheckTime = Runtime.CurrentTimeMillis();
		}

		private bool DisksTurnedBad(ICollection<string> preCheckFailedDirs, ICollection<string
			> postCheckDirs)
		{
			bool disksFailed = false;
			foreach (string dir in postCheckDirs)
			{
				if (!preCheckFailedDirs.Contains(dir))
				{
					disksFailed = true;
					break;
				}
			}
			return disksFailed;
		}

		private bool DisksTurnedGood(ICollection<string> preCheckDirs, ICollection<string
			> postCheckDirs)
		{
			bool disksTurnedGood = false;
			foreach (string dir in preCheckDirs)
			{
				if (!postCheckDirs.Contains(dir))
				{
					disksTurnedGood = true;
					break;
				}
			}
			return disksTurnedGood;
		}

		/// <exception cref="System.IO.IOException"/>
		private Path GetPathToRead(string pathStr, IList<string> dirs)
		{
			// remove the leading slash from the path (to make sure that the uri
			// resolution results in a valid path on the dir being checked)
			if (pathStr.StartsWith("/"))
			{
				pathStr = Sharpen.Runtime.Substring(pathStr, 1);
			}
			FileSystem localFS = FileSystem.GetLocal(GetConfig());
			foreach (string dir in dirs)
			{
				try
				{
					Path tmpDir = new Path(dir);
					FilePath tmpFile = tmpDir.IsAbsolute() ? new FilePath(localFS.MakeQualified(tmpDir
						).ToUri()) : new FilePath(dir);
					Path file = new Path(tmpFile.GetPath(), pathStr);
					if (localFS.Exists(file))
					{
						return file;
					}
				}
				catch (IOException ie)
				{
					// ignore
					Log.Warn("Failed to find " + pathStr + " at " + dir, ie);
				}
			}
			throw new IOException("Could not find " + pathStr + " in any of" + " the directories"
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetLocalPathForWrite(string pathStr)
		{
			return localDirsAllocator.GetLocalPathForWrite(pathStr, GetConfig());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetLocalPathForWrite(string pathStr, long size, bool checkWrite
			)
		{
			return localDirsAllocator.GetLocalPathForWrite(pathStr, size, GetConfig(), checkWrite
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetLogPathForWrite(string pathStr, bool checkWrite)
		{
			return logDirsAllocator.GetLocalPathForWrite(pathStr, LocalDirAllocator.SizeUnknown
				, GetConfig(), checkWrite);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetLogPathToRead(string pathStr)
		{
			return GetPathToRead(pathStr, GetLogDirsForRead());
		}

		public static string[] ValidatePaths(string[] paths)
		{
			AList<string> validPaths = new AList<string>();
			for (int i = 0; i < paths.Length; ++i)
			{
				try
				{
					URI uriPath = (new Path(paths[i])).ToUri();
					if (uriPath.GetScheme() == null || uriPath.GetScheme().Equals(FileScheme))
					{
						validPaths.AddItem(new Path(uriPath.GetPath()).ToString());
					}
					else
					{
						Log.Warn(paths[i] + " is not a valid path. Path should be with " + FileScheme + " scheme or without scheme"
							);
						throw new YarnRuntimeException(paths[i] + " is not a valid path. Path should be with "
							 + FileScheme + " scheme or without scheme");
					}
				}
				catch (ArgumentException e)
				{
					Log.Warn(e.Message);
					throw new YarnRuntimeException(paths[i] + " is not a valid path. Path should be with "
						 + FileScheme + " scheme or without scheme");
				}
			}
			string[] arrValidPaths = new string[validPaths.Count];
			Sharpen.Collections.ToArray(validPaths, arrValidPaths);
			return arrValidPaths;
		}
	}
}
