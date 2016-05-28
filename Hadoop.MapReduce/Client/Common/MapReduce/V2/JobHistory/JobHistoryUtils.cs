using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Jobhistory
{
	public class JobHistoryUtils
	{
		/// <summary>Permissions for the history staging dir while JobInProgress.</summary>
		public static readonly FsPermission HistoryStagingDirPermissions = FsPermission.CreateImmutable
			((short)0x1c0);

		/// <summary>Permissions for the user directory under the staging directory.</summary>
		public static readonly FsPermission HistoryStagingUserDirPermissions = FsPermission
			.CreateImmutable((short)0x1c0);

		/// <summary>Permissions for the history done dir and derivatives.</summary>
		public static readonly FsPermission HistoryDoneDirPermission = FsPermission.CreateImmutable
			((short)0x1f8);

		public static readonly FsPermission HistoryDoneFilePermission = FsPermission.CreateImmutable
			((short)0x1f8);

		/// <summary>Umask for the done dir and derivatives.</summary>
		public static readonly FsPermission HistoryDoneDirUmask = FsPermission.CreateImmutable
			((short)(0x1f8 ^ 0x1ff));

		/// <summary>Permissions for the intermediate done directory.</summary>
		public static readonly FsPermission HistoryIntermediateDoneDirPermissions = FsPermission
			.CreateImmutable((short)0x3ff);

		/// <summary>Permissions for the user directory under the intermediate done directory.
		/// 	</summary>
		public static readonly FsPermission HistoryIntermediateUserDirPermissions = FsPermission
			.CreateImmutable((short)0x1f8);

		public static readonly FsPermission HistoryIntermediateFilePermissions = FsPermission
			.CreateImmutable((short)0x1f8);

		/// <summary>Suffix for configuration files.</summary>
		public const string ConfFileNameSuffix = "_conf.xml";

		/// <summary>Suffix for summary files.</summary>
		public const string SummaryFileNameSuffix = ".summary";

		/// <summary>Job History File extension.</summary>
		public const string JobHistoryFileExtension = ".jhist";

		public const int Version = 4;

		public const int SerialNumberDirectoryDigits = 6;

		public const string TimestampDirRegex = "\\d{4}" + "\\" + Path.Separator + "\\d{2}"
			 + "\\" + Path.Separator + "\\d{2}";

		public static readonly Sharpen.Pattern TimestampDirPattern = Sharpen.Pattern.Compile
			(TimestampDirRegex);

		private static readonly string TimestampDirFormat = "%04d" + FilePath.separator +
			 "%02d" + FilePath.separator + "%02d";

		private static readonly Log Log = LogFactory.GetLog(typeof(JobHistoryUtils));

		private sealed class _PathFilter_126 : PathFilter
		{
			public _PathFilter_126()
			{
			}

			// rwx------
			// rwx------
			public bool Accept(Path path)
			{
				return path.GetName().EndsWith(JobHistoryUtils.ConfFileNameSuffix);
			}
		}

		private static readonly PathFilter ConfFilter = new _PathFilter_126();

		private sealed class _PathFilter_133 : PathFilter
		{
			public _PathFilter_133()
			{
			}

			public bool Accept(Path path)
			{
				return path.GetName().EndsWith(JobHistoryUtils.JobHistoryFileExtension);
			}
		}

		private static readonly PathFilter JobHistoryFileFilter = new _PathFilter_133();

		/// <summary>Checks whether the provided path string is a valid job history file.</summary>
		/// <param name="pathString">the path to be checked.</param>
		/// <returns>true is the path is a valid job history filename else return false</returns>
		public static bool IsValidJobHistoryFileName(string pathString)
		{
			return pathString.EndsWith(JobHistoryFileExtension);
		}

		/// <summary>Returns the jobId from a job history file name.</summary>
		/// <param name="pathString">the path string.</param>
		/// <returns>the JobId</returns>
		/// <exception cref="System.IO.IOException">if the filename format is invalid.</exception>
		public static JobID GetJobIDFromHistoryFilePath(string pathString)
		{
			string[] parts = pathString.Split(Path.Separator);
			string fileNamePart = parts[parts.Length - 1];
			JobIndexInfo jobIndexInfo = FileNameIndexUtils.GetIndexInfo(fileNamePart);
			return TypeConverter.FromYarn(jobIndexInfo.GetJobId());
		}

		/// <summary>Gets a PathFilter which would match configuration files.</summary>
		/// <returns>
		/// the patch filter
		/// <see cref="Org.Apache.Hadoop.FS.PathFilter"/>
		/// for matching conf files.
		/// </returns>
		public static PathFilter GetConfFileFilter()
		{
			return ConfFilter;
		}

		/// <summary>Gets a PathFilter which would match job history file names.</summary>
		/// <returns>
		/// the path filter
		/// <see cref="Org.Apache.Hadoop.FS.PathFilter"/>
		/// matching job history files.
		/// </returns>
		public static PathFilter GetHistoryFileFilter()
		{
			return JobHistoryFileFilter;
		}

		/// <summary>Gets the configured directory prefix for In Progress history files.</summary>
		/// <param name="conf">the configuration for hte job</param>
		/// <param name="jobId">the id of the job the history file is for.</param>
		/// <returns>A string representation of the prefix.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string GetConfiguredHistoryStagingDirPrefix(Configuration conf, string
			 jobId)
		{
			string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
			Path stagingPath = MRApps.GetStagingAreaDir(conf, user);
			Path path = new Path(stagingPath, jobId);
			string logDir = path.ToString();
			return EnsurePathInDefaultFileSystem(logDir, conf);
		}

		/// <summary>Gets the configured directory prefix for intermediate done history files.
		/// 	</summary>
		/// <param name="conf"/>
		/// <returns>A string representation of the prefix.</returns>
		public static string GetConfiguredHistoryIntermediateDoneDirPrefix(Configuration 
			conf)
		{
			string doneDirPrefix = conf.Get(JHAdminConfig.MrHistoryIntermediateDoneDir);
			if (doneDirPrefix == null)
			{
				doneDirPrefix = conf.Get(MRJobConfig.MrAmStagingDir, MRJobConfig.DefaultMrAmStagingDir
					) + "/history/done_intermediate";
			}
			return EnsurePathInDefaultFileSystem(doneDirPrefix, conf);
		}

		/// <summary>Gets the configured directory prefix for Done history files.</summary>
		/// <param name="conf">the configuration object</param>
		/// <returns>the done history directory</returns>
		public static string GetConfiguredHistoryServerDoneDirPrefix(Configuration conf)
		{
			string doneDirPrefix = conf.Get(JHAdminConfig.MrHistoryDoneDir);
			if (doneDirPrefix == null)
			{
				doneDirPrefix = conf.Get(MRJobConfig.MrAmStagingDir, MRJobConfig.DefaultMrAmStagingDir
					) + "/history/done";
			}
			return EnsurePathInDefaultFileSystem(doneDirPrefix, conf);
		}

		/// <summary>
		/// Get default file system URI for the cluster (used to ensure consistency
		/// of history done/staging locations) over different context
		/// </summary>
		/// <returns>Default file context</returns>
		private static FileContext GetDefaultFileContext()
		{
			// If FS_DEFAULT_NAME_KEY was set solely by core-default.xml then we ignore
			// ignore it. This prevents defaulting history paths to file system specified
			// by core-default.xml which would not make sense in any case. For a test
			// case to exploit this functionality it should create core-site.xml
			FileContext fc = null;
			Configuration defaultConf = new Configuration();
			string[] sources;
			sources = defaultConf.GetPropertySources(CommonConfigurationKeysPublic.FsDefaultNameKey
				);
			if (sources != null && (!Arrays.AsList(sources).Contains("core-default.xml") || sources
				.Length > 1))
			{
				try
				{
					fc = FileContext.GetFileContext(defaultConf);
					Log.Info("Default file system [" + fc.GetDefaultFileSystem().GetUri() + "]");
				}
				catch (UnsupportedFileSystemException e)
				{
					Log.Error("Unable to create default file context [" + defaultConf.Get(CommonConfigurationKeysPublic
						.FsDefaultNameKey) + "]", e);
				}
			}
			else
			{
				Log.Info("Default file system is set solely " + "by core-default.xml therefore -  ignoring"
					);
			}
			return fc;
		}

		/// <summary>
		/// Ensure that path belongs to cluster's default file system unless
		/// 1.
		/// </summary>
		/// <remarks>
		/// Ensure that path belongs to cluster's default file system unless
		/// 1. it is already fully qualified.
		/// 2. current job configuration uses default file system
		/// 3. running from a test case without core-site.xml
		/// </remarks>
		/// <param name="sourcePath">source path</param>
		/// <param name="conf">the job configuration</param>
		/// <returns>full qualified path (if necessary) in default file system</returns>
		private static string EnsurePathInDefaultFileSystem(string sourcePath, Configuration
			 conf)
		{
			Path path = new Path(sourcePath);
			FileContext fc = GetDefaultFileContext();
			if (fc == null || fc.GetDefaultFileSystem().GetUri().ToString().Equals(conf.Get(CommonConfigurationKeysPublic
				.FsDefaultNameKey, string.Empty)) || path.ToUri().GetAuthority() != null || path
				.ToUri().GetScheme() != null)
			{
				return sourcePath;
			}
			return fc.MakeQualified(path).ToString();
		}

		/// <summary>Gets the user directory for intermediate done history files.</summary>
		/// <param name="conf">the configuration object</param>
		/// <returns>the intermediate done directory for jobhistory files.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string GetHistoryIntermediateDoneDirForUser(Configuration conf)
		{
			return new Path(GetConfiguredHistoryIntermediateDoneDirPrefix(conf), UserGroupInformation
				.GetCurrentUser().GetShortUserName()).ToString();
		}

		public static bool ShouldCreateNonUserDirectory(Configuration conf)
		{
			// Returning true by default to allow non secure single node clusters to work
			// without any configuration change.
			return conf.GetBoolean(MRJobConfig.MrAmCreateJhIntermediateBaseDir, true);
		}

		/// <summary>Get the job history file path for non Done history files.</summary>
		public static Path GetStagingJobHistoryFile(Path dir, JobId jobId, int attempt)
		{
			return GetStagingJobHistoryFile(dir, TypeConverter.FromYarn(jobId).ToString(), attempt
				);
		}

		/// <summary>Get the job history file path for non Done history files.</summary>
		public static Path GetStagingJobHistoryFile(Path dir, string jobId, int attempt)
		{
			return new Path(dir, jobId + "_" + attempt + JobHistoryFileExtension);
		}

		/// <summary>Get the done configuration file name for a job.</summary>
		/// <param name="jobId">the jobId.</param>
		/// <returns>the conf file name.</returns>
		public static string GetIntermediateConfFileName(JobId jobId)
		{
			return TypeConverter.FromYarn(jobId).ToString() + ConfFileNameSuffix;
		}

		/// <summary>Get the done summary file name for a job.</summary>
		/// <param name="jobId">the jobId.</param>
		/// <returns>the conf file name.</returns>
		public static string GetIntermediateSummaryFileName(JobId jobId)
		{
			return TypeConverter.FromYarn(jobId).ToString() + SummaryFileNameSuffix;
		}

		/// <summary>Gets the conf file path for jobs in progress.</summary>
		/// <param name="logDir">the log directory prefix.</param>
		/// <param name="jobId">the jobId.</param>
		/// <param name="attempt">attempt number for this job.</param>
		/// <returns>the conf file path for jobs in progress.</returns>
		public static Path GetStagingConfFile(Path logDir, JobId jobId, int attempt)
		{
			Path jobFilePath = null;
			if (logDir != null)
			{
				jobFilePath = new Path(logDir, TypeConverter.FromYarn(jobId).ToString() + "_" + attempt
					 + ConfFileNameSuffix);
			}
			return jobFilePath;
		}

		/// <summary>Gets the serial number part of the path based on the jobId and serialNumber format.
		/// 	</summary>
		/// <param name="id"/>
		/// <param name="serialNumberFormat"/>
		/// <returns>the serial number part of the patch based on the jobId and serial number format.
		/// 	</returns>
		public static string SerialNumberDirectoryComponent(JobId id, string serialNumberFormat
			)
		{
			return Sharpen.Runtime.Substring(string.Format(serialNumberFormat, Sharpen.Extensions.ValueOf
				(JobSerialNumber(id))), 0, SerialNumberDirectoryDigits);
		}

		/// <summary>Extracts the timstamp component from the path.</summary>
		/// <param name="path"/>
		/// <returns>the timestamp component from the path</returns>
		public static string GetTimestampPartFromPath(string path)
		{
			Matcher matcher = TimestampDirPattern.Matcher(path);
			if (matcher.Find())
			{
				string matched = matcher.Group();
				string ret = string.Intern(matched);
				return ret;
			}
			else
			{
				return null;
			}
		}

		/// <summary>Gets the history subdirectory based on the jobId, timestamp and serial number format.
		/// 	</summary>
		/// <param name="id"/>
		/// <param name="timestampComponent"/>
		/// <param name="serialNumberFormat"/>
		/// <returns>the history sub directory based on the jobid, timestamp and serial number format
		/// 	</returns>
		public static string HistoryLogSubdirectory(JobId id, string timestampComponent, 
			string serialNumberFormat)
		{
			//    String result = LOG_VERSION_STRING;
			string result = string.Empty;
			string serialNumberDirectory = SerialNumberDirectoryComponent(id, serialNumberFormat
				);
			result = result + timestampComponent + FilePath.separator + serialNumberDirectory
				 + FilePath.separator;
			return result;
		}

		/// <summary>Gets the timestamp component based on millisecond time.</summary>
		/// <param name="millisecondTime"/>
		/// <returns>the timestamp component based on millisecond time</returns>
		public static string TimestampDirectoryComponent(long millisecondTime)
		{
			Calendar timestamp = Calendar.GetInstance();
			timestamp.SetTimeInMillis(millisecondTime);
			string dateString = null;
			dateString = string.Format(TimestampDirFormat, timestamp.Get(Calendar.Year), timestamp
				.Get(Calendar.Month) + 1, timestamp.Get(Calendar.DayOfMonth));
			// months are 0-based in Calendar, but people will expect January to
			// be month #1.
			dateString = string.Intern(dateString);
			return dateString;
		}

		public static string DoneSubdirsBeforeSerialTail()
		{
			// date
			string result = "/*/*/*";
			// YYYY/MM/DD ;
			return result;
		}

		/// <summary>Computes a serial number used as part of directory naming for the given jobId.
		/// 	</summary>
		/// <param name="id">the jobId.</param>
		/// <returns>the serial number used as part of directory naming for the given jobid</returns>
		public static int JobSerialNumber(JobId id)
		{
			return id.GetId();
		}

		/// <exception cref="System.IO.IOException"/>
		public static IList<FileStatus> LocalGlobber(FileContext fc, Path root, string tail
			)
		{
			return LocalGlobber(fc, root, tail, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public static IList<FileStatus> LocalGlobber(FileContext fc, Path root, string tail
			, PathFilter filter)
		{
			return LocalGlobber(fc, root, tail, filter, null);
		}

		// hasMismatches is just used to return a second value if you want
		// one. I would have used MutableBoxedBoolean if such had been provided.
		/// <exception cref="System.IO.IOException"/>
		public static IList<FileStatus> LocalGlobber(FileContext fc, Path root, string tail
			, PathFilter filter, AtomicBoolean hasFlatFiles)
		{
			if (tail.Equals(string.Empty))
			{
				return (ListFilteredStatus(fc, root, filter));
			}
			if (tail.StartsWith("/*"))
			{
				Path[] subdirs = FilteredStat2Paths(RemoteIterToList(fc.ListStatus(root)), true, 
					hasFlatFiles);
				IList<IList<FileStatus>> subsubdirs = new List<IList<FileStatus>>();
				int subsubdirCount = 0;
				if (subdirs.Length == 0)
				{
					return new List<FileStatus>();
				}
				string newTail = Sharpen.Runtime.Substring(tail, 2);
				for (int i = 0; i < subdirs.Length; ++i)
				{
					subsubdirs.AddItem(LocalGlobber(fc, subdirs[i], newTail, filter, null));
					// subsubdirs.set(i, localGlobber(fc, subdirs[i], newTail, filter,
					// null));
					subsubdirCount += subsubdirs[i].Count;
				}
				IList<FileStatus> result = new List<FileStatus>();
				for (int i_1 = 0; i_1 < subsubdirs.Count; ++i_1)
				{
					Sharpen.Collections.AddAll(result, subsubdirs[i_1]);
				}
				return result;
			}
			if (tail.StartsWith("/"))
			{
				int split = tail.IndexOf('/', 1);
				if (split < 0)
				{
					return ListFilteredStatus(fc, new Path(root, Sharpen.Runtime.Substring(tail, 1)), 
						filter);
				}
				else
				{
					string thisSegment = Sharpen.Runtime.Substring(tail, 1, split);
					string newTail = Sharpen.Runtime.Substring(tail, split);
					return LocalGlobber(fc, new Path(root, thisSegment), newTail, filter, hasFlatFiles
						);
				}
			}
			IOException e = new IOException("localGlobber: bad tail");
			throw e;
		}

		/// <exception cref="System.IO.IOException"/>
		private static IList<FileStatus> ListFilteredStatus(FileContext fc, Path root, PathFilter
			 filter)
		{
			IList<FileStatus> fsList = RemoteIterToList(fc.ListStatus(root));
			if (filter == null)
			{
				return fsList;
			}
			else
			{
				IList<FileStatus> filteredList = new List<FileStatus>();
				foreach (FileStatus fs in fsList)
				{
					if (filter.Accept(fs.GetPath()))
					{
						filteredList.AddItem(fs);
					}
				}
				return filteredList;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static IList<FileStatus> RemoteIterToList(RemoteIterator<FileStatus> rIter
			)
		{
			IList<FileStatus> fsList = new List<FileStatus>();
			if (rIter == null)
			{
				return fsList;
			}
			while (rIter.HasNext())
			{
				fsList.AddItem(rIter.Next());
			}
			return fsList;
		}

		// hasMismatches is just used to return a second value if you want
		// one. I would have used MutableBoxedBoolean if such had been provided.
		private static Path[] FilteredStat2Paths(IList<FileStatus> stats, bool dirs, AtomicBoolean
			 hasMismatches)
		{
			int resultCount = 0;
			if (hasMismatches == null)
			{
				hasMismatches = new AtomicBoolean(false);
			}
			for (int i = 0; i < stats.Count; ++i)
			{
				if (stats[i].IsDirectory() == dirs)
				{
					stats.Set(resultCount++, stats[i]);
				}
				else
				{
					hasMismatches.Set(true);
				}
			}
			Path[] result = new Path[resultCount];
			for (int i_1 = 0; i_1 < resultCount; i_1++)
			{
				result[i_1] = stats[i_1].GetPath();
			}
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		public static Path GetPreviousJobHistoryPath(Configuration conf, ApplicationAttemptId
			 applicationAttemptId)
		{
			string jobId = TypeConverter.FromYarn(applicationAttemptId.GetApplicationId()).ToString
				();
			string jobhistoryDir = JobHistoryUtils.GetConfiguredHistoryStagingDirPrefix(conf, 
				jobId);
			Path histDirPath = FileContext.GetFileContext(conf).MakeQualified(new Path(jobhistoryDir
				));
			FileContext fc = FileContext.GetFileContext(histDirPath.ToUri(), conf);
			return fc.MakeQualified(JobHistoryUtils.GetStagingJobHistoryFile(histDirPath, jobId
				, (applicationAttemptId.GetAttemptId() - 1)));
		}

		/// <summary>Looks for the dirs to clean.</summary>
		/// <remarks>
		/// Looks for the dirs to clean.  The folder structure is YYYY/MM/DD/Serial so
		/// we can use that to more efficiently find the directories to clean by
		/// comparing the cutoff timestamp with the timestamp from the folder
		/// structure.
		/// </remarks>
		/// <param name="fc">done dir FileContext</param>
		/// <param name="root">folder for completed jobs</param>
		/// <param name="cutoff">The cutoff for the max history age</param>
		/// <returns>The list of directories for cleaning</returns>
		/// <exception cref="System.IO.IOException"/>
		public static IList<FileStatus> GetHistoryDirsForCleaning(FileContext fc, Path root
			, long cutoff)
		{
			IList<FileStatus> fsList = new AList<FileStatus>();
			Calendar cCal = Calendar.GetInstance();
			cCal.SetTimeInMillis(cutoff);
			int cYear = cCal.Get(Calendar.Year);
			int cMonth = cCal.Get(Calendar.Month) + 1;
			int cDate = cCal.Get(Calendar.Date);
			RemoteIterator<FileStatus> yearDirIt = fc.ListStatus(root);
			while (yearDirIt.HasNext())
			{
				FileStatus yearDir = yearDirIt.Next();
				try
				{
					int year = System.Convert.ToInt32(yearDir.GetPath().GetName());
					if (year <= cYear)
					{
						RemoteIterator<FileStatus> monthDirIt = fc.ListStatus(yearDir.GetPath());
						while (monthDirIt.HasNext())
						{
							FileStatus monthDir = monthDirIt.Next();
							try
							{
								int month = System.Convert.ToInt32(monthDir.GetPath().GetName());
								// If we only checked the month here, then something like 07/2013
								// would incorrectly not pass when the cutoff is 06/2014
								if (year < cYear || month <= cMonth)
								{
									RemoteIterator<FileStatus> dateDirIt = fc.ListStatus(monthDir.GetPath());
									while (dateDirIt.HasNext())
									{
										FileStatus dateDir = dateDirIt.Next();
										try
										{
											int date = System.Convert.ToInt32(dateDir.GetPath().GetName());
											// If we only checked the date here, then something like
											// 07/21/2013 would incorrectly not pass when the cutoff is
											// 08/20/2013 or 07/20/2012
											if (year < cYear || month < cMonth || date <= cDate)
											{
												Sharpen.Collections.AddAll(fsList, RemoteIterToList(fc.ListStatus(dateDir.GetPath
													())));
											}
										}
										catch (FormatException)
										{
										}
									}
								}
							}
							catch (FormatException)
							{
							}
						}
					}
				}
				catch (FormatException)
				{
				}
			}
			// the directory didn't fit the format we're looking for so
			// skip the dir
			// the directory didn't fit the format we're looking for so skip
			// the dir
			// the directory didn't fit the format we're looking for so skip the dir
			return fsList;
		}
	}
}
