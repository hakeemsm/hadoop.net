using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>A utility to manage job submission files.</summary>
	public class JobSubmissionFiles
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(JobSubmissionFiles));

		public static readonly FsPermission JobDirPermission = FsPermission.CreateImmutable
			((short)0x1c0);

		public static readonly FsPermission JobFilePermission = FsPermission.CreateImmutable
			((short)0x1a4);

		// job submission directory is private!
		// rwx--------
		//job files are world-wide readable and owner writable
		// rw-r--r--
		public static Path GetJobSplitFile(Path jobSubmissionDir)
		{
			return new Path(jobSubmissionDir, "job.split");
		}

		public static Path GetJobSplitMetaFile(Path jobSubmissionDir)
		{
			return new Path(jobSubmissionDir, "job.splitmetainfo");
		}

		/// <summary>Get the job conf path.</summary>
		public static Path GetJobConfPath(Path jobSubmitDir)
		{
			return new Path(jobSubmitDir, "job.xml");
		}

		/// <summary>Get the job jar path.</summary>
		public static Path GetJobJar(Path jobSubmitDir)
		{
			return new Path(jobSubmitDir, "job.jar");
		}

		/// <summary>Get the job distributed cache files path.</summary>
		/// <param name="jobSubmitDir"/>
		public static Path GetJobDistCacheFiles(Path jobSubmitDir)
		{
			return new Path(jobSubmitDir, "files");
		}

		/// <summary>Get the job distributed cache path for log4j properties.</summary>
		/// <param name="jobSubmitDir"/>
		public static Path GetJobLog4jFile(Path jobSubmitDir)
		{
			return new Path(jobSubmitDir, "log4j");
		}

		/// <summary>Get the job distributed cache archives path.</summary>
		/// <param name="jobSubmitDir"></param>
		public static Path GetJobDistCacheArchives(Path jobSubmitDir)
		{
			return new Path(jobSubmitDir, "archives");
		}

		/// <summary>Get the job distributed cache libjars path.</summary>
		/// <param name="jobSubmitDir"></param>
		public static Path GetJobDistCacheLibjars(Path jobSubmitDir)
		{
			return new Path(jobSubmitDir, "libjars");
		}

		/// <summary>Initializes the staging directory and returns the path.</summary>
		/// <remarks>
		/// Initializes the staging directory and returns the path. It also
		/// keeps track of all necessary ownership and permissions
		/// </remarks>
		/// <param name="cluster"/>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static Path GetStagingDir(Cluster cluster, Configuration conf)
		{
			Path stagingArea = cluster.GetStagingAreaDir();
			FileSystem fs = stagingArea.GetFileSystem(conf);
			string realUser;
			string currentUser;
			UserGroupInformation ugi = UserGroupInformation.GetLoginUser();
			realUser = ugi.GetShortUserName();
			currentUser = UserGroupInformation.GetCurrentUser().GetShortUserName();
			if (fs.Exists(stagingArea))
			{
				FileStatus fsStatus = fs.GetFileStatus(stagingArea);
				string owner = fsStatus.GetOwner();
				if (!(owner.Equals(currentUser) || owner.Equals(realUser)))
				{
					throw new IOException("The ownership on the staging directory " + stagingArea + " is not as expected. "
						 + "It is owned by " + owner + ". The directory must " + "be owned by the submitter "
						 + currentUser + " or " + "by " + realUser);
				}
				if (!fsStatus.GetPermission().Equals(JobDirPermission))
				{
					Log.Info("Permissions on staging directory " + stagingArea + " are " + "incorrect: "
						 + fsStatus.GetPermission() + ". Fixing permissions " + "to correct value " + JobDirPermission
						);
					fs.SetPermission(stagingArea, JobDirPermission);
				}
			}
			else
			{
				fs.Mkdirs(stagingArea, new FsPermission(JobDirPermission));
			}
			return stagingArea;
		}
	}
}
