using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	/// <summary>Manages a list of local storage directories.</summary>
	internal class DirectoryCollection
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.DirectoryCollection
			));

		public enum DiskErrorCause
		{
			DiskFull,
			Other
		}

		internal class DiskErrorInformation
		{
			internal DirectoryCollection.DiskErrorCause cause;

			internal string message;

			internal DiskErrorInformation(DirectoryCollection.DiskErrorCause cause, string message
				)
			{
				this.cause = cause;
				this.message = message;
			}
		}

		/// <summary>Returns a merged list which contains all the elements of l1 and l2</summary>
		/// <param name="l1">the first list to be included</param>
		/// <param name="l2">the second list to be included</param>
		/// <returns>a new list containing all the elements of the first and second list</returns>
		internal static IList<string> Concat(IList<string> l1, IList<string> l2)
		{
			IList<string> ret = new AList<string>(l1.Count + l2.Count);
			Sharpen.Collections.AddAll(ret, l1);
			Sharpen.Collections.AddAll(ret, l2);
			return ret;
		}

		private IList<string> localDirs;

		private IList<string> errorDirs;

		private IList<string> fullDirs;

		private int numFailures;

		private float diskUtilizationPercentageCutoff;

		private long diskUtilizationSpaceCutoff;

		/// <summary>Create collection for the directories specified.</summary>
		/// <remarks>Create collection for the directories specified. No check for free space.
		/// 	</remarks>
		/// <param name="dirs">directories to be monitored</param>
		public DirectoryCollection(string[] dirs)
			: this(dirs, 100.0F, 0)
		{
		}

		/// <summary>Create collection for the directories specified.</summary>
		/// <remarks>
		/// Create collection for the directories specified. Users must specify the
		/// maximum percentage of disk utilization allowed. Minimum amount of disk
		/// space is not checked.
		/// </remarks>
		/// <param name="dirs">directories to be monitored</param>
		/// <param name="utilizationPercentageCutOff">
		/// percentage of disk that can be used before the dir is taken out of
		/// the good dirs list
		/// </param>
		public DirectoryCollection(string[] dirs, float utilizationPercentageCutOff)
			: this(dirs, utilizationPercentageCutOff, 0)
		{
		}

		/// <summary>Create collection for the directories specified.</summary>
		/// <remarks>
		/// Create collection for the directories specified. Users must specify the
		/// minimum amount of free space that must be available for the dir to be used.
		/// </remarks>
		/// <param name="dirs">directories to be monitored</param>
		/// <param name="utilizationSpaceCutOff">
		/// minimum space, in MB, that must be available on the disk for the
		/// dir to be marked as good
		/// </param>
		public DirectoryCollection(string[] dirs, long utilizationSpaceCutOff)
			: this(dirs, 100.0F, utilizationSpaceCutOff)
		{
		}

		/// <summary>Create collection for the directories specified.</summary>
		/// <remarks>
		/// Create collection for the directories specified. Users must specify the
		/// maximum percentage of disk utilization allowed and the minimum amount of
		/// free space that must be available for the dir to be used. If either check
		/// fails the dir is removed from the good dirs list.
		/// </remarks>
		/// <param name="dirs">directories to be monitored</param>
		/// <param name="utilizationPercentageCutOff">
		/// percentage of disk that can be used before the dir is taken out of
		/// the good dirs list
		/// </param>
		/// <param name="utilizationSpaceCutOff">
		/// minimum space, in MB, that must be available on the disk for the
		/// dir to be marked as good
		/// </param>
		public DirectoryCollection(string[] dirs, float utilizationPercentageCutOff, long
			 utilizationSpaceCutOff)
		{
			// Good local storage directories
			localDirs = new CopyOnWriteArrayList<string>(dirs);
			errorDirs = new CopyOnWriteArrayList<string>();
			fullDirs = new CopyOnWriteArrayList<string>();
			diskUtilizationPercentageCutoff = utilizationPercentageCutOff < 0.0F ? 0.0F : (utilizationPercentageCutOff
				 > 100.0F ? 100.0F : utilizationPercentageCutOff);
			diskUtilizationSpaceCutoff = utilizationSpaceCutOff < 0 ? 0 : utilizationSpaceCutOff;
		}

		/// <returns>the current valid directories</returns>
		internal virtual IList<string> GetGoodDirs()
		{
			lock (this)
			{
				return Sharpen.Collections.UnmodifiableList(localDirs);
			}
		}

		/// <returns>the failed directories</returns>
		internal virtual IList<string> GetFailedDirs()
		{
			lock (this)
			{
				return Sharpen.Collections.UnmodifiableList(DirectoryCollection.Concat(errorDirs, 
					fullDirs));
			}
		}

		/// <returns>the directories that have used all disk space</returns>
		internal virtual IList<string> GetFullDirs()
		{
			lock (this)
			{
				return fullDirs;
			}
		}

		/// <returns>total the number of directory failures seen till now</returns>
		internal virtual int GetNumFailures()
		{
			lock (this)
			{
				return numFailures;
			}
		}

		/// <summary>
		/// Create any non-existent directories and parent directories, updating the
		/// list of valid directories if necessary.
		/// </summary>
		/// <param name="localFs">local file system to use</param>
		/// <param name="perm">absolute permissions to use for any directories created</param>
		/// <returns>true if there were no errors, false if at least one error occurred</returns>
		internal virtual bool CreateNonExistentDirs(FileContext localFs, FsPermission perm
			)
		{
			lock (this)
			{
				bool failed = false;
				foreach (string dir in localDirs)
				{
					try
					{
						CreateDir(localFs, new Path(dir), perm);
					}
					catch (IOException e)
					{
						Log.Warn("Unable to create directory " + dir + " error " + e.Message + ", removing from the list of valid directories."
							);
						localDirs.Remove(dir);
						errorDirs.AddItem(dir);
						numFailures++;
						failed = true;
					}
				}
				return !failed;
			}
		}

		/// <summary>
		/// Check the health of current set of local directories(good and failed),
		/// updating the list of valid directories if necessary.
		/// </summary>
		/// <returns>
		/// <em>true</em> if there is a new disk-failure identified in this
		/// checking or a failed directory passes the disk check <em>false</em>
		/// otherwise.
		/// </returns>
		internal virtual bool CheckDirs()
		{
			lock (this)
			{
				bool setChanged = false;
				ICollection<string> preCheckGoodDirs = new HashSet<string>(localDirs);
				ICollection<string> preCheckFullDirs = new HashSet<string>(fullDirs);
				ICollection<string> preCheckOtherErrorDirs = new HashSet<string>(errorDirs);
				IList<string> failedDirs = DirectoryCollection.Concat(errorDirs, fullDirs);
				IList<string> allLocalDirs = DirectoryCollection.Concat(localDirs, failedDirs);
				IDictionary<string, DirectoryCollection.DiskErrorInformation> dirsFailedCheck = TestDirs
					(allLocalDirs);
				localDirs.Clear();
				errorDirs.Clear();
				fullDirs.Clear();
				foreach (KeyValuePair<string, DirectoryCollection.DiskErrorInformation> entry in 
					dirsFailedCheck)
				{
					string dir = entry.Key;
					DirectoryCollection.DiskErrorInformation errorInformation = entry.Value;
					switch (entry.Value.cause)
					{
						case DirectoryCollection.DiskErrorCause.DiskFull:
						{
							fullDirs.AddItem(entry.Key);
							break;
						}

						case DirectoryCollection.DiskErrorCause.Other:
						{
							errorDirs.AddItem(entry.Key);
							break;
						}
					}
					if (preCheckGoodDirs.Contains(dir))
					{
						Log.Warn("Directory " + dir + " error, " + errorInformation.message + ", removing from list of valid directories"
							);
						setChanged = true;
						numFailures++;
					}
				}
				foreach (string dir_1 in allLocalDirs)
				{
					if (!dirsFailedCheck.Contains(dir_1))
					{
						localDirs.AddItem(dir_1);
						if (preCheckFullDirs.Contains(dir_1) || preCheckOtherErrorDirs.Contains(dir_1))
						{
							setChanged = true;
							Log.Info("Directory " + dir_1 + " passed disk check, adding to list of valid directories."
								);
						}
					}
				}
				ICollection<string> postCheckFullDirs = new HashSet<string>(fullDirs);
				ICollection<string> postCheckOtherDirs = new HashSet<string>(errorDirs);
				foreach (string dir_2 in preCheckFullDirs)
				{
					if (postCheckOtherDirs.Contains(dir_2))
					{
						Log.Warn("Directory " + dir_2 + " error " + dirsFailedCheck[dir_2].message);
					}
				}
				foreach (string dir_3 in preCheckOtherErrorDirs)
				{
					if (postCheckFullDirs.Contains(dir_3))
					{
						Log.Warn("Directory " + dir_3 + " error " + dirsFailedCheck[dir_3].message);
					}
				}
				return setChanged;
			}
		}

		internal virtual IDictionary<string, DirectoryCollection.DiskErrorInformation> TestDirs
			(IList<string> dirs)
		{
			Dictionary<string, DirectoryCollection.DiskErrorInformation> ret = new Dictionary
				<string, DirectoryCollection.DiskErrorInformation>();
			foreach (string dir in dirs)
			{
				string msg;
				try
				{
					FilePath testDir = new FilePath(dir);
					DiskChecker.CheckDir(testDir);
					if (IsDiskUsageOverPercentageLimit(testDir))
					{
						msg = "used space above threshold of " + diskUtilizationPercentageCutoff + "%";
						ret[dir] = new DirectoryCollection.DiskErrorInformation(DirectoryCollection.DiskErrorCause
							.DiskFull, msg);
						continue;
					}
					else
					{
						if (IsDiskFreeSpaceUnderLimit(testDir))
						{
							msg = "free space below limit of " + diskUtilizationSpaceCutoff + "MB";
							ret[dir] = new DirectoryCollection.DiskErrorInformation(DirectoryCollection.DiskErrorCause
								.DiskFull, msg);
							continue;
						}
					}
					// create a random dir to make sure fs isn't in read-only mode
					VerifyDirUsingMkdir(testDir);
				}
				catch (IOException ie)
				{
					ret[dir] = new DirectoryCollection.DiskErrorInformation(DirectoryCollection.DiskErrorCause
						.Other, ie.Message);
				}
			}
			return ret;
		}

		/// <summary>
		/// Function to test whether a dir is working correctly by actually creating a
		/// random directory.
		/// </summary>
		/// <param name="dir">the dir to test</param>
		/// <exception cref="System.IO.IOException"/>
		private void VerifyDirUsingMkdir(FilePath dir)
		{
			string randomDirName = RandomStringUtils.RandomAlphanumeric(5);
			FilePath target = new FilePath(dir, randomDirName);
			int i = 0;
			while (target.Exists())
			{
				randomDirName = RandomStringUtils.RandomAlphanumeric(5) + i;
				target = new FilePath(dir, randomDirName);
				i++;
			}
			try
			{
				DiskChecker.CheckDir(target);
			}
			finally
			{
				FileUtils.DeleteQuietly(target);
			}
		}

		private bool IsDiskUsageOverPercentageLimit(FilePath dir)
		{
			float freePercentage = 100 * (dir.GetUsableSpace() / (float)dir.GetTotalSpace());
			float usedPercentage = 100.0F - freePercentage;
			return (usedPercentage > diskUtilizationPercentageCutoff || usedPercentage >= 100.0F
				);
		}

		private bool IsDiskFreeSpaceUnderLimit(FilePath dir)
		{
			long freeSpace = dir.GetUsableSpace() / (1024 * 1024);
			return freeSpace < this.diskUtilizationSpaceCutoff;
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateDir(FileContext localFs, Path dir, FsPermission perm)
		{
			if (dir == null)
			{
				return;
			}
			try
			{
				localFs.GetFileStatus(dir);
			}
			catch (FileNotFoundException)
			{
				CreateDir(localFs, dir.GetParent(), perm);
				localFs.Mkdir(dir, perm, false);
				if (!perm.Equals(perm.ApplyUMask(localFs.GetUMask())))
				{
					localFs.SetPermission(dir, perm);
				}
			}
		}

		public virtual float GetDiskUtilizationPercentageCutoff()
		{
			return diskUtilizationPercentageCutoff;
		}

		public virtual void SetDiskUtilizationPercentageCutoff(float diskUtilizationPercentageCutoff
			)
		{
			this.diskUtilizationPercentageCutoff = diskUtilizationPercentageCutoff < 0.0F ? 0.0F
				 : (diskUtilizationPercentageCutoff > 100.0F ? 100.0F : diskUtilizationPercentageCutoff
				);
		}

		public virtual long GetDiskUtilizationSpaceCutoff()
		{
			return diskUtilizationSpaceCutoff;
		}

		public virtual void SetDiskUtilizationSpaceCutoff(long diskUtilizationSpaceCutoff
			)
		{
			diskUtilizationSpaceCutoff = diskUtilizationSpaceCutoff < 0 ? 0 : diskUtilizationSpaceCutoff;
			this.diskUtilizationSpaceCutoff = diskUtilizationSpaceCutoff;
		}
	}
}
