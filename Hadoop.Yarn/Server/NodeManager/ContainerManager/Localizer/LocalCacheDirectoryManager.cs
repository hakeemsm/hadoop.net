using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	/// <summary>
	/// <see cref="LocalCacheDirectoryManager"/>
	/// is used for managing hierarchical
	/// directories for local cache. It will allow to restrict the number of files in
	/// a directory to
	/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.NmLocalCacheMaxFilesPerDirectory
	/// 	"/>
	/// which
	/// includes 36 sub-directories (named from 0 to 9 and a to z). Root directory is
	/// represented by an empty string. It internally maintains a vacant directory
	/// queue. As soon as the file count for the directory reaches its limit; new
	/// files will not be created in it until at least one file is deleted from it.
	/// New sub directories are not created unless a
	/// <see cref="GetRelativePathForLocalization()"/>
	/// request
	/// is made and nonFullDirectories are empty.
	/// Note : this structure only returns relative localization path but doesn't
	/// create one on disk.
	/// </summary>
	public class LocalCacheDirectoryManager
	{
		private readonly int perDirectoryFileLimit;

		public const int DirectoriesPerLevel = 36;

		private Queue<LocalCacheDirectoryManager.Directory> nonFullDirectories;

		private Dictionary<string, LocalCacheDirectoryManager.Directory> knownDirectories;

		private int totalSubDirectories;

		public LocalCacheDirectoryManager(Configuration conf)
		{
			// total 36 = a to z plus 0 to 9
			totalSubDirectories = 0;
			LocalCacheDirectoryManager.Directory rootDir = new LocalCacheDirectoryManager.Directory
				(totalSubDirectories);
			nonFullDirectories = new List<LocalCacheDirectoryManager.Directory>();
			knownDirectories = new Dictionary<string, LocalCacheDirectoryManager.Directory>();
			knownDirectories[string.Empty] = rootDir;
			nonFullDirectories.AddItem(rootDir);
			this.perDirectoryFileLimit = conf.GetInt(YarnConfiguration.NmLocalCacheMaxFilesPerDirectory
				, YarnConfiguration.DefaultNmLocalCacheMaxFilesPerDirectory) - 36;
		}

		/// <summary>
		/// This method will return relative path from the first available vacant
		/// directory.
		/// </summary>
		/// <returns>
		/// 
		/// <see cref="string"/>
		/// relative path for localization
		/// </returns>
		public virtual string GetRelativePathForLocalization()
		{
			lock (this)
			{
				if (nonFullDirectories.IsEmpty())
				{
					totalSubDirectories++;
					LocalCacheDirectoryManager.Directory newDir = new LocalCacheDirectoryManager.Directory
						(totalSubDirectories);
					nonFullDirectories.AddItem(newDir);
					knownDirectories[newDir.GetRelativePath()] = newDir;
				}
				LocalCacheDirectoryManager.Directory subDir = nonFullDirectories.Peek();
				if (subDir.IncrementAndGetCount() >= perDirectoryFileLimit)
				{
					nonFullDirectories.Remove();
				}
				return subDir.GetRelativePath();
			}
		}

		/// <summary>
		/// This method will reduce the file count for the directory represented by
		/// path.
		/// </summary>
		/// <remarks>
		/// This method will reduce the file count for the directory represented by
		/// path. The root directory of this Local cache directory manager is
		/// represented by an empty string.
		/// </remarks>
		public virtual void DecrementFileCountForPath(string relPath)
		{
			lock (this)
			{
				relPath = relPath == null ? string.Empty : relPath.Trim();
				LocalCacheDirectoryManager.Directory subDir = knownDirectories[relPath];
				int oldCount = subDir.GetCount();
				if (subDir.DecrementAndGetCount() < perDirectoryFileLimit && oldCount >= perDirectoryFileLimit)
				{
					nonFullDirectories.AddItem(subDir);
				}
			}
		}

		/// <summary>Increment the file count for a relative directory within the cache</summary>
		/// <param name="relPath">the relative path</param>
		public virtual void IncrementFileCountForPath(string relPath)
		{
			lock (this)
			{
				relPath = relPath == null ? string.Empty : relPath.Trim();
				LocalCacheDirectoryManager.Directory subDir = knownDirectories[relPath];
				if (subDir == null)
				{
					int dirnum = LocalCacheDirectoryManager.Directory.GetDirectoryNumber(relPath);
					totalSubDirectories = Math.Max(dirnum, totalSubDirectories);
					subDir = new LocalCacheDirectoryManager.Directory(dirnum);
					nonFullDirectories.AddItem(subDir);
					knownDirectories[subDir.GetRelativePath()] = subDir;
				}
				if (subDir.IncrementAndGetCount() >= perDirectoryFileLimit)
				{
					nonFullDirectories.Remove(subDir);
				}
			}
		}

		/// <summary>
		/// Given a path to a directory within a local cache tree return the
		/// root of the cache directory.
		/// </summary>
		/// <param name="path">the directory within a cache directory</param>
		/// <returns>the local cache directory root or null if not found</returns>
		public static Path GetCacheDirectoryRoot(Path path)
		{
			while (path != null)
			{
				string name = path.GetName();
				if (name.Length != 1)
				{
					return path;
				}
				int dirnum = DirectoriesPerLevel;
				try
				{
					dirnum = System.Convert.ToInt32(name, DirectoriesPerLevel);
				}
				catch (FormatException)
				{
				}
				if (dirnum >= DirectoriesPerLevel)
				{
					return path;
				}
				path = path.GetParent();
			}
			return path;
		}

		[VisibleForTesting]
		internal virtual LocalCacheDirectoryManager.Directory GetDirectory(string relPath
			)
		{
			lock (this)
			{
				return knownDirectories[relPath];
			}
		}

		internal class Directory
		{
			private readonly string relativePath;

			private int fileCount;

			/*
			* It limits the number of files and sub directories in the directory to the
			* limit LocalCacheDirectoryManager#perDirectoryFileLimit.
			*/
			internal static string GetRelativePath(int directoryNo)
			{
				string relativePath = string.Empty;
				if (directoryNo > 0)
				{
					string tPath = Sharpen.Extensions.ToString(directoryNo - 1, DirectoriesPerLevel);
					StringBuilder sb = new StringBuilder();
					if (tPath.Length == 1)
					{
						sb.Append(tPath[0]);
					}
					else
					{
						// this is done to make sure we also reuse 0th sub directory
						sb.Append(Sharpen.Extensions.ToString(System.Convert.ToInt32(Sharpen.Runtime.Substring
							(tPath, 0, 1), DirectoriesPerLevel) - 1, DirectoriesPerLevel));
					}
					for (int i = 1; i < tPath.Length; i++)
					{
						sb.Append(Path.Separator).Append(tPath[i]);
					}
					relativePath = sb.ToString();
				}
				return relativePath;
			}

			internal static int GetDirectoryNumber(string relativePath)
			{
				string numStr = relativePath.Replace("/", string.Empty);
				if (relativePath.IsEmpty())
				{
					return 0;
				}
				if (numStr.Length > 1)
				{
					// undo step from getRelativePath() to reuse 0th sub directory
					string firstChar = Sharpen.Extensions.ToString(System.Convert.ToInt32(Sharpen.Runtime.Substring
						(numStr, 0, 1), DirectoriesPerLevel) + 1, DirectoriesPerLevel);
					numStr = firstChar + Sharpen.Runtime.Substring(numStr, 1);
				}
				return System.Convert.ToInt32(numStr, DirectoriesPerLevel) + 1;
			}

			public Directory(int directoryNo)
			{
				fileCount = 0;
				relativePath = GetRelativePath(directoryNo);
			}

			public virtual int IncrementAndGetCount()
			{
				return ++fileCount;
			}

			public virtual int DecrementAndGetCount()
			{
				return --fileCount;
			}

			public virtual string GetRelativePath()
			{
				return relativePath;
			}

			public virtual int GetCount()
			{
				return fileCount;
			}
		}
	}
}
