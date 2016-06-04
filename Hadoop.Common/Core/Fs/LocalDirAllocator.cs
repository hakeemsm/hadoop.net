using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Fs;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// An implementation of a round-robin scheme for disk allocation for creating
	/// files.
	/// </summary>
	/// <remarks>
	/// An implementation of a round-robin scheme for disk allocation for creating
	/// files. The way it works is that it is kept track what disk was last
	/// allocated for a file write. For the current request, the next disk from
	/// the set of disks would be allocated if the free space on the disk is
	/// sufficient enough to accommodate the file that is being considered for
	/// creation. If the space requirements cannot be met, the next disk in order
	/// would be tried and so on till a disk is found with sufficient capacity.
	/// Once a disk with sufficient space is identified, a check is done to make
	/// sure that the disk is writable. Also, there is an API provided that doesn't
	/// take the space requirements into consideration but just checks whether the
	/// disk under consideration is writable (this should be used for cases where
	/// the file size is not known apriori). An API is provided to read a path that
	/// was created earlier. That API works by doing a scan of all the disks for the
	/// input pathname.
	/// This implementation also provides the functionality of having multiple
	/// allocators per JVM (one for each unique functionality or context, like
	/// mapred, dfs-client, etc.). It ensures that there is only one instance of
	/// an allocator per context per JVM.
	/// Note:
	/// 1. The contexts referred above are actually the configuration items defined
	/// in the Configuration class like "mapred.local.dir" (for which we want to
	/// control the dir allocations). The context-strings are exactly those
	/// configuration items.
	/// 2. This implementation does not take into consideration cases where
	/// a disk becomes read-only or goes out of space while a file is being written
	/// to (disks are shared between multiple processes, and so the latter situation
	/// is probable).
	/// 3. In the class implementation, "Disk" is referred to as "Dir", which
	/// actually points to the configured directory on the Disk which will be the
	/// parent for all file write/read allocations.
	/// </remarks>
	public class LocalDirAllocator
	{
		private static IDictionary<string, LocalDirAllocator.AllocatorPerContext> contexts
			 = new SortedDictionary<string, LocalDirAllocator.AllocatorPerContext>();

		private string contextCfgItemName;

		/// <summary>Used when size of file to be allocated is unknown.</summary>
		public const int SizeUnknown = -1;

		/// <summary>Create an allocator object</summary>
		/// <param name="contextCfgItemName"/>
		public LocalDirAllocator(string contextCfgItemName)
		{
			//A Map from the config item names like "mapred.local.dir"
			//to the instance of the AllocatorPerContext. This
			//is a static object to make sure there exists exactly one instance per JVM
			this.contextCfgItemName = contextCfgItemName;
		}

		/// <summary>
		/// This method must be used to obtain the dir allocation context for a
		/// particular value of the context name.
		/// </summary>
		/// <remarks>
		/// This method must be used to obtain the dir allocation context for a
		/// particular value of the context name. The context name must be an item
		/// defined in the Configuration object for which we want to control the
		/// dir allocations (e.g., <code>mapred.local.dir</code>). The method will
		/// create a context for that name if it doesn't already exist.
		/// </remarks>
		private LocalDirAllocator.AllocatorPerContext ObtainContext(string contextCfgItemName
			)
		{
			lock (contexts)
			{
				LocalDirAllocator.AllocatorPerContext l = contexts[contextCfgItemName];
				if (l == null)
				{
					contexts[contextCfgItemName] = (l = new LocalDirAllocator.AllocatorPerContext(contextCfgItemName
						));
				}
				return l;
			}
		}

		/// <summary>Get a path from the local FS.</summary>
		/// <remarks>
		/// Get a path from the local FS. This method should be used if the size of
		/// the file is not known apriori. We go round-robin over the set of disks
		/// (via the configured dirs) and return the first complete path where
		/// we could create the parent directory of the passed path.
		/// </remarks>
		/// <param name="pathStr">
		/// the requested path (this will be created on the first
		/// available disk)
		/// </param>
		/// <param name="conf">the Configuration object</param>
		/// <returns>the complete path to the file on a local disk</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetLocalPathForWrite(string pathStr, Configuration conf)
		{
			return GetLocalPathForWrite(pathStr, SizeUnknown, conf);
		}

		/// <summary>Get a path from the local FS.</summary>
		/// <remarks>
		/// Get a path from the local FS. Pass size as
		/// SIZE_UNKNOWN if not known apriori. We
		/// round-robin over the set of disks (via the configured dirs) and return
		/// the first complete path which has enough space
		/// </remarks>
		/// <param name="pathStr">
		/// the requested path (this will be created on the first
		/// available disk)
		/// </param>
		/// <param name="size">the size of the file that is going to be written</param>
		/// <param name="conf">the Configuration object</param>
		/// <returns>the complete path to the file on a local disk</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetLocalPathForWrite(string pathStr, long size, Configuration
			 conf)
		{
			return GetLocalPathForWrite(pathStr, size, conf, true);
		}

		/// <summary>Get a path from the local FS.</summary>
		/// <remarks>
		/// Get a path from the local FS. Pass size as
		/// SIZE_UNKNOWN if not known apriori. We
		/// round-robin over the set of disks (via the configured dirs) and return
		/// the first complete path which has enough space
		/// </remarks>
		/// <param name="pathStr">
		/// the requested path (this will be created on the first
		/// available disk)
		/// </param>
		/// <param name="size">the size of the file that is going to be written</param>
		/// <param name="conf">the Configuration object</param>
		/// <param name="checkWrite">ensure that the path is writable</param>
		/// <returns>the complete path to the file on a local disk</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetLocalPathForWrite(string pathStr, long size, Configuration
			 conf, bool checkWrite)
		{
			LocalDirAllocator.AllocatorPerContext context = ObtainContext(contextCfgItemName);
			return context.GetLocalPathForWrite(pathStr, size, conf, checkWrite);
		}

		/// <summary>Get a path from the local FS for reading.</summary>
		/// <remarks>
		/// Get a path from the local FS for reading. We search through all the
		/// configured dirs for the file's existence and return the complete
		/// path to the file when we find one
		/// </remarks>
		/// <param name="pathStr">the requested file (this will be searched)</param>
		/// <param name="conf">the Configuration object</param>
		/// <returns>the complete path to the file on a local disk</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetLocalPathToRead(string pathStr, Configuration conf)
		{
			LocalDirAllocator.AllocatorPerContext context = ObtainContext(contextCfgItemName);
			return context.GetLocalPathToRead(pathStr, conf);
		}

		/// <summary>Get all of the paths that currently exist in the working directories.</summary>
		/// <param name="pathStr">the path underneath the roots</param>
		/// <param name="conf">the configuration to look up the roots in</param>
		/// <returns>all of the paths that exist under any of the roots</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual IEnumerable<Path> GetAllLocalPathsToRead(string pathStr, Configuration
			 conf)
		{
			LocalDirAllocator.AllocatorPerContext context;
			lock (this)
			{
				context = ObtainContext(contextCfgItemName);
			}
			return context.GetAllLocalPathsToRead(pathStr, conf);
		}

		/// <summary>Creates a temporary file in the local FS.</summary>
		/// <remarks>
		/// Creates a temporary file in the local FS. Pass size as -1 if not known
		/// apriori. We round-robin over the set of disks (via the configured dirs)
		/// and select the first complete path which has enough space. A file is
		/// created on this directory. The file is guaranteed to go away when the
		/// JVM exits.
		/// </remarks>
		/// <param name="pathStr">prefix for the temporary file</param>
		/// <param name="size">the size of the file that is going to be written</param>
		/// <param name="conf">the Configuration object</param>
		/// <returns>a unique temporary file</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual FilePath CreateTmpFileForWrite(string pathStr, long size, Configuration
			 conf)
		{
			LocalDirAllocator.AllocatorPerContext context = ObtainContext(contextCfgItemName);
			return context.CreateTmpFileForWrite(pathStr, size, conf);
		}

		/// <summary>Method to check whether a context is valid</summary>
		/// <param name="contextCfgItemName"/>
		/// <returns>true/false</returns>
		public static bool IsContextValid(string contextCfgItemName)
		{
			lock (contexts)
			{
				return contexts.Contains(contextCfgItemName);
			}
		}

		/// <summary>Removes the context from the context config items</summary>
		/// <param name="contextCfgItemName"/>
		[Obsolete]
		public static void RemoveContext(string contextCfgItemName)
		{
			lock (contexts)
			{
				Sharpen.Collections.Remove(contexts, contextCfgItemName);
			}
		}

		/// <summary>
		/// We search through all the configured dirs for the file's existence
		/// and return true when we find
		/// </summary>
		/// <param name="pathStr">the requested file (this will be searched)</param>
		/// <param name="conf">the Configuration object</param>
		/// <returns>true if files exist. false otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool IfExists(string pathStr, Configuration conf)
		{
			LocalDirAllocator.AllocatorPerContext context = ObtainContext(contextCfgItemName);
			return context.IfExists(pathStr, conf);
		}

		/// <summary>Get the current directory index for the given configuration item.</summary>
		/// <returns>the current directory index for the given configuration item.</returns>
		internal virtual int GetCurrentDirectoryIndex()
		{
			LocalDirAllocator.AllocatorPerContext context = ObtainContext(contextCfgItemName);
			return context.GetCurrentDirectoryIndex();
		}

		private class AllocatorPerContext
		{
			private readonly Log Log = LogFactory.GetLog(typeof(LocalDirAllocator.AllocatorPerContext
				));

			private int dirNumLastAccessed;

			private Random dirIndexRandomizer = new Random();

			private FileSystem localFS;

			private DF[] dirDF;

			private string contextCfgItemName;

			private string[] localDirs;

			private string savedLocalDirs = string.Empty;

			public AllocatorPerContext(string contextCfgItemName)
			{
				this.contextCfgItemName = contextCfgItemName;
			}

			/// <summary>
			/// This method gets called everytime before any read/write to make sure
			/// that any change to localDirs is reflected immediately.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			private void ConfChanged(Configuration conf)
			{
				lock (this)
				{
					string newLocalDirs = conf.Get(contextCfgItemName);
					if (!newLocalDirs.Equals(savedLocalDirs))
					{
						localDirs = StringUtils.GetTrimmedStrings(newLocalDirs);
						localFS = FileSystem.GetLocal(conf);
						int numDirs = localDirs.Length;
						AList<string> dirs = new AList<string>(numDirs);
						AList<DF> dfList = new AList<DF>(numDirs);
						for (int i = 0; i < numDirs; i++)
						{
							try
							{
								// filter problematic directories
								Path tmpDir = new Path(localDirs[i]);
								if (localFS.Mkdirs(tmpDir) || localFS.Exists(tmpDir))
								{
									try
									{
										FilePath tmpFile = tmpDir.IsAbsolute() ? new FilePath(localFS.MakeQualified(tmpDir
											).ToUri()) : new FilePath(localDirs[i]);
										DiskChecker.CheckDir(tmpFile);
										dirs.AddItem(tmpFile.GetPath());
										dfList.AddItem(new DF(tmpFile, 30000));
									}
									catch (DiskChecker.DiskErrorException de)
									{
										Log.Warn(localDirs[i] + " is not writable\n", de);
									}
								}
								else
								{
									Log.Warn("Failed to create " + localDirs[i]);
								}
							}
							catch (IOException ie)
							{
								Log.Warn("Failed to create " + localDirs[i] + ": " + ie.Message + "\n", ie);
							}
						}
						//ignore
						localDirs = Sharpen.Collections.ToArray(dirs, new string[dirs.Count]);
						dirDF = Sharpen.Collections.ToArray(dfList, new DF[dirs.Count]);
						savedLocalDirs = newLocalDirs;
						// randomize the first disk picked in the round-robin selection 
						dirNumLastAccessed = dirIndexRandomizer.Next(dirs.Count);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private Path CreatePath(string path, bool checkWrite)
			{
				Path file = new Path(new Path(localDirs[dirNumLastAccessed]), path);
				if (checkWrite)
				{
					//check whether we are able to create a directory here. If the disk
					//happens to be RDONLY we will fail
					try
					{
						DiskChecker.CheckDir(new FilePath(file.GetParent().ToUri().GetPath()));
						return file;
					}
					catch (DiskChecker.DiskErrorException d)
					{
						Log.Warn("Disk Error Exception: ", d);
						return null;
					}
				}
				return file;
			}

			/// <summary>Get the current directory index.</summary>
			/// <returns>the current directory index.</returns>
			internal virtual int GetCurrentDirectoryIndex()
			{
				return dirNumLastAccessed;
			}

			/// <summary>Get a path from the local FS.</summary>
			/// <remarks>
			/// Get a path from the local FS. If size is known, we go
			/// round-robin over the set of disks (via the configured dirs) and return
			/// the first complete path which has enough space.
			/// If size is not known, use roulette selection -- pick directories
			/// with probability proportional to their available space.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual Path GetLocalPathForWrite(string pathStr, long size, Configuration
				 conf, bool checkWrite)
			{
				lock (this)
				{
					ConfChanged(conf);
					int numDirs = localDirs.Length;
					int numDirsSearched = 0;
					//remove the leading slash from the path (to make sure that the uri
					//resolution results in a valid path on the dir being checked)
					if (pathStr.StartsWith("/"))
					{
						pathStr = Sharpen.Runtime.Substring(pathStr, 1);
					}
					Path returnPath = null;
					if (size == SizeUnknown)
					{
						//do roulette selection: pick dir with probability 
						//proportional to available size
						long[] availableOnDisk = new long[dirDF.Length];
						long totalAvailable = 0;
						//build the "roulette wheel"
						for (int i = 0; i < dirDF.Length; ++i)
						{
							availableOnDisk[i] = dirDF[i].GetAvailable();
							totalAvailable += availableOnDisk[i];
						}
						if (totalAvailable == 0)
						{
							throw new DiskChecker.DiskErrorException("No space available in any of the local directories."
								);
						}
						// Keep rolling the wheel till we get a valid path
						Random r = new Random();
						while (numDirsSearched < numDirs && returnPath == null)
						{
							long randomPosition = ((long)(((ulong)r.NextLong()) >> 1)) % totalAvailable;
							int dir = 0;
							while (randomPosition > availableOnDisk[dir])
							{
								randomPosition -= availableOnDisk[dir];
								dir++;
							}
							dirNumLastAccessed = dir;
							returnPath = CreatePath(pathStr, checkWrite);
							if (returnPath == null)
							{
								totalAvailable -= availableOnDisk[dir];
								availableOnDisk[dir] = 0;
								// skip this disk
								numDirsSearched++;
							}
						}
					}
					else
					{
						while (numDirsSearched < numDirs && returnPath == null)
						{
							long capacity = dirDF[dirNumLastAccessed].GetAvailable();
							if (capacity > size)
							{
								returnPath = CreatePath(pathStr, checkWrite);
							}
							dirNumLastAccessed++;
							dirNumLastAccessed = dirNumLastAccessed % numDirs;
							numDirsSearched++;
						}
					}
					if (returnPath != null)
					{
						return returnPath;
					}
					//no path found
					throw new DiskChecker.DiskErrorException("Could not find any valid local " + "directory for "
						 + pathStr);
				}
			}

			/// <summary>Creates a file on the local FS.</summary>
			/// <remarks>
			/// Creates a file on the local FS. Pass size as
			/// <see cref="LocalDirAllocator.SIZE_UNKNOWN"/>
			/// if not known apriori. We
			/// round-robin over the set of disks (via the configured dirs) and return
			/// a file on the first path which has enough space. The file is guaranteed
			/// to go away when the JVM exits.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual FilePath CreateTmpFileForWrite(string pathStr, long size, Configuration
				 conf)
			{
				// find an appropriate directory
				Path path = GetLocalPathForWrite(pathStr, size, conf, true);
				FilePath dir = new FilePath(path.GetParent().ToUri().GetPath());
				string prefix = path.GetName();
				// create a temp file on this directory
				FilePath result = FilePath.CreateTempFile(prefix, null, dir);
				result.DeleteOnExit();
				return result;
			}

			/// <summary>Get a path from the local FS for reading.</summary>
			/// <remarks>
			/// Get a path from the local FS for reading. We search through all the
			/// configured dirs for the file's existence and return the complete
			/// path to the file when we find one
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual Path GetLocalPathToRead(string pathStr, Configuration conf)
			{
				lock (this)
				{
					ConfChanged(conf);
					int numDirs = localDirs.Length;
					int numDirsSearched = 0;
					//remove the leading slash from the path (to make sure that the uri
					//resolution results in a valid path on the dir being checked)
					if (pathStr.StartsWith("/"))
					{
						pathStr = Sharpen.Runtime.Substring(pathStr, 1);
					}
					while (numDirsSearched < numDirs)
					{
						Path file = new Path(localDirs[numDirsSearched], pathStr);
						if (localFS.Exists(file))
						{
							return file;
						}
						numDirsSearched++;
					}
					//no path found
					throw new DiskChecker.DiskErrorException("Could not find " + pathStr + " in any of"
						 + " the configured local directories");
				}
			}

			private class PathIterator : IEnumerator<Path>, IEnumerable<Path>
			{
				private readonly FileSystem fs;

				private readonly string pathStr;

				private int i = 0;

				private readonly string[] rootDirs;

				private Path next = null;

				/// <exception cref="System.IO.IOException"/>
				private PathIterator(FileSystem fs, string pathStr, string[] rootDirs)
				{
					this.fs = fs;
					this.pathStr = pathStr;
					this.rootDirs = rootDirs;
					Advance();
				}

				public virtual bool HasNext()
				{
					return next != null;
				}

				/// <exception cref="System.IO.IOException"/>
				private void Advance()
				{
					while (i < rootDirs.Length)
					{
						next = new Path(rootDirs[i++], pathStr);
						if (fs.Exists(next))
						{
							return;
						}
					}
					next = null;
				}

				public virtual Path Next()
				{
					Path result = next;
					try
					{
						Advance();
					}
					catch (IOException ie)
					{
						throw new RuntimeException("Can't check existance of " + next, ie);
					}
					if (result == null)
					{
						throw new NoSuchElementException();
					}
					return result;
				}

				public virtual void Remove()
				{
					throw new NotSupportedException("read only iterator");
				}

				public virtual IEnumerator<Path> GetEnumerator()
				{
					return this;
				}
			}

			/// <summary>Get all of the paths that currently exist in the working directories.</summary>
			/// <param name="pathStr">the path underneath the roots</param>
			/// <param name="conf">the configuration to look up the roots in</param>
			/// <returns>all of the paths that exist under any of the roots</returns>
			/// <exception cref="System.IO.IOException"/>
			internal virtual IEnumerable<Path> GetAllLocalPathsToRead(string pathStr, Configuration
				 conf)
			{
				lock (this)
				{
					ConfChanged(conf);
					if (pathStr.StartsWith("/"))
					{
						pathStr = Sharpen.Runtime.Substring(pathStr, 1);
					}
					return new LocalDirAllocator.AllocatorPerContext.PathIterator(localFS, pathStr, localDirs
						);
				}
			}

			/// <summary>
			/// We search through all the configured dirs for the file's existence
			/// and return true when we find one
			/// </summary>
			public virtual bool IfExists(string pathStr, Configuration conf)
			{
				lock (this)
				{
					try
					{
						int numDirs = localDirs.Length;
						int numDirsSearched = 0;
						//remove the leading slash from the path (to make sure that the uri
						//resolution results in a valid path on the dir being checked)
						if (pathStr.StartsWith("/"))
						{
							pathStr = Sharpen.Runtime.Substring(pathStr, 1);
						}
						while (numDirsSearched < numDirs)
						{
							Path file = new Path(localDirs[numDirsSearched], pathStr);
							if (localFS.Exists(file))
							{
								return true;
							}
							numDirsSearched++;
						}
					}
					catch (IOException)
					{
					}
					// IGNORE and try again
					return false;
				}
			}
		}
	}
}
