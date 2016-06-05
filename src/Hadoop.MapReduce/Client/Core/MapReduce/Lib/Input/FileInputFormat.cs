using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// A base class for file-based
	/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}"/>
	/// s.
	/// <p><code>FileInputFormat</code> is the base class for all file-based
	/// <code>InputFormat</code>s. This provides a generic implementation of
	/// <see cref="FileInputFormat{K, V}.GetSplits(Org.Apache.Hadoop.Mapreduce.JobContext)
	/// 	"/>
	/// .
	/// Subclasses of <code>FileInputFormat</code> can also override the
	/// <see cref="FileInputFormat{K, V}.IsSplitable(Org.Apache.Hadoop.Mapreduce.JobContext, Org.Apache.Hadoop.FS.Path)
	/// 	"/>
	/// method to ensure input-files are
	/// not split-up and are processed as a whole by
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
	/// 	>
	/// s.
	/// </summary>
	public abstract class FileInputFormat<K, V> : InputFormat<K, V>
	{
		public const string InputDir = "mapreduce.input.fileinputformat.inputdir";

		public const string SplitMaxsize = "mapreduce.input.fileinputformat.split.maxsize";

		public const string SplitMinsize = "mapreduce.input.fileinputformat.split.minsize";

		public const string PathfilterClass = "mapreduce.input.pathFilter.class";

		public const string NumInputFiles = "mapreduce.input.fileinputformat.numinputfiles";

		public const string InputDirRecursive = "mapreduce.input.fileinputformat.input.dir.recursive";

		public const string ListStatusNumThreads = "mapreduce.input.fileinputformat.list-status.num-threads";

		public const int DefaultListStatusNumThreads = 1;

		private static readonly Log Log = LogFactory.GetLog(typeof(FileInputFormat));

		private const double SplitSlop = 1.1;

		public enum Counter
		{
			BytesRead
		}

		private sealed class _PathFilter_90 : PathFilter
		{
			public _PathFilter_90()
			{
			}

			// 10% slop
			public bool Accept(Path p)
			{
				string name = p.GetName();
				return !name.StartsWith("_") && !name.StartsWith(".");
			}
		}

		private static readonly PathFilter hiddenFileFilter = new _PathFilter_90();

		/// <summary>
		/// Proxy PathFilter that accepts a path only if all filters given in the
		/// constructor do.
		/// </summary>
		/// <remarks>
		/// Proxy PathFilter that accepts a path only if all filters given in the
		/// constructor do. Used by the listPaths() to apply the built-in
		/// hiddenFileFilter together with a user provided one (if any).
		/// </remarks>
		private class MultiPathFilter : PathFilter
		{
			private IList<PathFilter> filters;

			public MultiPathFilter(IList<PathFilter> filters)
			{
				this.filters = filters;
			}

			public virtual bool Accept(Path path)
			{
				foreach (PathFilter filter in filters)
				{
					if (!filter.Accept(path))
					{
						return false;
					}
				}
				return true;
			}
		}

		/// <param name="job">the job to modify</param>
		/// <param name="inputDirRecursive"/>
		public static void SetInputDirRecursive(Job job, bool inputDirRecursive)
		{
			job.GetConfiguration().SetBoolean(InputDirRecursive, inputDirRecursive);
		}

		/// <param name="job">the job to look at.</param>
		/// <returns>should the files to be read recursively?</returns>
		public static bool GetInputDirRecursive(JobContext job)
		{
			return job.GetConfiguration().GetBoolean(InputDirRecursive, false);
		}

		/// <summary>Get the lower bound on split size imposed by the format.</summary>
		/// <returns>the number of bytes of the minimal split for this format</returns>
		protected internal virtual long GetFormatMinSplitSize()
		{
			return 1;
		}

		/// <summary>
		/// Is the given filename splitable? Usually, true, but if the file is
		/// stream compressed, it will not be.
		/// </summary>
		/// <remarks>
		/// Is the given filename splitable? Usually, true, but if the file is
		/// stream compressed, it will not be.
		/// <code>FileInputFormat</code> implementations can override this and return
		/// <code>false</code> to ensure that individual input files are never split-up
		/// so that
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
		/// 	>
		/// s process entire files.
		/// </remarks>
		/// <param name="context">the job context</param>
		/// <param name="filename">the file name to check</param>
		/// <returns>is this file splitable?</returns>
		protected internal virtual bool IsSplitable(JobContext context, Path filename)
		{
			return true;
		}

		/// <summary>Set a PathFilter to be applied to the input paths for the map-reduce job.
		/// 	</summary>
		/// <param name="job">the job to modify</param>
		/// <param name="filter">the PathFilter class use for filtering the input paths.</param>
		public static void SetInputPathFilter(Job job, Type filter)
		{
			job.GetConfiguration().SetClass(PathfilterClass, filter, typeof(PathFilter));
		}

		/// <summary>Set the minimum input split size</summary>
		/// <param name="job">the job to modify</param>
		/// <param name="size">the minimum size</param>
		public static void SetMinInputSplitSize(Job job, long size)
		{
			job.GetConfiguration().SetLong(SplitMinsize, size);
		}

		/// <summary>Get the minimum split size</summary>
		/// <param name="job">the job</param>
		/// <returns>the minimum number of bytes that can be in a split</returns>
		public static long GetMinSplitSize(JobContext job)
		{
			return job.GetConfiguration().GetLong(SplitMinsize, 1L);
		}

		/// <summary>Set the maximum split size</summary>
		/// <param name="job">the job to modify</param>
		/// <param name="size">the maximum split size</param>
		public static void SetMaxInputSplitSize(Job job, long size)
		{
			job.GetConfiguration().SetLong(SplitMaxsize, size);
		}

		/// <summary>Get the maximum split size.</summary>
		/// <param name="context">the job to look at.</param>
		/// <returns>the maximum number of bytes a split can include</returns>
		public static long GetMaxSplitSize(JobContext context)
		{
			return context.GetConfiguration().GetLong(SplitMaxsize, long.MaxValue);
		}

		/// <summary>Get a PathFilter instance of the filter set for the input paths.</summary>
		/// <returns>the PathFilter instance set for the job, NULL if none has been set.</returns>
		public static PathFilter GetInputPathFilter(JobContext context)
		{
			Configuration conf = context.GetConfiguration();
			Type filterClass = conf.GetClass<PathFilter>(PathfilterClass, null);
			return (filterClass != null) ? (PathFilter)ReflectionUtils.NewInstance(filterClass
				, conf) : null;
		}

		/// <summary>List input directories.</summary>
		/// <remarks>
		/// List input directories.
		/// Subclasses may override to, e.g., select only files matching a regular
		/// expression.
		/// </remarks>
		/// <param name="job">the job to list input paths for</param>
		/// <returns>array of FileStatus objects</returns>
		/// <exception cref="System.IO.IOException">if zero items.</exception>
		protected internal virtual IList<FileStatus> ListStatus(JobContext job)
		{
			Path[] dirs = GetInputPaths(job);
			if (dirs.Length == 0)
			{
				throw new IOException("No input paths specified in job");
			}
			// get tokens for all the required FileSystems..
			TokenCache.ObtainTokensForNamenodes(job.GetCredentials(), dirs, job.GetConfiguration
				());
			// Whether we need to recursive look into the directory structure
			bool recursive = GetInputDirRecursive(job);
			// creates a MultiPathFilter with the hiddenFileFilter and the
			// user provided one (if any).
			IList<PathFilter> filters = new AList<PathFilter>();
			filters.AddItem(hiddenFileFilter);
			PathFilter jobFilter = GetInputPathFilter(job);
			if (jobFilter != null)
			{
				filters.AddItem(jobFilter);
			}
			PathFilter inputFilter = new FileInputFormat.MultiPathFilter(filters);
			IList<FileStatus> result = null;
			int numThreads = job.GetConfiguration().GetInt(ListStatusNumThreads, DefaultListStatusNumThreads
				);
			StopWatch sw = new StopWatch().Start();
			if (numThreads == 1)
			{
				result = SingleThreadedListStatus(job, dirs, inputFilter, recursive);
			}
			else
			{
				IEnumerable<FileStatus> locatedFiles = null;
				try
				{
					LocatedFileStatusFetcher locatedFileStatusFetcher = new LocatedFileStatusFetcher(
						job.GetConfiguration(), dirs, recursive, inputFilter, true);
					locatedFiles = locatedFileStatusFetcher.GetFileStatuses();
				}
				catch (Exception)
				{
					throw new IOException("Interrupted while getting file statuses");
				}
				result = Lists.NewArrayList(locatedFiles);
			}
			sw.Stop();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Time taken to get FileStatuses: " + sw.Now(TimeUnit.Milliseconds));
			}
			Log.Info("Total input paths to process : " + result.Count);
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		private IList<FileStatus> SingleThreadedListStatus(JobContext job, Path[] dirs, PathFilter
			 inputFilter, bool recursive)
		{
			IList<FileStatus> result = new AList<FileStatus>();
			IList<IOException> errors = new AList<IOException>();
			for (int i = 0; i < dirs.Length; ++i)
			{
				Path p = dirs[i];
				FileSystem fs = p.GetFileSystem(job.GetConfiguration());
				FileStatus[] matches = fs.GlobStatus(p, inputFilter);
				if (matches == null)
				{
					errors.AddItem(new IOException("Input path does not exist: " + p));
				}
				else
				{
					if (matches.Length == 0)
					{
						errors.AddItem(new IOException("Input Pattern " + p + " matches 0 files"));
					}
					else
					{
						foreach (FileStatus globStat in matches)
						{
							if (globStat.IsDirectory())
							{
								RemoteIterator<LocatedFileStatus> iter = fs.ListLocatedStatus(globStat.GetPath());
								while (iter.HasNext())
								{
									LocatedFileStatus stat = iter.Next();
									if (inputFilter.Accept(stat.GetPath()))
									{
										if (recursive && stat.IsDirectory())
										{
											AddInputPathRecursively(result, fs, stat.GetPath(), inputFilter);
										}
										else
										{
											result.AddItem(stat);
										}
									}
								}
							}
							else
							{
								result.AddItem(globStat);
							}
						}
					}
				}
			}
			if (!errors.IsEmpty())
			{
				throw new InvalidInputException(errors);
			}
			return result;
		}

		/// <summary>Add files in the input path recursively into the results.</summary>
		/// <param name="result">The List to store all files.</param>
		/// <param name="fs">The FileSystem.</param>
		/// <param name="path">The input path.</param>
		/// <param name="inputFilter">The input filter that can be used to filter files/dirs.
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void AddInputPathRecursively(IList<FileStatus> result, 
			FileSystem fs, Path path, PathFilter inputFilter)
		{
			RemoteIterator<LocatedFileStatus> iter = fs.ListLocatedStatus(path);
			while (iter.HasNext())
			{
				LocatedFileStatus stat = iter.Next();
				if (inputFilter.Accept(stat.GetPath()))
				{
					if (stat.IsDirectory())
					{
						AddInputPathRecursively(result, fs, stat.GetPath(), inputFilter);
					}
					else
					{
						result.AddItem(stat);
					}
				}
			}
		}

		/// <summary>A factory that makes the split for this class.</summary>
		/// <remarks>
		/// A factory that makes the split for this class. It can be overridden
		/// by sub-classes to make sub-types
		/// </remarks>
		protected internal virtual FileSplit MakeSplit(Path file, long start, long length
			, string[] hosts)
		{
			return new FileSplit(file, start, length, hosts);
		}

		/// <summary>A factory that makes the split for this class.</summary>
		/// <remarks>
		/// A factory that makes the split for this class. It can be overridden
		/// by sub-classes to make sub-types
		/// </remarks>
		protected internal virtual FileSplit MakeSplit(Path file, long start, long length
			, string[] hosts, string[] inMemoryHosts)
		{
			return new FileSplit(file, start, length, hosts, inMemoryHosts);
		}

		/// <summary>Generate the list of files and make them into FileSplits.</summary>
		/// <param name="job">the job context</param>
		/// <exception cref="System.IO.IOException"/>
		public override IList<InputSplit> GetSplits(JobContext job)
		{
			StopWatch sw = new StopWatch().Start();
			long minSize = Math.Max(GetFormatMinSplitSize(), GetMinSplitSize(job));
			long maxSize = GetMaxSplitSize(job);
			// generate splits
			IList<InputSplit> splits = new AList<InputSplit>();
			IList<FileStatus> files = ListStatus(job);
			foreach (FileStatus file in files)
			{
				Path path = file.GetPath();
				long length = file.GetLen();
				if (length != 0)
				{
					BlockLocation[] blkLocations;
					if (file is LocatedFileStatus)
					{
						blkLocations = ((LocatedFileStatus)file).GetBlockLocations();
					}
					else
					{
						FileSystem fs = path.GetFileSystem(job.GetConfiguration());
						blkLocations = fs.GetFileBlockLocations(file, 0, length);
					}
					if (IsSplitable(job, path))
					{
						long blockSize = file.GetBlockSize();
						long splitSize = ComputeSplitSize(blockSize, minSize, maxSize);
						long bytesRemaining = length;
						while (((double)bytesRemaining) / splitSize > SplitSlop)
						{
							int blkIndex = GetBlockIndex(blkLocations, length - bytesRemaining);
							splits.AddItem(MakeSplit(path, length - bytesRemaining, splitSize, blkLocations[blkIndex
								].GetHosts(), blkLocations[blkIndex].GetCachedHosts()));
							bytesRemaining -= splitSize;
						}
						if (bytesRemaining != 0)
						{
							int blkIndex = GetBlockIndex(blkLocations, length - bytesRemaining);
							splits.AddItem(MakeSplit(path, length - bytesRemaining, bytesRemaining, blkLocations
								[blkIndex].GetHosts(), blkLocations[blkIndex].GetCachedHosts()));
						}
					}
					else
					{
						// not splitable
						splits.AddItem(MakeSplit(path, 0, length, blkLocations[0].GetHosts(), blkLocations
							[0].GetCachedHosts()));
					}
				}
				else
				{
					//Create empty hosts array for zero length files
					splits.AddItem(MakeSplit(path, 0, length, new string[0]));
				}
			}
			// Save the number of input files for metrics/loadgen
			job.GetConfiguration().SetLong(NumInputFiles, files.Count);
			sw.Stop();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Total # of splits generated by getSplits: " + splits.Count + ", TimeTaken: "
					 + sw.Now(TimeUnit.Milliseconds));
			}
			return splits;
		}

		protected internal virtual long ComputeSplitSize(long blockSize, long minSize, long
			 maxSize)
		{
			return Math.Max(minSize, Math.Min(maxSize, blockSize));
		}

		protected internal virtual int GetBlockIndex(BlockLocation[] blkLocations, long offset
			)
		{
			for (int i = 0; i < blkLocations.Length; i++)
			{
				// is the offset inside this block?
				if ((blkLocations[i].GetOffset() <= offset) && (offset < blkLocations[i].GetOffset
					() + blkLocations[i].GetLength()))
				{
					return i;
				}
			}
			BlockLocation last = blkLocations[blkLocations.Length - 1];
			long fileLength = last.GetOffset() + last.GetLength() - 1;
			throw new ArgumentException("Offset " + offset + " is outside of file (0.." + fileLength
				 + ")");
		}

		/// <summary>
		/// Sets the given comma separated paths as the list of inputs
		/// for the map-reduce job.
		/// </summary>
		/// <param name="job">the job</param>
		/// <param name="commaSeparatedPaths">
		/// Comma separated paths to be set as
		/// the list of inputs for the map-reduce job.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public static void SetInputPaths(Job job, string commaSeparatedPaths)
		{
			SetInputPaths(job, StringUtils.StringToPath(GetPathStrings(commaSeparatedPaths)));
		}

		/// <summary>
		/// Add the given comma separated paths to the list of inputs for
		/// the map-reduce job.
		/// </summary>
		/// <param name="job">The job to modify</param>
		/// <param name="commaSeparatedPaths">
		/// Comma separated paths to be added to
		/// the list of inputs for the map-reduce job.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public static void AddInputPaths(Job job, string commaSeparatedPaths)
		{
			foreach (string str in GetPathStrings(commaSeparatedPaths))
			{
				AddInputPath(job, new Path(str));
			}
		}

		/// <summary>
		/// Set the array of
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// s as the list of inputs
		/// for the map-reduce job.
		/// </summary>
		/// <param name="job">The job to modify</param>
		/// <param name="inputPaths">
		/// the
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// s of the input directories/files
		/// for the map-reduce job.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public static void SetInputPaths(Job job, params Path[] inputPaths)
		{
			Configuration conf = job.GetConfiguration();
			Path path = inputPaths[0].GetFileSystem(conf).MakeQualified(inputPaths[0]);
			StringBuilder str = new StringBuilder(StringUtils.EscapeString(path.ToString()));
			for (int i = 1; i < inputPaths.Length; i++)
			{
				str.Append(StringUtils.CommaStr);
				path = inputPaths[i].GetFileSystem(conf).MakeQualified(inputPaths[i]);
				str.Append(StringUtils.EscapeString(path.ToString()));
			}
			conf.Set(InputDir, str.ToString());
		}

		/// <summary>
		/// Add a
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// to the list of inputs for the map-reduce job.
		/// </summary>
		/// <param name="job">
		/// The
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Job"/>
		/// to modify
		/// </param>
		/// <param name="path">
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// to be added to the list of inputs for
		/// the map-reduce job.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public static void AddInputPath(Job job, Path path)
		{
			Configuration conf = job.GetConfiguration();
			path = path.GetFileSystem(conf).MakeQualified(path);
			string dirStr = StringUtils.EscapeString(path.ToString());
			string dirs = conf.Get(InputDir);
			conf.Set(InputDir, dirs == null ? dirStr : dirs + "," + dirStr);
		}

		// This method escapes commas in the glob pattern of the given paths.
		private static string[] GetPathStrings(string commaSeparatedPaths)
		{
			int length = commaSeparatedPaths.Length;
			int curlyOpen = 0;
			int pathStart = 0;
			bool globPattern = false;
			IList<string> pathStrings = new AList<string>();
			for (int i = 0; i < length; i++)
			{
				char ch = commaSeparatedPaths[i];
				switch (ch)
				{
					case '{':
					{
						curlyOpen++;
						if (!globPattern)
						{
							globPattern = true;
						}
						break;
					}

					case '}':
					{
						curlyOpen--;
						if (curlyOpen == 0 && globPattern)
						{
							globPattern = false;
						}
						break;
					}

					case ',':
					{
						if (!globPattern)
						{
							pathStrings.AddItem(Sharpen.Runtime.Substring(commaSeparatedPaths, pathStart, i));
							pathStart = i + 1;
						}
						break;
					}

					default:
					{
						continue;
					}
				}
			}
			// nothing special to do for this character
			pathStrings.AddItem(Sharpen.Runtime.Substring(commaSeparatedPaths, pathStart, length
				));
			return Sharpen.Collections.ToArray(pathStrings, new string[0]);
		}

		/// <summary>
		/// Get the list of input
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// s for the map-reduce job.
		/// </summary>
		/// <param name="context">The job</param>
		/// <returns>
		/// the list of input
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// s for the map-reduce job.
		/// </returns>
		public static Path[] GetInputPaths(JobContext context)
		{
			string dirs = context.GetConfiguration().Get(InputDir, string.Empty);
			string[] list = StringUtils.Split(dirs);
			Path[] result = new Path[list.Length];
			for (int i = 0; i < list.Length; i++)
			{
				result[i] = new Path(StringUtils.UnEscapeString(list[i]));
			}
			return result;
		}
	}
}
