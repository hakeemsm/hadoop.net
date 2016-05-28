using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// A base class for file-based
	/// <see cref="InputFormat{K, V}"/>
	/// .
	/// <p><code>FileInputFormat</code> is the base class for all file-based
	/// <code>InputFormat</code>s. This provides a generic implementation of
	/// <see cref="FileInputFormat{K, V}.GetSplits(JobConf, int)"/>
	/// .
	/// Subclasses of <code>FileInputFormat</code> can also override the
	/// <see cref="FileInputFormat{K, V}.IsSplitable(Org.Apache.Hadoop.FS.FileSystem, Org.Apache.Hadoop.FS.Path)
	/// 	"/>
	/// method to ensure input-files are
	/// not split-up and are processed as a whole by
	/// <see cref="Mapper{K1, V1, K2, V2}"/>
	/// s.
	/// </summary>
	public abstract class FileInputFormat<K, V> : InputFormat<K, V>
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(FileInputFormat));

		public enum Counter
		{
			BytesRead
		}

		public const string NumInputFiles = FileInputFormat.NumInputFiles;

		public const string InputDirRecursive = FileInputFormat.InputDirRecursive;

		private const double SplitSlop = 1.1;

		private long minSplitSize = 1;

		private sealed class _PathFilter_86 : PathFilter
		{
			public _PathFilter_86()
			{
			}

			// 10% slop
			public bool Accept(Path p)
			{
				string name = p.GetName();
				return !name.StartsWith("_") && !name.StartsWith(".");
			}
		}

		private static readonly PathFilter hiddenFileFilter = new _PathFilter_86();

		protected internal virtual void SetMinSplitSize(long minSplitSize)
		{
			this.minSplitSize = minSplitSize;
		}

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
		/// <see cref="Mapper{K1, V1, K2, V2}"/>
		/// s process entire files.
		/// </remarks>
		/// <param name="fs">the file system that the file is on</param>
		/// <param name="filename">the file name to check</param>
		/// <returns>is this file splitable?</returns>
		protected internal virtual bool IsSplitable(FileSystem fs, Path filename)
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public abstract RecordReader<K, V> GetRecordReader(InputSplit split, JobConf job, 
			Reporter reporter);

		/// <summary>Set a PathFilter to be applied to the input paths for the map-reduce job.
		/// 	</summary>
		/// <param name="filter">the PathFilter class use for filtering the input paths.</param>
		public static void SetInputPathFilter(JobConf conf, Type filter)
		{
			conf.SetClass(FileInputFormat.PathfilterClass, filter, typeof(PathFilter));
		}

		/// <summary>Get a PathFilter instance of the filter set for the input paths.</summary>
		/// <returns>the PathFilter instance set for the job, NULL if none has been set.</returns>
		public static PathFilter GetInputPathFilter(JobConf conf)
		{
			Type filterClass = conf.GetClass<PathFilter>(FileInputFormat.PathfilterClass, null
				);
			return (filterClass != null) ? ReflectionUtils.NewInstance(filterClass, conf) : null;
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

		/// <summary>List input directories.</summary>
		/// <remarks>
		/// List input directories.
		/// Subclasses may override to, e.g., select only files matching a regular
		/// expression.
		/// </remarks>
		/// <param name="job">the job to list input paths for</param>
		/// <returns>array of FileStatus objects</returns>
		/// <exception cref="System.IO.IOException">if zero items.</exception>
		protected internal virtual FileStatus[] ListStatus(JobConf job)
		{
			Path[] dirs = GetInputPaths(job);
			if (dirs.Length == 0)
			{
				throw new IOException("No input paths specified in job");
			}
			// get tokens for all the required FileSystems..
			TokenCache.ObtainTokensForNamenodes(job.GetCredentials(), dirs, job);
			// Whether we need to recursive look into the directory structure
			bool recursive = job.GetBoolean(InputDirRecursive, false);
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
			FileStatus[] result;
			int numThreads = job.GetInt(FileInputFormat.ListStatusNumThreads, FileInputFormat
				.DefaultListStatusNumThreads);
			StopWatch sw = new StopWatch().Start();
			if (numThreads == 1)
			{
				IList<FileStatus> locatedFiles = SingleThreadedListStatus(job, dirs, inputFilter, 
					recursive);
				result = Sharpen.Collections.ToArray(locatedFiles, new FileStatus[locatedFiles.Count
					]);
			}
			else
			{
				IEnumerable<FileStatus> locatedFiles = null;
				try
				{
					LocatedFileStatusFetcher locatedFileStatusFetcher = new LocatedFileStatusFetcher(
						job, dirs, recursive, inputFilter, false);
					locatedFiles = locatedFileStatusFetcher.GetFileStatuses();
				}
				catch (Exception)
				{
					throw new IOException("Interrupted while getting file statuses");
				}
				result = Iterables.ToArray<FileStatus>(locatedFiles);
			}
			sw.Stop();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Time taken to get FileStatuses: " + sw.Now(TimeUnit.Milliseconds));
			}
			Log.Info("Total input paths to process : " + result.Length);
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		private IList<FileStatus> SingleThreadedListStatus(JobConf job, Path[] dirs, PathFilter
			 inputFilter, bool recursive)
		{
			IList<FileStatus> result = new AList<FileStatus>();
			IList<IOException> errors = new AList<IOException>();
			foreach (Path p in dirs)
			{
				FileSystem fs = p.GetFileSystem(job);
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

		/// <summary>
		/// Splits files returned by
		/// <see cref="FileInputFormat{K, V}.ListStatus(JobConf)"/>
		/// when
		/// they're too big.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual InputSplit[] GetSplits(JobConf job, int numSplits)
		{
			StopWatch sw = new StopWatch().Start();
			FileStatus[] files = ListStatus(job);
			// Save the number of input files for metrics/loadgen
			job.SetLong(NumInputFiles, files.Length);
			long totalSize = 0;
			// compute total size
			foreach (FileStatus file in files)
			{
				// check we have valid files
				if (file.IsDirectory())
				{
					throw new IOException("Not a file: " + file.GetPath());
				}
				totalSize += file.GetLen();
			}
			long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
			long minSize = Math.Max(job.GetLong(FileInputFormat.SplitMinsize, 1), minSplitSize
				);
			// generate splits
			AList<FileSplit> splits = new AList<FileSplit>(numSplits);
			NetworkTopology clusterMap = new NetworkTopology();
			foreach (FileStatus file_1 in files)
			{
				Path path = file_1.GetPath();
				long length = file_1.GetLen();
				if (length != 0)
				{
					FileSystem fs = path.GetFileSystem(job);
					BlockLocation[] blkLocations;
					if (file_1 is LocatedFileStatus)
					{
						blkLocations = ((LocatedFileStatus)file_1).GetBlockLocations();
					}
					else
					{
						blkLocations = fs.GetFileBlockLocations(file_1, 0, length);
					}
					if (IsSplitable(fs, path))
					{
						long blockSize = file_1.GetBlockSize();
						long splitSize = ComputeSplitSize(goalSize, minSize, blockSize);
						long bytesRemaining = length;
						while (((double)bytesRemaining) / splitSize > SplitSlop)
						{
							string[][] splitHosts = GetSplitHostsAndCachedHosts(blkLocations, length - bytesRemaining
								, splitSize, clusterMap);
							splits.AddItem(MakeSplit(path, length - bytesRemaining, splitSize, splitHosts[0], 
								splitHosts[1]));
							bytesRemaining -= splitSize;
						}
						if (bytesRemaining != 0)
						{
							string[][] splitHosts = GetSplitHostsAndCachedHosts(blkLocations, length - bytesRemaining
								, bytesRemaining, clusterMap);
							splits.AddItem(MakeSplit(path, length - bytesRemaining, bytesRemaining, splitHosts
								[0], splitHosts[1]));
						}
					}
					else
					{
						string[][] splitHosts = GetSplitHostsAndCachedHosts(blkLocations, 0, length, clusterMap
							);
						splits.AddItem(MakeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
					}
				}
				else
				{
					//Create empty hosts array for zero length files
					splits.AddItem(MakeSplit(path, 0, length, new string[0]));
				}
			}
			sw.Stop();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Total # of splits generated by getSplits: " + splits.Count + ", TimeTaken: "
					 + sw.Now(TimeUnit.Milliseconds));
			}
			return Sharpen.Collections.ToArray(splits, new FileSplit[splits.Count]);
		}

		protected internal virtual long ComputeSplitSize(long goalSize, long minSize, long
			 blockSize)
		{
			return Math.Max(minSize, Math.Min(goalSize, blockSize));
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
		/// <param name="conf">Configuration of the job</param>
		/// <param name="commaSeparatedPaths">
		/// Comma separated paths to be set as
		/// the list of inputs for the map-reduce job.
		/// </param>
		public static void SetInputPaths(JobConf conf, string commaSeparatedPaths)
		{
			SetInputPaths(conf, StringUtils.StringToPath(GetPathStrings(commaSeparatedPaths))
				);
		}

		/// <summary>
		/// Add the given comma separated paths to the list of inputs for
		/// the map-reduce job.
		/// </summary>
		/// <param name="conf">The configuration of the job</param>
		/// <param name="commaSeparatedPaths">
		/// Comma separated paths to be added to
		/// the list of inputs for the map-reduce job.
		/// </param>
		public static void AddInputPaths(JobConf conf, string commaSeparatedPaths)
		{
			foreach (string str in GetPathStrings(commaSeparatedPaths))
			{
				AddInputPath(conf, new Path(str));
			}
		}

		/// <summary>
		/// Set the array of
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// s as the list of inputs
		/// for the map-reduce job.
		/// </summary>
		/// <param name="conf">Configuration of the job.</param>
		/// <param name="inputPaths">
		/// the
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// s of the input directories/files
		/// for the map-reduce job.
		/// </param>
		public static void SetInputPaths(JobConf conf, params Path[] inputPaths)
		{
			Path path = new Path(conf.GetWorkingDirectory(), inputPaths[0]);
			StringBuilder str = new StringBuilder(StringUtils.EscapeString(path.ToString()));
			for (int i = 1; i < inputPaths.Length; i++)
			{
				str.Append(StringUtils.CommaStr);
				path = new Path(conf.GetWorkingDirectory(), inputPaths[i]);
				str.Append(StringUtils.EscapeString(path.ToString()));
			}
			conf.Set(FileInputFormat.InputDir, str.ToString());
		}

		/// <summary>
		/// Add a
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// to the list of inputs for the map-reduce job.
		/// </summary>
		/// <param name="conf">The configuration of the job</param>
		/// <param name="path">
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// to be added to the list of inputs for
		/// the map-reduce job.
		/// </param>
		public static void AddInputPath(JobConf conf, Path path)
		{
			path = new Path(conf.GetWorkingDirectory(), path);
			string dirStr = StringUtils.EscapeString(path.ToString());
			string dirs = conf.Get(FileInputFormat.InputDir);
			conf.Set(FileInputFormat.InputDir, dirs == null ? dirStr : dirs + StringUtils.CommaStr
				 + dirStr);
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
		/// <param name="conf">The configuration of the job</param>
		/// <returns>
		/// the list of input
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// s for the map-reduce job.
		/// </returns>
		public static Path[] GetInputPaths(JobConf conf)
		{
			string dirs = conf.Get(FileInputFormat.InputDir, string.Empty);
			string[] list = StringUtils.Split(dirs);
			Path[] result = new Path[list.Length];
			for (int i = 0; i < list.Length; i++)
			{
				result[i] = new Path(StringUtils.UnEscapeString(list[i]));
			}
			return result;
		}

		private void SortInDescendingOrder(IList<FileInputFormat.NodeInfo> mylist)
		{
			mylist.Sort(new _IComparer_526());
		}

		private sealed class _IComparer_526 : IComparer<FileInputFormat.NodeInfo>
		{
			public _IComparer_526()
			{
			}

			public int Compare(FileInputFormat.NodeInfo obj1, FileInputFormat.NodeInfo obj2)
			{
				if (obj1 == null || obj2 == null)
				{
					return -1;
				}
				if (obj1.GetValue() == obj2.GetValue())
				{
					return 0;
				}
				else
				{
					return ((obj1.GetValue() < obj2.GetValue()) ? 1 : -1);
				}
			}
		}

		/// <summary>
		/// This function identifies and returns the hosts that contribute
		/// most for a given split.
		/// </summary>
		/// <remarks>
		/// This function identifies and returns the hosts that contribute
		/// most for a given split. For calculating the contribution, rack
		/// locality is treated on par with host locality, so hosts from racks
		/// that contribute the most are preferred over hosts on racks that
		/// contribute less
		/// </remarks>
		/// <param name="blkLocations">The list of block locations</param>
		/// <param name="offset"></param>
		/// <param name="splitSize"></param>
		/// <returns>an array of hosts that contribute most to this split</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual string[] GetSplitHosts(BlockLocation[] blkLocations, long
			 offset, long splitSize, NetworkTopology clusterMap)
		{
			return GetSplitHostsAndCachedHosts(blkLocations, offset, splitSize, clusterMap)[0
				];
		}

		/// <summary>
		/// This function identifies and returns the hosts that contribute
		/// most for a given split.
		/// </summary>
		/// <remarks>
		/// This function identifies and returns the hosts that contribute
		/// most for a given split. For calculating the contribution, rack
		/// locality is treated on par with host locality, so hosts from racks
		/// that contribute the most are preferred over hosts on racks that
		/// contribute less
		/// </remarks>
		/// <param name="blkLocations">The list of block locations</param>
		/// <param name="offset"></param>
		/// <param name="splitSize"></param>
		/// <returns>
		/// two arrays - one of hosts that contribute most to this split, and
		/// one of hosts that contribute most to this split that have the data
		/// cached on them
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		private string[][] GetSplitHostsAndCachedHosts(BlockLocation[] blkLocations, long
			 offset, long splitSize, NetworkTopology clusterMap)
		{
			int startIndex = GetBlockIndex(blkLocations, offset);
			long bytesInThisBlock = blkLocations[startIndex].GetOffset() + blkLocations[startIndex
				].GetLength() - offset;
			//If this is the only block, just return
			if (bytesInThisBlock >= splitSize)
			{
				return new string[][] { blkLocations[startIndex].GetHosts(), blkLocations[startIndex
					].GetCachedHosts() };
			}
			long bytesInFirstBlock = bytesInThisBlock;
			int index = startIndex + 1;
			splitSize -= bytesInThisBlock;
			while (splitSize > 0)
			{
				bytesInThisBlock = Math.Min(splitSize, blkLocations[index++].GetLength());
				splitSize -= bytesInThisBlock;
			}
			long bytesInLastBlock = bytesInThisBlock;
			int endIndex = index - 1;
			IDictionary<Node, FileInputFormat.NodeInfo> hostsMap = new IdentityHashMap<Node, 
				FileInputFormat.NodeInfo>();
			IDictionary<Node, FileInputFormat.NodeInfo> racksMap = new IdentityHashMap<Node, 
				FileInputFormat.NodeInfo>();
			string[] allTopos = new string[0];
			// Build the hierarchy and aggregate the contribution of 
			// bytes at each level. See TestGetSplitHosts.java 
			for (index = startIndex; index <= endIndex; index++)
			{
				// Establish the bytes in this block
				if (index == startIndex)
				{
					bytesInThisBlock = bytesInFirstBlock;
				}
				else
				{
					if (index == endIndex)
					{
						bytesInThisBlock = bytesInLastBlock;
					}
					else
					{
						bytesInThisBlock = blkLocations[index].GetLength();
					}
				}
				allTopos = blkLocations[index].GetTopologyPaths();
				// If no topology information is available, just
				// prefix a fakeRack
				if (allTopos.Length == 0)
				{
					allTopos = FakeRacks(blkLocations, index);
				}
				// NOTE: This code currently works only for one level of
				// hierarchy (rack/host). However, it is relatively easy
				// to extend this to support aggregation at different
				// levels 
				foreach (string topo in allTopos)
				{
					Node node;
					Node parentNode;
					FileInputFormat.NodeInfo nodeInfo;
					FileInputFormat.NodeInfo parentNodeInfo;
					node = clusterMap.GetNode(topo);
					if (node == null)
					{
						node = new NodeBase(topo);
						clusterMap.Add(node);
					}
					nodeInfo = hostsMap[node];
					if (nodeInfo == null)
					{
						nodeInfo = new FileInputFormat.NodeInfo(node);
						hostsMap[node] = nodeInfo;
						parentNode = node.GetParent();
						parentNodeInfo = racksMap[parentNode];
						if (parentNodeInfo == null)
						{
							parentNodeInfo = new FileInputFormat.NodeInfo(parentNode);
							racksMap[parentNode] = parentNodeInfo;
						}
						parentNodeInfo.AddLeaf(nodeInfo);
					}
					else
					{
						nodeInfo = hostsMap[node];
						parentNode = node.GetParent();
						parentNodeInfo = racksMap[parentNode];
					}
					nodeInfo.AddValue(index, bytesInThisBlock);
					parentNodeInfo.AddValue(index, bytesInThisBlock);
				}
			}
			// for all topos
			// for all indices
			// We don't yet support cached hosts when bytesInThisBlock > splitSize
			return new string[][] { IdentifyHosts(allTopos.Length, racksMap), new string[0] };
		}

		private string[] IdentifyHosts(int replicationFactor, IDictionary<Node, FileInputFormat.NodeInfo
			> racksMap)
		{
			string[] retVal = new string[replicationFactor];
			IList<FileInputFormat.NodeInfo> rackList = new List<FileInputFormat.NodeInfo>();
			Sharpen.Collections.AddAll(rackList, racksMap.Values);
			// Sort the racks based on their contribution to this split
			SortInDescendingOrder(rackList);
			bool done = false;
			int index = 0;
			// Get the host list for all our aggregated items, sort
			// them and return the top entries
			foreach (FileInputFormat.NodeInfo ni in rackList)
			{
				ICollection<FileInputFormat.NodeInfo> hostSet = ni.GetLeaves();
				IList<FileInputFormat.NodeInfo> hostList = new List<FileInputFormat.NodeInfo>();
				Sharpen.Collections.AddAll(hostList, hostSet);
				// Sort the hosts in this rack based on their contribution
				SortInDescendingOrder(hostList);
				foreach (FileInputFormat.NodeInfo host in hostList)
				{
					// Strip out the port number from the host name
					retVal[index++] = host.node.GetName().Split(":")[0];
					if (index == replicationFactor)
					{
						done = true;
						break;
					}
				}
				if (done == true)
				{
					break;
				}
			}
			return retVal;
		}

		/// <exception cref="System.IO.IOException"/>
		private string[] FakeRacks(BlockLocation[] blkLocations, int index)
		{
			string[] allHosts = blkLocations[index].GetHosts();
			string[] allTopos = new string[allHosts.Length];
			for (int i = 0; i < allHosts.Length; i++)
			{
				allTopos[i] = NetworkTopology.DefaultRack + "/" + allHosts[i];
			}
			return allTopos;
		}

		private class NodeInfo
		{
			internal readonly Node node;

			internal readonly ICollection<int> blockIds;

			internal readonly ICollection<FileInputFormat.NodeInfo> leaves;

			private long value;

			internal NodeInfo(Node node)
			{
				this.node = node;
				blockIds = new HashSet<int>();
				leaves = new HashSet<FileInputFormat.NodeInfo>();
			}

			internal virtual long GetValue()
			{
				return value;
			}

			internal virtual void AddValue(int blockIndex, long value)
			{
				if (blockIds.AddItem(blockIndex) == true)
				{
					this.value += value;
				}
			}

			internal virtual ICollection<FileInputFormat.NodeInfo> GetLeaves()
			{
				return leaves;
			}

			internal virtual void AddLeaf(FileInputFormat.NodeInfo nodeInfo)
			{
				leaves.AddItem(nodeInfo);
			}
		}
	}
}
