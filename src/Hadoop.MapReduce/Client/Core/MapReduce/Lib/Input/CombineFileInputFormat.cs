using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// An abstract
	/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}"/>
	/// that returns
	/// <see cref="CombineFileSplit"/>
	/// 's in
	/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}.GetSplits(Org.Apache.Hadoop.Mapreduce.JobContext)
	/// 	"/>
	/// method.
	/// Splits are constructed from the files under the input paths.
	/// A split cannot have files from different pools.
	/// Each split returned may contain blocks from different files.
	/// If a maxSplitSize is specified, then blocks on the same node are
	/// combined to form a single split. Blocks that are left over are
	/// then combined with other blocks in the same rack.
	/// If maxSplitSize is not specified, then blocks from the same rack
	/// are combined in a single split; no attempt is made to create
	/// node-local splits.
	/// If the maxSplitSize is equal to the block size, then this class
	/// is similar to the default splitting behavior in Hadoop: each
	/// block is a locally processed split.
	/// Subclasses implement
	/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}.CreateRecordReader(Org.Apache.Hadoop.Mapreduce.InputSplit, Org.Apache.Hadoop.Mapreduce.TaskAttemptContext)
	/// 	"/>
	/// to construct <code>RecordReader</code>'s for
	/// <code>CombineFileSplit</code>'s.
	/// </summary>
	/// <seealso cref="CombineFileSplit"/>
	public abstract class CombineFileInputFormat<K, V> : FileInputFormat<K, V>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.Input.CombineFileInputFormat
			));

		public const string SplitMinsizePernode = "mapreduce.input.fileinputformat.split.minsize.per.node";

		public const string SplitMinsizePerrack = "mapreduce.input.fileinputformat.split.minsize.per.rack";

		private long maxSplitSize = 0;

		private long minSplitSizeNode = 0;

		private long minSplitSizeRack = 0;

		private AList<CombineFileInputFormat.MultiPathFilter> pools = new AList<CombineFileInputFormat.MultiPathFilter
			>();

		private Dictionary<string, ICollection<string>> rackToNodes = new Dictionary<string
			, ICollection<string>>();

		// ability to limit the size of a single split
		// A pool of input paths filters. A split cannot have blocks from files
		// across multiple pools.
		// mapping from a rack name to the set of Nodes in the rack 
		/// <summary>Specify the maximum size (in bytes) of each split.</summary>
		/// <remarks>
		/// Specify the maximum size (in bytes) of each split. Each split is
		/// approximately equal to the specified size.
		/// </remarks>
		protected internal virtual void SetMaxSplitSize(long maxSplitSize)
		{
			this.maxSplitSize = maxSplitSize;
		}

		/// <summary>Specify the minimum size (in bytes) of each split per node.</summary>
		/// <remarks>
		/// Specify the minimum size (in bytes) of each split per node.
		/// This applies to data that is left over after combining data on a single
		/// node into splits that are of maximum size specified by maxSplitSize.
		/// This leftover data will be combined into its own split if its size
		/// exceeds minSplitSizeNode.
		/// </remarks>
		protected internal virtual void SetMinSplitSizeNode(long minSplitSizeNode)
		{
			this.minSplitSizeNode = minSplitSizeNode;
		}

		/// <summary>Specify the minimum size (in bytes) of each split per rack.</summary>
		/// <remarks>
		/// Specify the minimum size (in bytes) of each split per rack.
		/// This applies to data that is left over after combining data on a single
		/// rack into splits that are of maximum size specified by maxSplitSize.
		/// This leftover data will be combined into its own split if its size
		/// exceeds minSplitSizeRack.
		/// </remarks>
		protected internal virtual void SetMinSplitSizeRack(long minSplitSizeRack)
		{
			this.minSplitSizeRack = minSplitSizeRack;
		}

		/// <summary>Create a new pool and add the filters to it.</summary>
		/// <remarks>
		/// Create a new pool and add the filters to it.
		/// A split cannot have files from different pools.
		/// </remarks>
		protected internal virtual void CreatePool(IList<PathFilter> filters)
		{
			pools.AddItem(new CombineFileInputFormat.MultiPathFilter(filters));
		}

		/// <summary>Create a new pool and add the filters to it.</summary>
		/// <remarks>
		/// Create a new pool and add the filters to it.
		/// A pathname can satisfy any one of the specified filters.
		/// A split cannot have files from different pools.
		/// </remarks>
		protected internal virtual void CreatePool(params PathFilter[] filters)
		{
			CombineFileInputFormat.MultiPathFilter multi = new CombineFileInputFormat.MultiPathFilter
				();
			foreach (PathFilter f in filters)
			{
				multi.Add(f);
			}
			pools.AddItem(multi);
		}

		protected internal override bool IsSplitable(JobContext context, Path file)
		{
			CompressionCodec codec = new CompressionCodecFactory(context.GetConfiguration()).
				GetCodec(file);
			if (null == codec)
			{
				return true;
			}
			return codec is SplittableCompressionCodec;
		}

		/// <summary>default constructor</summary>
		public CombineFileInputFormat()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<InputSplit> GetSplits(JobContext job)
		{
			long minSizeNode = 0;
			long minSizeRack = 0;
			long maxSize = 0;
			Configuration conf = job.GetConfiguration();
			// the values specified by setxxxSplitSize() takes precedence over the
			// values that might have been specified in the config
			if (minSplitSizeNode != 0)
			{
				minSizeNode = minSplitSizeNode;
			}
			else
			{
				minSizeNode = conf.GetLong(SplitMinsizePernode, 0);
			}
			if (minSplitSizeRack != 0)
			{
				minSizeRack = minSplitSizeRack;
			}
			else
			{
				minSizeRack = conf.GetLong(SplitMinsizePerrack, 0);
			}
			if (maxSplitSize != 0)
			{
				maxSize = maxSplitSize;
			}
			else
			{
				maxSize = conf.GetLong("mapreduce.input.fileinputformat.split.maxsize", 0);
			}
			// If maxSize is not configured, a single split will be generated per
			// node.
			if (minSizeNode != 0 && maxSize != 0 && minSizeNode > maxSize)
			{
				throw new IOException("Minimum split size pernode " + minSizeNode + " cannot be larger than maximum split size "
					 + maxSize);
			}
			if (minSizeRack != 0 && maxSize != 0 && minSizeRack > maxSize)
			{
				throw new IOException("Minimum split size per rack " + minSizeRack + " cannot be larger than maximum split size "
					 + maxSize);
			}
			if (minSizeRack != 0 && minSizeNode > minSizeRack)
			{
				throw new IOException("Minimum split size per node " + minSizeNode + " cannot be larger than minimum split "
					 + "size per rack " + minSizeRack);
			}
			// all the files in input set
			IList<FileStatus> stats = ListStatus(job);
			IList<InputSplit> splits = new AList<InputSplit>();
			if (stats.Count == 0)
			{
				return splits;
			}
			// In one single iteration, process all the paths in a single pool.
			// Processing one pool at a time ensures that a split contains paths
			// from a single pool only.
			foreach (CombineFileInputFormat.MultiPathFilter onepool in pools)
			{
				AList<FileStatus> myPaths = new AList<FileStatus>();
				// pick one input path. If it matches all the filters in a pool,
				// add it to the output set
				for (IEnumerator<FileStatus> iter = stats.GetEnumerator(); iter.HasNext(); )
				{
					FileStatus p = iter.Next();
					if (onepool.Accept(p.GetPath()))
					{
						myPaths.AddItem(p);
						// add it to my output set
						iter.Remove();
					}
				}
				// create splits for all files in this pool.
				GetMoreSplits(job, myPaths, maxSize, minSizeNode, minSizeRack, splits);
			}
			// create splits for all files that are not in any pool.
			GetMoreSplits(job, stats, maxSize, minSizeNode, minSizeRack, splits);
			// free up rackToNodes map
			rackToNodes.Clear();
			return splits;
		}

		/// <summary>Return all the splits in the specified set of paths</summary>
		/// <exception cref="System.IO.IOException"/>
		private void GetMoreSplits(JobContext job, IList<FileStatus> stats, long maxSize, 
			long minSizeNode, long minSizeRack, IList<InputSplit> splits)
		{
			Configuration conf = job.GetConfiguration();
			// all blocks for all the files in input set
			CombineFileInputFormat.OneFileInfo[] files;
			// mapping from a rack name to the list of blocks it has
			Dictionary<string, IList<CombineFileInputFormat.OneBlockInfo>> rackToBlocks = new 
				Dictionary<string, IList<CombineFileInputFormat.OneBlockInfo>>();
			// mapping from a block to the nodes on which it has replicas
			Dictionary<CombineFileInputFormat.OneBlockInfo, string[]> blockToNodes = new Dictionary
				<CombineFileInputFormat.OneBlockInfo, string[]>();
			// mapping from a node to the list of blocks that it contains
			Dictionary<string, ICollection<CombineFileInputFormat.OneBlockInfo>> nodeToBlocks
				 = new Dictionary<string, ICollection<CombineFileInputFormat.OneBlockInfo>>();
			files = new CombineFileInputFormat.OneFileInfo[stats.Count];
			if (stats.Count == 0)
			{
				return;
			}
			// populate all the blocks for all files
			long totLength = 0;
			int i = 0;
			foreach (FileStatus stat in stats)
			{
				files[i] = new CombineFileInputFormat.OneFileInfo(stat, conf, IsSplitable(job, stat
					.GetPath()), rackToBlocks, blockToNodes, nodeToBlocks, rackToNodes, maxSize);
				totLength += files[i].GetLength();
			}
			CreateSplits(nodeToBlocks, blockToNodes, rackToBlocks, totLength, maxSize, minSizeNode
				, minSizeRack, splits);
		}

		[VisibleForTesting]
		internal virtual void CreateSplits(IDictionary<string, ICollection<CombineFileInputFormat.OneBlockInfo
			>> nodeToBlocks, IDictionary<CombineFileInputFormat.OneBlockInfo, string[]> blockToNodes
			, IDictionary<string, IList<CombineFileInputFormat.OneBlockInfo>> rackToBlocks, 
			long totLength, long maxSize, long minSizeNode, long minSizeRack, IList<InputSplit
			> splits)
		{
			AList<CombineFileInputFormat.OneBlockInfo> validBlocks = new AList<CombineFileInputFormat.OneBlockInfo
				>();
			long curSplitSize = 0;
			int totalNodes = nodeToBlocks.Count;
			long totalLength = totLength;
			Multiset<string> splitsPerNode = HashMultiset.Create();
			ICollection<string> completedNodes = new HashSet<string>();
			while (true)
			{
				// it is allowed for maxSize to be 0. Disable smoothing load for such cases
				// process all nodes and create splits that are local to a node. Generate
				// one split per node iteration, and walk over nodes multiple times to
				// distribute the splits across nodes. 
				for (IEnumerator<KeyValuePair<string, ICollection<CombineFileInputFormat.OneBlockInfo
					>>> iter = nodeToBlocks.GetEnumerator(); iter.HasNext(); )
				{
					KeyValuePair<string, ICollection<CombineFileInputFormat.OneBlockInfo>> one = iter
						.Next();
					string node = one.Key;
					// Skip the node if it has previously been marked as completed.
					if (completedNodes.Contains(node))
					{
						continue;
					}
					ICollection<CombineFileInputFormat.OneBlockInfo> blocksInCurrentNode = one.Value;
					// for each block, copy it into validBlocks. Delete it from
					// blockToNodes so that the same block does not appear in
					// two different splits.
					IEnumerator<CombineFileInputFormat.OneBlockInfo> oneBlockIter = blocksInCurrentNode
						.GetEnumerator();
					while (oneBlockIter.HasNext())
					{
						CombineFileInputFormat.OneBlockInfo oneblock = oneBlockIter.Next();
						// Remove all blocks which may already have been assigned to other
						// splits.
						if (!blockToNodes.Contains(oneblock))
						{
							oneBlockIter.Remove();
							continue;
						}
						validBlocks.AddItem(oneblock);
						Sharpen.Collections.Remove(blockToNodes, oneblock);
						curSplitSize += oneblock.length;
						// if the accumulated split size exceeds the maximum, then
						// create this split.
						if (maxSize != 0 && curSplitSize >= maxSize)
						{
							// create an input split and add it to the splits array
							AddCreatedSplit(splits, Sharpen.Collections.Singleton(node), validBlocks);
							totalLength -= curSplitSize;
							curSplitSize = 0;
							splitsPerNode.AddItem(node);
							// Remove entries from blocksInNode so that we don't walk these
							// again.
							blocksInCurrentNode.RemoveAll(validBlocks);
							validBlocks.Clear();
							// Done creating a single split for this node. Move on to the next
							// node so that splits are distributed across nodes.
							break;
						}
					}
					if (validBlocks.Count != 0)
					{
						// This implies that the last few blocks (or all in case maxSize=0)
						// were not part of a split. The node is complete.
						// if there were any blocks left over and their combined size is
						// larger than minSplitNode, then combine them into one split.
						// Otherwise add them back to the unprocessed pool. It is likely
						// that they will be combined with other blocks from the
						// same rack later on.
						// This condition also kicks in when max split size is not set. All
						// blocks on a node will be grouped together into a single split.
						if (minSizeNode != 0 && curSplitSize >= minSizeNode && splitsPerNode.Count(node) 
							== 0)
						{
							// haven't created any split on this machine. so its ok to add a
							// smaller one for parallelism. Otherwise group it in the rack for
							// balanced size create an input split and add it to the splits
							// array
							AddCreatedSplit(splits, Sharpen.Collections.Singleton(node), validBlocks);
							totalLength -= curSplitSize;
							splitsPerNode.AddItem(node);
							// Remove entries from blocksInNode so that we don't walk this again.
							blocksInCurrentNode.RemoveAll(validBlocks);
						}
						else
						{
							// The node is done. This was the last set of blocks for this node.
							// Put the unplaced blocks back into the pool for later rack-allocation.
							foreach (CombineFileInputFormat.OneBlockInfo oneblock in validBlocks)
							{
								blockToNodes[oneblock] = oneblock.hosts;
							}
						}
						validBlocks.Clear();
						curSplitSize = 0;
						completedNodes.AddItem(node);
					}
					else
					{
						// No in-flight blocks.
						if (blocksInCurrentNode.Count == 0)
						{
							// Node is done. All blocks were fit into node-local splits.
							completedNodes.AddItem(node);
						}
					}
				}
				// else Run through the node again.
				// Check if node-local assignments are complete.
				if (completedNodes.Count == totalNodes || totalLength == 0)
				{
					// All nodes have been walked over and marked as completed or all blocks
					// have been assigned. The rest should be handled via rackLock assignment.
					Log.Info("DEBUG: Terminated node allocation with : CompletedNodes: " + completedNodes
						.Count + ", size left: " + totalLength);
					break;
				}
			}
			// if blocks in a rack are below the specified minimum size, then keep them
			// in 'overflow'. After the processing of all racks is complete, these 
			// overflow blocks will be combined into splits.
			AList<CombineFileInputFormat.OneBlockInfo> overflowBlocks = new AList<CombineFileInputFormat.OneBlockInfo
				>();
			ICollection<string> racks = new HashSet<string>();
			// Process all racks over and over again until there is no more work to do.
			while (blockToNodes.Count > 0)
			{
				// Create one split for this rack before moving over to the next rack. 
				// Come back to this rack after creating a single split for each of the 
				// remaining racks.
				// Process one rack location at a time, Combine all possible blocks that
				// reside on this rack as one split. (constrained by minimum and maximum
				// split size).
				// iterate over all racks 
				for (IEnumerator<KeyValuePair<string, IList<CombineFileInputFormat.OneBlockInfo>>
					> iter = rackToBlocks.GetEnumerator(); iter.HasNext(); )
				{
					KeyValuePair<string, IList<CombineFileInputFormat.OneBlockInfo>> one = iter.Next(
						);
					racks.AddItem(one.Key);
					IList<CombineFileInputFormat.OneBlockInfo> blocks = one.Value;
					// for each block, copy it into validBlocks. Delete it from 
					// blockToNodes so that the same block does not appear in 
					// two different splits.
					bool createdSplit = false;
					foreach (CombineFileInputFormat.OneBlockInfo oneblock in blocks)
					{
						if (blockToNodes.Contains(oneblock))
						{
							validBlocks.AddItem(oneblock);
							Sharpen.Collections.Remove(blockToNodes, oneblock);
							curSplitSize += oneblock.length;
							// if the accumulated split size exceeds the maximum, then 
							// create this split.
							if (maxSize != 0 && curSplitSize >= maxSize)
							{
								// create an input split and add it to the splits array
								AddCreatedSplit(splits, GetHosts(racks), validBlocks);
								createdSplit = true;
								break;
							}
						}
					}
					// if we created a split, then just go to the next rack
					if (createdSplit)
					{
						curSplitSize = 0;
						validBlocks.Clear();
						racks.Clear();
						continue;
					}
					if (!validBlocks.IsEmpty())
					{
						if (minSizeRack != 0 && curSplitSize >= minSizeRack)
						{
							// if there is a minimum size specified, then create a single split
							// otherwise, store these blocks into overflow data structure
							AddCreatedSplit(splits, GetHosts(racks), validBlocks);
						}
						else
						{
							// There were a few blocks in this rack that 
							// remained to be processed. Keep them in 'overflow' block list. 
							// These will be combined later.
							Sharpen.Collections.AddAll(overflowBlocks, validBlocks);
						}
					}
					curSplitSize = 0;
					validBlocks.Clear();
					racks.Clear();
				}
			}
			System.Diagnostics.Debug.Assert(blockToNodes.IsEmpty());
			System.Diagnostics.Debug.Assert(curSplitSize == 0);
			System.Diagnostics.Debug.Assert(validBlocks.IsEmpty());
			System.Diagnostics.Debug.Assert(racks.IsEmpty());
			// Process all overflow blocks
			foreach (CombineFileInputFormat.OneBlockInfo oneblock_1 in overflowBlocks)
			{
				validBlocks.AddItem(oneblock_1);
				curSplitSize += oneblock_1.length;
				// This might cause an exiting rack location to be re-added,
				// but it should be ok.
				for (int i = 0; i < oneblock_1.racks.Length; i++)
				{
					racks.AddItem(oneblock_1.racks[i]);
				}
				// if the accumulated split size exceeds the maximum, then 
				// create this split.
				if (maxSize != 0 && curSplitSize >= maxSize)
				{
					// create an input split and add it to the splits array
					AddCreatedSplit(splits, GetHosts(racks), validBlocks);
					curSplitSize = 0;
					validBlocks.Clear();
					racks.Clear();
				}
			}
			// Process any remaining blocks, if any.
			if (!validBlocks.IsEmpty())
			{
				AddCreatedSplit(splits, GetHosts(racks), validBlocks);
			}
		}

		/// <summary>
		/// Create a single split from the list of blocks specified in validBlocks
		/// Add this new split into splitList.
		/// </summary>
		private void AddCreatedSplit(IList<InputSplit> splitList, ICollection<string> locations
			, AList<CombineFileInputFormat.OneBlockInfo> validBlocks)
		{
			// create an input split
			Path[] fl = new Path[validBlocks.Count];
			long[] offset = new long[validBlocks.Count];
			long[] length = new long[validBlocks.Count];
			for (int i = 0; i < validBlocks.Count; i++)
			{
				fl[i] = validBlocks[i].onepath;
				offset[i] = validBlocks[i].offset;
				length[i] = validBlocks[i].length;
			}
			// add this split to the list that is returned
			CombineFileSplit thissplit = new CombineFileSplit(fl, offset, length, Sharpen.Collections.ToArray
				(locations, new string[0]));
			splitList.AddItem(thissplit);
		}

		/// <summary>This is not implemented yet.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract override RecordReader<K, V> CreateRecordReader(InputSplit split, 
			TaskAttemptContext context);

		/// <summary>information about one file from the File System</summary>
		internal class OneFileInfo
		{
			private long fileSize;

			private CombineFileInputFormat.OneBlockInfo[] blocks;

			/// <exception cref="System.IO.IOException"/>
			internal OneFileInfo(FileStatus stat, Configuration conf, bool isSplitable, Dictionary
				<string, IList<CombineFileInputFormat.OneBlockInfo>> rackToBlocks, Dictionary<CombineFileInputFormat.OneBlockInfo
				, string[]> blockToNodes, Dictionary<string, ICollection<CombineFileInputFormat.OneBlockInfo
				>> nodeToBlocks, Dictionary<string, ICollection<string>> rackToNodes, long maxSize
				)
			{
				// size of the file
				// all blocks in this file
				this.fileSize = 0;
				// get block locations from file system
				BlockLocation[] locations;
				if (stat is LocatedFileStatus)
				{
					locations = ((LocatedFileStatus)stat).GetBlockLocations();
				}
				else
				{
					FileSystem fs = stat.GetPath().GetFileSystem(conf);
					locations = fs.GetFileBlockLocations(stat, 0, stat.GetLen());
				}
				// create a list of all block and their locations
				if (locations == null)
				{
					blocks = new CombineFileInputFormat.OneBlockInfo[0];
				}
				else
				{
					if (locations.Length == 0 && !stat.IsDirectory())
					{
						locations = new BlockLocation[] { new BlockLocation() };
					}
					if (!isSplitable)
					{
						// if the file is not splitable, just create the one block with
						// full file length
						blocks = new CombineFileInputFormat.OneBlockInfo[1];
						fileSize = stat.GetLen();
						blocks[0] = new CombineFileInputFormat.OneBlockInfo(stat.GetPath(), 0, fileSize, 
							locations[0].GetHosts(), locations[0].GetTopologyPaths());
					}
					else
					{
						AList<CombineFileInputFormat.OneBlockInfo> blocksList = new AList<CombineFileInputFormat.OneBlockInfo
							>(locations.Length);
						for (int i = 0; i < locations.Length; i++)
						{
							fileSize += locations[i].GetLength();
							// each split can be a maximum of maxSize
							long left = locations[i].GetLength();
							long myOffset = locations[i].GetOffset();
							long myLength = 0;
							do
							{
								if (maxSize == 0)
								{
									myLength = left;
								}
								else
								{
									if (left > maxSize && left < 2 * maxSize)
									{
										// if remainder is between max and 2*max - then
										// instead of creating splits of size max, left-max we
										// create splits of size left/2 and left/2. This is
										// a heuristic to avoid creating really really small
										// splits.
										myLength = left / 2;
									}
									else
									{
										myLength = Math.Min(maxSize, left);
									}
								}
								CombineFileInputFormat.OneBlockInfo oneblock = new CombineFileInputFormat.OneBlockInfo
									(stat.GetPath(), myOffset, myLength, locations[i].GetHosts(), locations[i].GetTopologyPaths
									());
								left -= myLength;
								myOffset += myLength;
								blocksList.AddItem(oneblock);
							}
							while (left > 0);
						}
						blocks = Sharpen.Collections.ToArray(blocksList, new CombineFileInputFormat.OneBlockInfo
							[blocksList.Count]);
					}
					PopulateBlockInfo(blocks, rackToBlocks, blockToNodes, nodeToBlocks, rackToNodes);
				}
			}

			[VisibleForTesting]
			internal static void PopulateBlockInfo(CombineFileInputFormat.OneBlockInfo[] blocks
				, IDictionary<string, IList<CombineFileInputFormat.OneBlockInfo>> rackToBlocks, 
				IDictionary<CombineFileInputFormat.OneBlockInfo, string[]> blockToNodes, IDictionary
				<string, ICollection<CombineFileInputFormat.OneBlockInfo>> nodeToBlocks, IDictionary
				<string, ICollection<string>> rackToNodes)
			{
				foreach (CombineFileInputFormat.OneBlockInfo oneblock in blocks)
				{
					// add this block to the block --> node locations map
					blockToNodes[oneblock] = oneblock.hosts;
					// For blocks that do not have host/rack information,
					// assign to default  rack.
					string[] racks = null;
					if (oneblock.hosts.Length == 0)
					{
						racks = new string[] { NetworkTopology.DefaultRack };
					}
					else
					{
						racks = oneblock.racks;
					}
					// add this block to the rack --> block map
					for (int j = 0; j < racks.Length; j++)
					{
						string rack = racks[j];
						IList<CombineFileInputFormat.OneBlockInfo> blklist = rackToBlocks[rack];
						if (blklist == null)
						{
							blklist = new AList<CombineFileInputFormat.OneBlockInfo>();
							rackToBlocks[rack] = blklist;
						}
						blklist.AddItem(oneblock);
						if (!racks[j].Equals(NetworkTopology.DefaultRack))
						{
							// Add this host to rackToNodes map
							AddHostToRack(rackToNodes, racks[j], oneblock.hosts[j]);
						}
					}
					// add this block to the node --> block map
					for (int j_1 = 0; j_1 < oneblock.hosts.Length; j_1++)
					{
						string node = oneblock.hosts[j_1];
						ICollection<CombineFileInputFormat.OneBlockInfo> blklist = nodeToBlocks[node];
						if (blklist == null)
						{
							blklist = new LinkedHashSet<CombineFileInputFormat.OneBlockInfo>();
							nodeToBlocks[node] = blklist;
						}
						blklist.AddItem(oneblock);
					}
				}
			}

			internal virtual long GetLength()
			{
				return fileSize;
			}

			internal virtual CombineFileInputFormat.OneBlockInfo[] GetBlocks()
			{
				return blocks;
			}
		}

		/// <summary>information about one block from the File System</summary>
		internal class OneBlockInfo
		{
			internal Path onepath;

			internal long offset;

			internal long length;

			internal string[] hosts;

			internal string[] racks;

			internal OneBlockInfo(Path path, long offset, long len, string[] hosts, string[] 
				topologyPaths)
			{
				// name of this file
				// offset in file
				// length of this block
				// nodes on which this block resides
				// network topology of hosts
				this.onepath = path;
				this.offset = offset;
				this.hosts = hosts;
				this.length = len;
				System.Diagnostics.Debug.Assert((hosts.Length == topologyPaths.Length || topologyPaths
					.Length == 0));
				// if the file system does not have any rack information, then
				// use dummy rack location.
				if (topologyPaths.Length == 0)
				{
					topologyPaths = new string[hosts.Length];
					for (int i = 0; i < topologyPaths.Length; i++)
					{
						topologyPaths[i] = (new NodeBase(hosts[i], NetworkTopology.DefaultRack)).ToString
							();
					}
				}
				// The topology paths have the host name included as the last 
				// component. Strip it.
				this.racks = new string[topologyPaths.Length];
				for (int i_1 = 0; i_1 < topologyPaths.Length; i_1++)
				{
					this.racks[i_1] = (new NodeBase(topologyPaths[i_1])).GetNetworkLocation();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual BlockLocation[] GetFileBlockLocations(FileSystem fs, FileStatus
			 stat)
		{
			if (stat is LocatedFileStatus)
			{
				return ((LocatedFileStatus)stat).GetBlockLocations();
			}
			return fs.GetFileBlockLocations(stat, 0, stat.GetLen());
		}

		private static void AddHostToRack(IDictionary<string, ICollection<string>> rackToNodes
			, string rack, string host)
		{
			ICollection<string> hosts = rackToNodes[rack];
			if (hosts == null)
			{
				hosts = new HashSet<string>();
				rackToNodes[rack] = hosts;
			}
			hosts.AddItem(host);
		}

		private ICollection<string> GetHosts(ICollection<string> racks)
		{
			ICollection<string> hosts = new HashSet<string>();
			foreach (string rack in racks)
			{
				if (rackToNodes.Contains(rack))
				{
					Sharpen.Collections.AddAll(hosts, rackToNodes[rack]);
				}
			}
			return hosts;
		}

		/// <summary>
		/// Accept a path only if any one of filters given in the
		/// constructor do.
		/// </summary>
		private class MultiPathFilter : PathFilter
		{
			private IList<PathFilter> filters;

			public MultiPathFilter()
			{
				this.filters = new AList<PathFilter>();
			}

			public MultiPathFilter(IList<PathFilter> filters)
			{
				this.filters = filters;
			}

			public virtual void Add(PathFilter one)
			{
				filters.AddItem(one);
			}

			public virtual bool Accept(Path path)
			{
				foreach (PathFilter filter in filters)
				{
					if (filter.Accept(path))
					{
						return true;
					}
				}
				return false;
			}

			public override string ToString()
			{
				StringBuilder buf = new StringBuilder();
				buf.Append("[");
				foreach (PathFilter f in filters)
				{
					buf.Append(f);
					buf.Append(",");
				}
				buf.Append("]");
				return buf.ToString();
			}
		}
	}
}
