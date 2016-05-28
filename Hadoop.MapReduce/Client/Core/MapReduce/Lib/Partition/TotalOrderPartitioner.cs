using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Partition
{
	/// <summary>
	/// Partitioner effecting a total order by reading split points from
	/// an externally generated source.
	/// </summary>
	public class TotalOrderPartitioner<K, V> : Partitioner<K, V>, Configurable
		where K : WritableComparable<object>
	{
		private TotalOrderPartitioner.Node partitions;

		public const string DefaultPath = "_partition.lst";

		public const string PartitionerPath = "mapreduce.totalorderpartitioner.path";

		public const string MaxTrieDepth = "mapreduce.totalorderpartitioner.trie.maxdepth";

		public const string NaturalOrder = "mapreduce.totalorderpartitioner.naturalorder";

		internal Configuration conf;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.Partition.TotalOrderPartitioner
			));

		public TotalOrderPartitioner()
		{
		}

		/// <summary>Read in the partition file and build indexing data structures.</summary>
		/// <remarks>
		/// Read in the partition file and build indexing data structures.
		/// If the keytype is
		/// <see cref="Org.Apache.Hadoop.IO.BinaryComparable"/>
		/// and
		/// <tt>total.order.partitioner.natural.order</tt> is not false, a trie
		/// of the first <tt>total.order.partitioner.max.trie.depth</tt>(2) + 1 bytes
		/// will be built. Otherwise, keys will be located using a binary search of
		/// the partition keyset using the
		/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
		/// defined for this job. The input file must be sorted with the same
		/// comparator and contain
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Task.JobContextImpl.GetNumReduceTasks()"/>
		/// - 1 keys.
		/// </remarks>
		public virtual void SetConf(Configuration conf)
		{
			// keytype from conf not static
			try
			{
				this.conf = conf;
				string parts = GetPartitionFile(conf);
				Path partFile = new Path(parts);
				FileSystem fs = (DefaultPath.Equals(parts)) ? FileSystem.GetLocal(conf) : partFile
					.GetFileSystem(conf);
				// assume in DistributedCache
				Job job = Job.GetInstance(conf);
				Type keyClass = (Type)job.GetMapOutputKeyClass();
				K[] splitPoints = ReadPartitions(fs, partFile, keyClass, conf);
				if (splitPoints.Length != job.GetNumReduceTasks() - 1)
				{
					throw new IOException("Wrong number of partitions in keyset");
				}
				RawComparator<K> comparator = (RawComparator<K>)job.GetSortComparator();
				for (int i = 0; i < splitPoints.Length - 1; ++i)
				{
					if (comparator.Compare(splitPoints[i], splitPoints[i + 1]) >= 0)
					{
						throw new IOException("Split points are out of order");
					}
				}
				bool natOrder = conf.GetBoolean(NaturalOrder, true);
				if (natOrder && typeof(BinaryComparable).IsAssignableFrom(keyClass))
				{
					partitions = BuildTrie((BinaryComparable[])splitPoints, 0, splitPoints.Length, new 
						byte[0], conf.GetInt(MaxTrieDepth, 200));
				}
				else
				{
					// Now that blocks of identical splitless trie nodes are 
					// represented reentrantly, and we develop a leaf for any trie
					// node with only one split point, the only reason for a depth
					// limit is to refute stack overflow or bloat in the pathological
					// case where the split points are long and mostly look like bytes 
					// iii...iixii...iii   .  Therefore, we make the default depth
					// limit large but not huge.
					partitions = new TotalOrderPartitioner.BinarySearchNode(this, splitPoints, comparator
						);
				}
			}
			catch (IOException e)
			{
				throw new ArgumentException("Can't read partitions file", e);
			}
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		// by construction, we know if our keytype
		public override int GetPartition(K key, V value, int numPartitions)
		{
			// is memcmp-able and uses the trie
			return partitions.FindPartition(key);
		}

		/// <summary>Set the path to the SequenceFile storing the sorted partition keyset.</summary>
		/// <remarks>
		/// Set the path to the SequenceFile storing the sorted partition keyset.
		/// It must be the case that for <tt>R</tt> reduces, there are <tt>R-1</tt>
		/// keys in the SequenceFile.
		/// </remarks>
		public static void SetPartitionFile(Configuration conf, Path p)
		{
			conf.Set(PartitionerPath, p.ToString());
		}

		/// <summary>Get the path to the SequenceFile storing the sorted partition keyset.</summary>
		/// <seealso cref="TotalOrderPartitioner{K, V}.SetPartitionFile(Org.Apache.Hadoop.Conf.Configuration, Org.Apache.Hadoop.FS.Path)
		/// 	"/>
		public static string GetPartitionFile(Configuration conf)
		{
			return conf.Get(PartitionerPath, DefaultPath);
		}

		/// <summary>Interface to the partitioner to locate a key in the partition keyset.</summary>
		internal interface Node<T>
		{
			/// <summary>
			/// Locate partition in keyset K, st [Ki..Ki+1) defines a partition,
			/// with implicit K0 = -inf, Kn = +inf, and |K| = #partitions - 1.
			/// </summary>
			int FindPartition(T key);
		}

		/// <summary>Base class for trie nodes.</summary>
		/// <remarks>
		/// Base class for trie nodes. If the keytype is memcomp-able, this builds
		/// tries of the first <tt>total.order.partitioner.max.trie.depth</tt>
		/// bytes.
		/// </remarks>
		internal abstract class TrieNode : TotalOrderPartitioner.Node<BinaryComparable>
		{
			private readonly int level;

			internal TrieNode(int level)
			{
				this.level = level;
			}

			internal virtual int GetLevel()
			{
				return level;
			}

			public abstract int FindPartition(BinaryComparable arg1);
		}

		/// <summary>
		/// For types that are not
		/// <see cref="Org.Apache.Hadoop.IO.BinaryComparable"/>
		/// or
		/// where disabled by <tt>total.order.partitioner.natural.order</tt>,
		/// search the partition keyset with a binary search.
		/// </summary>
		internal class BinarySearchNode : TotalOrderPartitioner.Node<K>
		{
			private readonly K[] splitPoints;

			private readonly RawComparator<K> comparator;

			internal BinarySearchNode(TotalOrderPartitioner<K, V> _enclosing, K[] splitPoints
				, RawComparator<K> comparator)
			{
				this._enclosing = _enclosing;
				this.splitPoints = splitPoints;
				this.comparator = comparator;
			}

			public virtual int FindPartition(K key)
			{
				int pos = System.Array.BinarySearch(this.splitPoints, key, this.comparator) + 1;
				return (pos < 0) ? -pos : pos;
			}

			private readonly TotalOrderPartitioner<K, V> _enclosing;
		}

		/// <summary>
		/// An inner trie node that contains 256 children based on the next
		/// character.
		/// </summary>
		internal class InnerTrieNode : TotalOrderPartitioner.TrieNode
		{
			private TotalOrderPartitioner.TrieNode[] child = new TotalOrderPartitioner.TrieNode
				[256];

			internal InnerTrieNode(TotalOrderPartitioner<K, V> _enclosing, int level)
				: base(level)
			{
				this._enclosing = _enclosing;
			}

			public override int FindPartition(BinaryComparable key)
			{
				int level = this.GetLevel();
				if (key.GetLength() <= level)
				{
					return this.child[0].FindPartition(key);
				}
				return this.child[unchecked((int)(0xFF)) & key.GetBytes()[level]].FindPartition(key
					);
			}

			private readonly TotalOrderPartitioner<K, V> _enclosing;
		}

		/// <param name="level">the tree depth at this node</param>
		/// <param name="splitPoints">
		/// the full split point vector, which holds
		/// the split point or points this leaf node
		/// should contain
		/// </param>
		/// <param name="lower">first INcluded element of splitPoints</param>
		/// <param name="upper">first EXcluded element of splitPoints</param>
		/// <returns>
		/// a leaf node.  They come in three kinds: no split points
		/// [and the findParttion returns a canned index], one split
		/// point [and we compare with a single comparand], or more
		/// than one [and we do a binary search].  The last case is
		/// rare.
		/// </returns>
		private TotalOrderPartitioner.TrieNode LeafTrieNodeFactory(int level, BinaryComparable
			[] splitPoints, int lower, int upper)
		{
			switch (upper - lower)
			{
				case 0:
				{
					return new TotalOrderPartitioner.UnsplitTrieNode(this, level, lower);
				}

				case 1:
				{
					return new TotalOrderPartitioner.SinglySplitTrieNode(this, level, splitPoints, lower
						);
				}

				default:
				{
					return new TotalOrderPartitioner.LeafTrieNode(this, level, splitPoints, lower, upper
						);
				}
			}
		}

		/// <summary>A leaf trie node that scans for the key between lower..upper.</summary>
		/// <remarks>
		/// A leaf trie node that scans for the key between lower..upper.
		/// We don't generate many of these now, since we usually continue trie-ing
		/// when more than one split point remains at this level. and we make different
		/// objects for nodes with 0 or 1 split point.
		/// </remarks>
		private class LeafTrieNode : TotalOrderPartitioner.TrieNode
		{
			internal readonly int lower;

			internal readonly int upper;

			internal readonly BinaryComparable[] splitPoints;

			internal LeafTrieNode(TotalOrderPartitioner<K, V> _enclosing, int level, BinaryComparable
				[] splitPoints, int lower, int upper)
				: base(level)
			{
				this._enclosing = _enclosing;
				this.lower = lower;
				this.upper = upper;
				this.splitPoints = splitPoints;
			}

			public override int FindPartition(BinaryComparable key)
			{
				int pos = System.Array.BinarySearch(this.splitPoints, this.lower, this.upper, key
					) + 1;
				return (pos < 0) ? -pos : pos;
			}

			private readonly TotalOrderPartitioner<K, V> _enclosing;
		}

		private class UnsplitTrieNode : TotalOrderPartitioner.TrieNode
		{
			internal readonly int result;

			internal UnsplitTrieNode(TotalOrderPartitioner<K, V> _enclosing, int level, int value
				)
				: base(level)
			{
				this._enclosing = _enclosing;
				this.result = value;
			}

			public override int FindPartition(BinaryComparable key)
			{
				return this.result;
			}

			private readonly TotalOrderPartitioner<K, V> _enclosing;
		}

		private class SinglySplitTrieNode : TotalOrderPartitioner.TrieNode
		{
			internal readonly int lower;

			internal readonly BinaryComparable mySplitPoint;

			internal SinglySplitTrieNode(TotalOrderPartitioner<K, V> _enclosing, int level, BinaryComparable
				[] splitPoints, int lower)
				: base(level)
			{
				this._enclosing = _enclosing;
				this.lower = lower;
				this.mySplitPoint = splitPoints[lower];
			}

			public override int FindPartition(BinaryComparable key)
			{
				return this.lower + (key.CompareTo(this.mySplitPoint) < 0 ? 0 : 1);
			}

			private readonly TotalOrderPartitioner<K, V> _enclosing;
		}

		/// <summary>Read the cut points from the given IFile.</summary>
		/// <param name="fs">The file system</param>
		/// <param name="p">The path to read</param>
		/// <param name="keyClass">The map output key class</param>
		/// <param name="job">The job config</param>
		/// <exception cref="System.IO.IOException"/>
		private K[] ReadPartitions(FileSystem fs, Path p, Type keyClass, Configuration conf
			)
		{
			// matching key types enforced by passing in
			// map output key class
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
			AList<K> parts = new AList<K>();
			K key = ReflectionUtils.NewInstance(keyClass, conf);
			NullWritable value = NullWritable.Get();
			try
			{
				while (reader.Next(key, value))
				{
					parts.AddItem(key);
					key = ReflectionUtils.NewInstance(keyClass, conf);
				}
				reader.Close();
				reader = null;
			}
			finally
			{
				IOUtils.Cleanup(Log, reader);
			}
			return Sharpen.Collections.ToArray(parts, (K[])System.Array.CreateInstance(keyClass
				, parts.Count));
		}

		/// <summary>
		/// This object contains a TrieNodeRef if there is such a thing that
		/// can be repeated.
		/// </summary>
		/// <remarks>
		/// This object contains a TrieNodeRef if there is such a thing that
		/// can be repeated.  Two adjacent trie node slots that contain no
		/// split points can be filled with the same trie node, even if they
		/// are not on the same level.  See buildTreeRec, below.
		/// </remarks>
		private class CarriedTrieNodeRef
		{
			internal TotalOrderPartitioner.TrieNode content;

			internal CarriedTrieNodeRef(TotalOrderPartitioner<K, V> _enclosing)
			{
				this._enclosing = _enclosing;
				this.content = null;
			}

			private readonly TotalOrderPartitioner<K, V> _enclosing;
		}

		/// <summary>
		/// Given a sorted set of cut points, build a trie that will find the correct
		/// partition quickly.
		/// </summary>
		/// <param name="splits">the list of cut points</param>
		/// <param name="lower">the lower bound of partitions 0..numPartitions-1</param>
		/// <param name="upper">the upper bound of partitions 0..numPartitions-1</param>
		/// <param name="prefix">the prefix that we have already checked against</param>
		/// <param name="maxDepth">the maximum depth we will build a trie for</param>
		/// <returns>the trie node that will divide the splits correctly</returns>
		private TotalOrderPartitioner.TrieNode BuildTrie(BinaryComparable[] splits, int lower
			, int upper, byte[] prefix, int maxDepth)
		{
			return BuildTrieRec(splits, lower, upper, prefix, maxDepth, new TotalOrderPartitioner.CarriedTrieNodeRef
				(this));
		}

		/// <summary>This is the core of buildTrie.</summary>
		/// <remarks>
		/// This is the core of buildTrie.  The interface, and stub, above, just adds
		/// an empty CarriedTrieNodeRef.
		/// We build trie nodes in depth first order, which is also in key space
		/// order.  Every leaf node is referenced as a slot in a parent internal
		/// node.  If two adjacent slots [in the DFO] hold leaf nodes that have
		/// no split point, then they are not separated by a split point either,
		/// because there's no place in key space for that split point to exist.
		/// When that happens, the leaf nodes would be semantically identical, and
		/// we reuse the object.  A single CarriedTrieNodeRef "ref" lives for the
		/// duration of the tree-walk.  ref carries a potentially reusable, unsplit
		/// leaf node for such reuse until a leaf node with a split arises, which
		/// breaks the chain until we need to make a new unsplit leaf node.
		/// Note that this use of CarriedTrieNodeRef means that for internal nodes,
		/// for internal nodes if this code is modified in any way we still need
		/// to make or fill in the subnodes in key space order.
		/// </remarks>
		private TotalOrderPartitioner.TrieNode BuildTrieRec(BinaryComparable[] splits, int
			 lower, int upper, byte[] prefix, int maxDepth, TotalOrderPartitioner.CarriedTrieNodeRef
			 @ref)
		{
			int depth = prefix.Length;
			// We generate leaves for a single split point as well as for 
			// no split points.
			if (depth >= maxDepth || lower >= upper - 1)
			{
				// If we have two consecutive requests for an unsplit trie node, we
				// can deliver the same one the second time.
				if (lower == upper && @ref.content != null)
				{
					return @ref.content;
				}
				TotalOrderPartitioner.TrieNode result = LeafTrieNodeFactory(depth, splits, lower, 
					upper);
				@ref.content = lower == upper ? result : null;
				return result;
			}
			TotalOrderPartitioner.InnerTrieNode result_1 = new TotalOrderPartitioner.InnerTrieNode
				(this, depth);
			byte[] trial = Arrays.CopyOf(prefix, prefix.Length + 1);
			// append an extra byte on to the prefix
			int currentBound = lower;
			for (int ch = 0; ch < unchecked((int)(0xFF)); ++ch)
			{
				trial[depth] = unchecked((byte)(ch + 1));
				lower = currentBound;
				while (currentBound < upper)
				{
					if (splits[currentBound].CompareTo(trial, 0, trial.Length) >= 0)
					{
						break;
					}
					currentBound += 1;
				}
				trial[depth] = unchecked((byte)ch);
				result_1.child[unchecked((int)(0xFF)) & ch] = BuildTrieRec(splits, lower, currentBound
					, trial, maxDepth, @ref);
			}
			// pick up the rest
			trial[depth] = unchecked((byte)unchecked((int)(0xFF)));
			result_1.child[unchecked((int)(0xFF))] = BuildTrieRec(splits, lower, currentBound
				, trial, maxDepth, @ref);
			return result_1;
		}
	}
}
