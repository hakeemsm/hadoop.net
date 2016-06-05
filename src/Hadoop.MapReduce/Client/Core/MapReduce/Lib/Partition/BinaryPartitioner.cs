using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Partition
{
	/// <summary>
	/// <p>Partition
	/// <see cref="Org.Apache.Hadoop.IO.BinaryComparable"/>
	/// keys using a configurable part of
	/// the bytes array returned by
	/// <see cref="Org.Apache.Hadoop.IO.BinaryComparable.GetBytes()"/>
	/// .</p>
	/// <p>The subarray to be used for the partitioning can be defined by means
	/// of the following properties:
	/// <ul>
	/// <li>
	/// <i>mapreduce.partition.binarypartitioner.left.offset</i>:
	/// left offset in array (0 by default)
	/// </li>
	/// <li>
	/// <i>mapreduce.partition.binarypartitioner.right.offset</i>:
	/// right offset in array (-1 by default)
	/// </li>
	/// </ul>
	/// Like in Python, both negative and positive offsets are allowed, but
	/// the meaning is slightly different. In case of an array of length 5,
	/// for instance, the possible offsets are:
	/// <pre><code>
	/// +---+---+---+---+---+
	/// | B | B | B | B | B |
	/// +---+---+---+---+---+
	/// 0   1   2   3   4
	/// -5  -4  -3  -2  -1
	/// </code></pre>
	/// The first row of numbers gives the position of the offsets 0...5 in
	/// the array; the second row gives the corresponding negative offsets.
	/// Contrary to Python, the specified subarray has byte <code>i</code>
	/// and <code>j</code> as first and last element, repectively, when
	/// <code>i</code> and <code>j</code> are the left and right offset.
	/// <p>For Hadoop programs written in Java, it is advisable to use one of
	/// the following static convenience methods for setting the offsets:
	/// <ul>
	/// <li>
	/// <see cref="BinaryPartitioner{V}.SetOffsets(Org.Apache.Hadoop.Conf.Configuration, int, int)
	/// 	"/>
	/// </li>
	/// <li>
	/// <see cref="BinaryPartitioner{V}.SetLeftOffset(Org.Apache.Hadoop.Conf.Configuration, int)
	/// 	"/>
	/// </li>
	/// <li>
	/// <see cref="BinaryPartitioner{V}.SetRightOffset(Org.Apache.Hadoop.Conf.Configuration, int)
	/// 	"/>
	/// </li>
	/// </ul>
	/// </summary>
	public class BinaryPartitioner<V> : Partitioner<BinaryComparable, V>, Configurable
	{
		public const string LeftOffsetPropertyName = "mapreduce.partition.binarypartitioner.left.offset";

		public const string RightOffsetPropertyName = "mapreduce.partition.binarypartitioner.right.offset";

		/// <summary>
		/// Set the subarray to be used for partitioning to
		/// <code>bytes[left:(right+1)]</code> in Python syntax.
		/// </summary>
		/// <param name="conf">configuration object</param>
		/// <param name="left">left Python-style offset</param>
		/// <param name="right">right Python-style offset</param>
		public static void SetOffsets(Configuration conf, int left, int right)
		{
			conf.SetInt(LeftOffsetPropertyName, left);
			conf.SetInt(RightOffsetPropertyName, right);
		}

		/// <summary>
		/// Set the subarray to be used for partitioning to
		/// <code>bytes[offset:]</code> in Python syntax.
		/// </summary>
		/// <param name="conf">configuration object</param>
		/// <param name="offset">left Python-style offset</param>
		public static void SetLeftOffset(Configuration conf, int offset)
		{
			conf.SetInt(LeftOffsetPropertyName, offset);
		}

		/// <summary>
		/// Set the subarray to be used for partitioning to
		/// <code>bytes[:(offset+1)]</code> in Python syntax.
		/// </summary>
		/// <param name="conf">configuration object</param>
		/// <param name="offset">right Python-style offset</param>
		public static void SetRightOffset(Configuration conf, int offset)
		{
			conf.SetInt(RightOffsetPropertyName, offset);
		}

		private Configuration conf;

		private int leftOffset;

		private int rightOffset;

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
			leftOffset = conf.GetInt(LeftOffsetPropertyName, 0);
			rightOffset = conf.GetInt(RightOffsetPropertyName, -1);
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <summary>
		/// Use (the specified slice of the array returned by)
		/// <see cref="Org.Apache.Hadoop.IO.BinaryComparable.GetBytes()"/>
		/// to partition.
		/// </summary>
		public override int GetPartition(BinaryComparable key, V value, int numPartitions
			)
		{
			int length = key.GetLength();
			int leftIndex = (leftOffset + length) % length;
			int rightIndex = (rightOffset + length) % length;
			int hash = WritableComparator.HashBytes(key.GetBytes(), leftIndex, rightIndex - leftIndex
				 + 1);
			return (hash & int.MaxValue) % numPartitions;
		}
	}
}
