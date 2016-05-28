using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.Terasort
{
	/// <summary>
	/// Generates the sampled split points, launches the job, and waits for it to
	/// finish.
	/// </summary>
	/// <remarks>
	/// Generates the sampled split points, launches the job, and waits for it to
	/// finish.
	/// <p>
	/// To run the program:
	/// <b>bin/hadoop jar hadoop-*-examples.jar terasort in-dir out-dir</b>
	/// </remarks>
	public class TeraSort : Configured, Tool
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TeraSort));

		internal static string SimplePartitioner = "mapreduce.terasort.simplepartitioner";

		internal static string OutputReplication = "mapreduce.terasort.output.replication";

		/// <summary>
		/// A partitioner that splits text keys into roughly equal partitions
		/// in a global sorted order.
		/// </summary>
		internal class TotalOrderPartitioner : Partitioner<Text, Text>, Configurable
		{
			private TeraSort.TotalOrderPartitioner.TrieNode trie;

			private Text[] splitPoints;

			private Configuration conf;

			/// <summary>A generic trie node</summary>
			internal abstract class TrieNode
			{
				private int level;

				internal TrieNode(int level)
				{
					this.level = level;
				}

				internal abstract int FindPartition(Text key);

				/// <exception cref="System.IO.IOException"/>
				internal abstract void Print(TextWriter strm);

				internal virtual int GetLevel()
				{
					return level;
				}
			}

			/// <summary>
			/// An inner trie node that contains 256 children based on the next
			/// character.
			/// </summary>
			internal class InnerTrieNode : TeraSort.TotalOrderPartitioner.TrieNode
			{
				private TeraSort.TotalOrderPartitioner.TrieNode[] child = new TeraSort.TotalOrderPartitioner.TrieNode
					[256];

				internal InnerTrieNode(int level)
					: base(level)
				{
				}

				internal override int FindPartition(Text key)
				{
					int level = GetLevel();
					if (key.GetLength() <= level)
					{
						return child[0].FindPartition(key);
					}
					return child[key.GetBytes()[level] & unchecked((int)(0xff))].FindPartition(key);
				}

				internal virtual void SetChild(int idx, TeraSort.TotalOrderPartitioner.TrieNode child
					)
				{
					this.child[idx] = child;
				}

				/// <exception cref="System.IO.IOException"/>
				internal override void Print(TextWriter strm)
				{
					for (int ch = 0; ch < 256; ++ch)
					{
						for (int i = 0; i < 2 * GetLevel(); ++i)
						{
							strm.Write(' ');
						}
						strm.Write(ch);
						strm.WriteLine(" ->");
						if (child[ch] != null)
						{
							child[ch].Print(strm);
						}
					}
				}
			}

			/// <summary>
			/// A leaf trie node that does string compares to figure out where the given
			/// key belongs between lower..upper.
			/// </summary>
			internal class LeafTrieNode : TeraSort.TotalOrderPartitioner.TrieNode
			{
				internal int lower;

				internal int upper;

				internal Text[] splitPoints;

				internal LeafTrieNode(int level, Text[] splitPoints, int lower, int upper)
					: base(level)
				{
					this.splitPoints = splitPoints;
					this.lower = lower;
					this.upper = upper;
				}

				internal override int FindPartition(Text key)
				{
					for (int i = lower; i < upper; ++i)
					{
						if (splitPoints[i].CompareTo(key) > 0)
						{
							return i;
						}
					}
					return upper;
				}

				/// <exception cref="System.IO.IOException"/>
				internal override void Print(TextWriter strm)
				{
					for (int i = 0; i < 2 * GetLevel(); ++i)
					{
						strm.Write(' ');
					}
					strm.Write(lower);
					strm.Write(", ");
					strm.WriteLine(upper);
				}
			}

			/// <summary>Read the cut points from the given sequence file.</summary>
			/// <param name="fs">the file system</param>
			/// <param name="p">the path to read</param>
			/// <param name="job">the job config</param>
			/// <returns>the strings to split the partitions on</returns>
			/// <exception cref="System.IO.IOException"/>
			private static Text[] ReadPartitions(FileSystem fs, Path p, Configuration conf)
			{
				int reduces = conf.GetInt(MRJobConfig.NumReduces, 1);
				Text[] result = new Text[reduces - 1];
				DataInputStream reader = fs.Open(p);
				for (int i = 0; i < reduces - 1; ++i)
				{
					result[i] = new Text();
					result[i].ReadFields(reader);
				}
				reader.Close();
				return result;
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
			private static TeraSort.TotalOrderPartitioner.TrieNode BuildTrie(Text[] splits, int
				 lower, int upper, Text prefix, int maxDepth)
			{
				int depth = prefix.GetLength();
				if (depth >= maxDepth || lower == upper)
				{
					return new TeraSort.TotalOrderPartitioner.LeafTrieNode(depth, splits, lower, upper
						);
				}
				TeraSort.TotalOrderPartitioner.InnerTrieNode result = new TeraSort.TotalOrderPartitioner.InnerTrieNode
					(depth);
				Text trial = new Text(prefix);
				// append an extra byte on to the prefix
				trial.Append(new byte[1], 0, 1);
				int currentBound = lower;
				for (int ch = 0; ch < 255; ++ch)
				{
					trial.GetBytes()[depth] = unchecked((byte)(ch + 1));
					lower = currentBound;
					while (currentBound < upper)
					{
						if (splits[currentBound].CompareTo(trial) >= 0)
						{
							break;
						}
						currentBound += 1;
					}
					trial.GetBytes()[depth] = unchecked((byte)ch);
					result.child[ch] = BuildTrie(splits, lower, currentBound, trial, maxDepth);
				}
				// pick up the rest
				trial.GetBytes()[depth] = unchecked((byte)255);
				result.child[255] = BuildTrie(splits, currentBound, upper, trial, maxDepth);
				return result;
			}

			public virtual void SetConf(Configuration conf)
			{
				try
				{
					FileSystem fs = FileSystem.GetLocal(conf);
					this.conf = conf;
					Path partFile = new Path(TeraInputFormat.PartitionFilename);
					splitPoints = ReadPartitions(fs, partFile, conf);
					trie = BuildTrie(splitPoints, 0, splitPoints.Length, new Text(), 2);
				}
				catch (IOException ie)
				{
					throw new ArgumentException("can't read partitions file", ie);
				}
			}

			public virtual Configuration GetConf()
			{
				return conf;
			}

			public TotalOrderPartitioner()
			{
			}

			public override int GetPartition(Text key, Text value, int numPartitions)
			{
				return trie.FindPartition(key);
			}
		}

		/// <summary>
		/// A total order partitioner that assigns keys based on their first
		/// PREFIX_LENGTH bytes, assuming a flat distribution.
		/// </summary>
		public class SimplePartitioner : Partitioner<Text, Text>, Configurable
		{
			internal int prefixesPerReduce;

			private const int PrefixLength = 3;

			private Configuration conf = null;

			public virtual void SetConf(Configuration conf)
			{
				this.conf = conf;
				prefixesPerReduce = (int)Math.Ceil((1 << (8 * PrefixLength)) / (float)conf.GetInt
					(MRJobConfig.NumReduces, 1));
			}

			public virtual Configuration GetConf()
			{
				return conf;
			}

			public override int GetPartition(Text key, Text value, int numPartitions)
			{
				byte[] bytes = key.GetBytes();
				int len = Math.Min(PrefixLength, key.GetLength());
				int prefix = 0;
				for (int i = 0; i < len; ++i)
				{
					prefix = (prefix << 8) | (unchecked((int)(0xff)) & bytes[i]);
				}
				return prefix / prefixesPerReduce;
			}
		}

		public static bool GetUseSimplePartitioner(JobContext job)
		{
			return job.GetConfiguration().GetBoolean(SimplePartitioner, false);
		}

		public static void SetUseSimplePartitioner(Job job, bool value)
		{
			job.GetConfiguration().SetBoolean(SimplePartitioner, value);
		}

		public static int GetOutputReplication(JobContext job)
		{
			return job.GetConfiguration().GetInt(OutputReplication, 1);
		}

		public static void SetOutputReplication(Job job, int value)
		{
			job.GetConfiguration().SetInt(OutputReplication, value);
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Log.Info("starting");
			Job job = Job.GetInstance(GetConf());
			Path inputDir = new Path(args[0]);
			Path outputDir = new Path(args[1]);
			bool useSimplePartitioner = GetUseSimplePartitioner(job);
			TeraInputFormat.SetInputPaths(job, inputDir);
			FileOutputFormat.SetOutputPath(job, outputDir);
			job.SetJobName("TeraSort");
			job.SetJarByClass(typeof(TeraSort));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(Text));
			job.SetInputFormatClass(typeof(TeraInputFormat));
			job.SetOutputFormatClass(typeof(TeraOutputFormat));
			if (useSimplePartitioner)
			{
				job.SetPartitionerClass(typeof(TeraSort.SimplePartitioner));
			}
			else
			{
				long start = Runtime.CurrentTimeMillis();
				Path partitionFile = new Path(outputDir, TeraInputFormat.PartitionFilename);
				URI partitionUri = new URI(partitionFile.ToString() + "#" + TeraInputFormat.PartitionFilename
					);
				try
				{
					TeraInputFormat.WritePartitionFile(job, partitionFile);
				}
				catch (Exception e)
				{
					Log.Error(e.Message);
					return -1;
				}
				job.AddCacheFile(partitionUri);
				long end = Runtime.CurrentTimeMillis();
				System.Console.Out.WriteLine("Spent " + (end - start) + "ms computing partitions."
					);
				job.SetPartitionerClass(typeof(TeraSort.TotalOrderPartitioner));
			}
			job.GetConfiguration().SetInt("dfs.replication", GetOutputReplication(job));
			TeraOutputFormat.SetFinalSync(job, true);
			int ret = job.WaitForCompletion(true) ? 0 : 1;
			Log.Info("done");
			return ret;
		}

		/// <param name="args"/>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new TeraSort(), args);
			System.Environment.Exit(res);
		}
	}
}
