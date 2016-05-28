using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.Dancing
{
	/// <summary>Launch a distributed pentomino solver.</summary>
	/// <remarks>
	/// Launch a distributed pentomino solver.
	/// It generates a complete list of prefixes of length N with each unique prefix
	/// as a separate line. A prefix is a sequence of N integers that denote the
	/// index of the row that is choosen for each column in order. Note that the
	/// next column is heuristically choosen by the solver, so it is dependant on
	/// the previous choice. That file is given as the input to
	/// map/reduce. The output key/value are the move prefix/solution as Text/Text.
	/// </remarks>
	public class DistributedPentomino : Configured, Tool
	{
		private const int PentDepth = 5;

		private const int PentWidth = 9;

		private const int PentHeight = 10;

		private const int DefaultMaps = 2000;

		/// <summary>
		/// Each map takes a line, which represents a prefix move and finds all of
		/// the solutions that start with that prefix.
		/// </summary>
		/// <remarks>
		/// Each map takes a line, which represents a prefix move and finds all of
		/// the solutions that start with that prefix. The output is the prefix as
		/// the key and the solution as the value.
		/// </remarks>
		public class PentMap : Mapper<WritableComparable<object>, Text, Text, Text>
		{
			private int width;

			private int height;

			private int depth;

			private Pentomino pent;

			private Text prefixString;

			private Mapper.Context context;

			/// <summary>
			/// For each solution, generate the prefix and a string representation
			/// of the solution.
			/// </summary>
			/// <remarks>
			/// For each solution, generate the prefix and a string representation
			/// of the solution. The solution starts with a newline, so that the output
			/// looks like:
			/// <prefix>,
			/// <solution>
			/// </remarks>
			internal class SolutionCatcher : DancingLinks.SolutionAcceptor<Pentomino.ColumnName
				>
			{
				public virtual void Solution(IList<IList<Pentomino.ColumnName>> answer)
				{
					string board = Pentomino.StringifySolution(this._enclosing.width, this._enclosing
						.height, answer);
					try
					{
						this._enclosing.context.Write(this._enclosing.prefixString, new Text("\n" + board
							));
						this._enclosing.context.GetCounter(this._enclosing.pent.GetCategory(answer)).Increment
							(1);
					}
					catch (IOException e)
					{
						System.Console.Error.WriteLine(StringUtils.StringifyException(e));
					}
					catch (Exception ie)
					{
						System.Console.Error.WriteLine(StringUtils.StringifyException(ie));
					}
				}

				internal SolutionCatcher(PentMap _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly PentMap _enclosing;
			}

			/// <summary>
			/// Break the prefix string into moves (a sequence of integer row ids that
			/// will be selected for each column in order).
			/// </summary>
			/// <remarks>
			/// Break the prefix string into moves (a sequence of integer row ids that
			/// will be selected for each column in order). Find all solutions with
			/// that prefix.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			protected override void Map<_T0>(WritableComparable<_T0> key, Text value, Mapper.Context
				 context)
			{
				prefixString = value;
				StringTokenizer itr = new StringTokenizer(prefixString.ToString(), ",");
				int[] prefix = new int[depth];
				int idx = 0;
				while (itr.HasMoreTokens())
				{
					string num = itr.NextToken();
					prefix[idx++] = System.Convert.ToInt32(num);
				}
				pent.Solve(prefix);
			}

			protected override void Setup(Mapper.Context context)
			{
				this.context = context;
				Configuration conf = context.GetConfiguration();
				depth = conf.GetInt(Pentomino.Depth, PentDepth);
				width = conf.GetInt(Pentomino.Width, PentWidth);
				height = conf.GetInt(Pentomino.Height, PentHeight);
				pent = (Pentomino)ReflectionUtils.NewInstance(conf.GetClass(Pentomino.Class, typeof(
					OneSidedPentomino)), conf);
				pent.Initialize(width, height);
				pent.SetPrinter(new DistributedPentomino.PentMap.SolutionCatcher(this));
			}
		}

		/// <summary>
		/// Create the input file with all of the possible combinations of the
		/// given depth.
		/// </summary>
		/// <param name="fs">the filesystem to write into</param>
		/// <param name="dir">the directory to write the input file into</param>
		/// <param name="pent">the puzzle</param>
		/// <param name="depth">the depth to explore when generating prefixes</param>
		/// <exception cref="System.IO.IOException"/>
		private static long CreateInputDirectory(FileSystem fs, Path dir, Pentomino pent, 
			int depth)
		{
			fs.Mkdirs(dir);
			IList<int[]> splits = pent.GetSplits(depth);
			Path input = new Path(dir, "part1");
			PrintWriter file = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream
				(fs.Create(input), 64 * 1024), Charsets.Utf8));
			foreach (int[] prefix in splits)
			{
				for (int i = 0; i < prefix.Length; ++i)
				{
					if (i != 0)
					{
						file.Write(',');
					}
					file.Write(prefix[i]);
				}
				file.Write('\n');
			}
			file.Close();
			return fs.GetFileStatus(input).GetLen();
		}

		/// <summary>Launch the solver on 9x10 board and the one sided pentominos.</summary>
		/// <remarks>
		/// Launch the solver on 9x10 board and the one sided pentominos.
		/// This takes about 2.5 hours on 20 nodes with 2 cpus/node.
		/// Splits the job into 2000 maps and 1 reduce.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new DistributedPentomino(), args);
			System.Environment.Exit(res);
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Configuration conf = GetConf();
			if (args.Length == 0)
			{
				System.Console.Out.WriteLine("Usage: pentomino <output> [-depth #] [-height #] [-width #]"
					);
				ToolRunner.PrintGenericCommandUsage(System.Console.Out);
				return 2;
			}
			// check for passed parameters, otherwise use defaults
			int width = conf.GetInt(Pentomino.Width, PentWidth);
			int height = conf.GetInt(Pentomino.Height, PentHeight);
			int depth = conf.GetInt(Pentomino.Depth, PentDepth);
			for (int i = 0; i < args.Length; i++)
			{
				if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-depth"))
				{
					depth = System.Convert.ToInt32(args[++i].Trim());
				}
				else
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-height"))
					{
						height = System.Convert.ToInt32(args[++i].Trim());
					}
					else
					{
						if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-width"))
						{
							width = System.Convert.ToInt32(args[++i].Trim());
						}
					}
				}
			}
			// now set the values within conf for M/R tasks to read, this
			// will ensure values are set preventing MAPREDUCE-4678
			conf.SetInt(Pentomino.Width, width);
			conf.SetInt(Pentomino.Height, height);
			conf.SetInt(Pentomino.Depth, depth);
			Type pentClass = conf.GetClass<Pentomino>(Pentomino.Class, typeof(OneSidedPentomino
				));
			int numMaps = conf.GetInt(MRJobConfig.NumMaps, DefaultMaps);
			Path output = new Path(args[0]);
			Path input = new Path(output + "_input");
			FileSystem fileSys = FileSystem.Get(conf);
			try
			{
				Job job = Job.GetInstance(conf);
				FileInputFormat.SetInputPaths(job, input);
				FileOutputFormat.SetOutputPath(job, output);
				job.SetJarByClass(typeof(DistributedPentomino.PentMap));
				job.SetJobName("dancingElephant");
				Pentomino pent = ReflectionUtils.NewInstance(pentClass, conf);
				pent.Initialize(width, height);
				long inputSize = CreateInputDirectory(fileSys, input, pent, depth);
				// for forcing the number of maps
				FileInputFormat.SetMaxInputSplitSize(job, (inputSize / numMaps));
				// the keys are the prefix strings
				job.SetOutputKeyClass(typeof(Text));
				// the values are puzzle solutions
				job.SetOutputValueClass(typeof(Text));
				job.SetMapperClass(typeof(DistributedPentomino.PentMap));
				job.SetReducerClass(typeof(Reducer));
				job.SetNumReduceTasks(1);
				return (job.WaitForCompletion(true) ? 0 : 1);
			}
			finally
			{
				fileSys.Delete(input, true);
			}
		}
	}
}
