using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.Terasort
{
	/// <summary>
	/// Generate 1 mapper per a file that checks to make sure the keys
	/// are sorted within each file.
	/// </summary>
	/// <remarks>
	/// Generate 1 mapper per a file that checks to make sure the keys
	/// are sorted within each file. The mapper also generates
	/// "$file:begin", first key and "$file:end", last key. The reduce verifies that
	/// all of the start/end items are in order.
	/// Any output from the reduce is problem report.
	/// <p>
	/// To run the program:
	/// <b>bin/hadoop jar hadoop-*-examples.jar teravalidate out-dir report-dir</b>
	/// <p>
	/// If there is any output, something is wrong and the output of the reduce
	/// will have the problem report.
	/// </remarks>
	public class TeraValidate : Configured, Tool
	{
		private static readonly Text Error = new Text("error");

		private static readonly Text Checksum = new Text("checksum");

		private static string TextifyBytes(Text t)
		{
			BytesWritable b = new BytesWritable();
			b.Set(t.GetBytes(), 0, t.GetLength());
			return b.ToString();
		}

		internal class ValidateMapper : Mapper<Text, Text, Text, Text>
		{
			private Text lastKey;

			private string filename;

			private Unsigned16 checksum = new Unsigned16();

			private Unsigned16 tmp = new Unsigned16();

			private Checksum crc32 = new PureJavaCrc32();

			/// <summary>Get the final part of the input name</summary>
			/// <param name="split">the input split</param>
			/// <returns>the "part-r-00000" for the input</returns>
			private string GetFilename(FileSplit split)
			{
				return split.GetPath().GetName();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(Text key, Text value, Mapper.Context context)
			{
				if (lastKey == null)
				{
					FileSplit fs = (FileSplit)context.GetInputSplit();
					filename = GetFilename(fs);
					context.Write(new Text(filename + ":begin"), key);
					lastKey = new Text();
				}
				else
				{
					if (key.CompareTo(lastKey) < 0)
					{
						context.Write(Error, new Text("misorder in " + filename + " between " + TextifyBytes
							(lastKey) + " and " + TextifyBytes(key)));
					}
				}
				// compute the crc of the key and value and add it to the sum
				crc32.Reset();
				crc32.Update(key.GetBytes(), 0, key.GetLength());
				crc32.Update(value.GetBytes(), 0, value.GetLength());
				tmp.Set(crc32.GetValue());
				checksum.Add(tmp);
				lastKey.Set(key);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Mapper.Context context)
			{
				if (lastKey != null)
				{
					context.Write(new Text(filename + ":end"), lastKey);
					context.Write(Checksum, new Text(checksum.ToString()));
				}
			}
		}

		/// <summary>
		/// Check the boundaries between the output files by making sure that the
		/// boundary keys are always increasing.
		/// </summary>
		/// <remarks>
		/// Check the boundaries between the output files by making sure that the
		/// boundary keys are always increasing.
		/// Also passes any error reports along intact.
		/// </remarks>
		internal class ValidateReducer : Reducer<Text, Text, Text, Text>
		{
			private bool firstKey = true;

			private Text lastKey = new Text();

			private Text lastValue = new Text();

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(Text key, IEnumerable<Text> values, Reducer.Context
				 context)
			{
				if (Error.Equals(key))
				{
					foreach (Text val in values)
					{
						context.Write(key, val);
					}
				}
				else
				{
					if (Checksum.Equals(key))
					{
						Unsigned16 tmp = new Unsigned16();
						Unsigned16 sum = new Unsigned16();
						foreach (Text val in values)
						{
							tmp.Set(val.ToString());
							sum.Add(tmp);
						}
						context.Write(Checksum, new Text(sum.ToString()));
					}
					else
					{
						Text value = values.GetEnumerator().Next();
						if (firstKey)
						{
							firstKey = false;
						}
						else
						{
							if (value.CompareTo(lastValue) < 0)
							{
								context.Write(Error, new Text("bad key partitioning:\n  file " + lastKey + " key "
									 + TextifyBytes(lastValue) + "\n  file " + key + " key " + TextifyBytes(value)));
							}
						}
						lastKey.Set(key);
						lastValue.Set(value);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Usage()
		{
			System.Console.Error.WriteLine("teravalidate <out-dir> <report-dir>");
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Job job = Job.GetInstance(GetConf());
			if (args.Length != 2)
			{
				Usage();
				return 1;
			}
			TeraInputFormat.SetInputPaths(job, new Path(args[0]));
			FileOutputFormat.SetOutputPath(job, new Path(args[1]));
			job.SetJobName("TeraValidate");
			job.SetJarByClass(typeof(TeraValidate));
			job.SetMapperClass(typeof(TeraValidate.ValidateMapper));
			job.SetReducerClass(typeof(TeraValidate.ValidateReducer));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(Text));
			// force a single reducer
			job.SetNumReduceTasks(1);
			// force a single split 
			FileInputFormat.SetMinInputSplitSize(job, long.MaxValue);
			job.SetInputFormatClass(typeof(TeraInputFormat));
			return job.WaitForCompletion(true) ? 0 : 1;
		}

		/// <param name="args"/>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new TeraValidate(), args);
			System.Environment.Exit(res);
		}
	}
}
