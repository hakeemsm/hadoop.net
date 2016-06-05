using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Lib.Reduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	/// <summary>
	/// MultiFileWordCount is an example to demonstrate the usage of
	/// MultiFileInputFormat.
	/// </summary>
	/// <remarks>
	/// MultiFileWordCount is an example to demonstrate the usage of
	/// MultiFileInputFormat. This examples counts the occurrences of
	/// words in the text files under the given input directory.
	/// </remarks>
	public class MultiFileWordCount : Configured, Tool
	{
		/// <summary>This record keeps &lt;filename,offset&gt; pairs.</summary>
		public class WordOffset : WritableComparable
		{
			private long offset;

			private string fileName;

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				this.offset = @in.ReadLong();
				this.fileName = Text.ReadString(@in);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				@out.WriteLong(offset);
				Text.WriteString(@out, fileName);
			}

			public virtual int CompareTo(object o)
			{
				MultiFileWordCount.WordOffset that = (MultiFileWordCount.WordOffset)o;
				int f = string.CompareOrdinal(this.fileName, that.fileName);
				if (f == 0)
				{
					return (int)Math.Signum((double)(this.offset - that.offset));
				}
				return f;
			}

			public override bool Equals(object obj)
			{
				if (obj is MultiFileWordCount.WordOffset)
				{
					return this.CompareTo(obj) == 0;
				}
				return false;
			}

			public override int GetHashCode()
			{
				System.Diagnostics.Debug.Assert(false, "hashCode not designed");
				return 42;
			}
			//an arbitrary constant
		}

		/// <summary>
		/// To use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Lib.Input.CombineFileInputFormat{K, V}"/>
		/// , one should extend it, to return a
		/// (custom)
		/// <see cref="Org.Apache.Hadoop.Mapreduce.RecordReader{KEYIN, VALUEIN}"/>
		/// . CombineFileInputFormat uses
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Lib.Input.CombineFileSplit"/>
		/// s.
		/// </summary>
		public class MyInputFormat : CombineFileInputFormat<MultiFileWordCount.WordOffset
			, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			public override RecordReader<MultiFileWordCount.WordOffset, Text> CreateRecordReader
				(InputSplit split, TaskAttemptContext context)
			{
				return new CombineFileRecordReader<MultiFileWordCount.WordOffset, Text>((CombineFileSplit
					)split, context, typeof(MultiFileWordCount.CombineFileLineRecordReader));
			}
		}

		/// <summary>
		/// RecordReader is responsible from extracting records from a chunk
		/// of the CombineFileSplit.
		/// </summary>
		public class CombineFileLineRecordReader : RecordReader<MultiFileWordCount.WordOffset
			, Text>
		{
			private long startOffset;

			private long end;

			private long pos;

			private FileSystem fs;

			private Path path;

			private MultiFileWordCount.WordOffset key;

			private Text value;

			private FSDataInputStream fileIn;

			private LineReader reader;

			/// <exception cref="System.IO.IOException"/>
			public CombineFileLineRecordReader(CombineFileSplit split, TaskAttemptContext context
				, int index)
			{
				//offset of the chunk;
				//end of the chunk;
				// current pos 
				this.path = split.GetPath(index);
				fs = this.path.GetFileSystem(context.GetConfiguration());
				this.startOffset = split.GetOffset(index);
				this.end = startOffset + split.GetLength(index);
				bool skipFirstLine = false;
				//open the file
				fileIn = fs.Open(path);
				if (startOffset != 0)
				{
					skipFirstLine = true;
					--startOffset;
					fileIn.Seek(startOffset);
				}
				reader = new LineReader(fileIn);
				if (skipFirstLine)
				{
					// skip first line and re-establish "startOffset".
					startOffset += reader.ReadLine(new Text(), 0, (int)Math.Min((long)int.MaxValue, end
						 - startOffset));
				}
				this.pos = startOffset;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Initialize(InputSplit split, TaskAttemptContext context)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override float GetProgress()
			{
				if (startOffset == end)
				{
					return 0.0f;
				}
				else
				{
					return Math.Min(1.0f, (pos - startOffset) / (float)(end - startOffset));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool NextKeyValue()
			{
				if (key == null)
				{
					key = new MultiFileWordCount.WordOffset();
					key.fileName = path.GetName();
				}
				key.offset = pos;
				if (value == null)
				{
					value = new Text();
				}
				int newSize = 0;
				if (pos < end)
				{
					newSize = reader.ReadLine(value);
					pos += newSize;
				}
				if (newSize == 0)
				{
					key = null;
					value = null;
					return false;
				}
				else
				{
					return true;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override MultiFileWordCount.WordOffset GetCurrentKey()
			{
				return key;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override Text GetCurrentValue()
			{
				return value;
			}
		}

		/// <summary>
		/// This Mapper is similar to the one in
		/// <see cref="TokenizerMapper"/>
		/// .
		/// </summary>
		public class MapClass : Mapper<MultiFileWordCount.WordOffset, Text, Text, IntWritable
			>
		{
			private static readonly IntWritable one = new IntWritable(1);

			private Text word = new Text();

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(MultiFileWordCount.WordOffset key, Text value, Mapper.Context
				 context)
			{
				string line = value.ToString();
				StringTokenizer itr = new StringTokenizer(line);
				while (itr.HasMoreTokens())
				{
					word.Set(itr.NextToken());
					context.Write(word, one);
				}
			}
		}

		private void PrintUsage()
		{
			System.Console.Out.WriteLine("Usage : multifilewc <input_dir> <output>");
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (args.Length < 2)
			{
				PrintUsage();
				return 2;
			}
			Job job = Job.GetInstance(GetConf());
			job.SetJobName("MultiFileWordCount");
			job.SetJarByClass(typeof(MultiFileWordCount));
			//set the InputFormat of the job to our InputFormat
			job.SetInputFormatClass(typeof(MultiFileWordCount.MyInputFormat));
			// the keys are words (strings)
			job.SetOutputKeyClass(typeof(Text));
			// the values are counts (ints)
			job.SetOutputValueClass(typeof(IntWritable));
			//use the defined mapper
			job.SetMapperClass(typeof(MultiFileWordCount.MapClass));
			//use the WordCount Reducer
			job.SetCombinerClass(typeof(IntSumReducer));
			job.SetReducerClass(typeof(IntSumReducer));
			FileInputFormat.AddInputPaths(job, args[0]);
			FileOutputFormat.SetOutputPath(job, new Path(args[1]));
			return job.WaitForCompletion(true) ? 0 : 1;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int ret = ToolRunner.Run(new MultiFileWordCount(), args);
			System.Environment.Exit(ret);
		}
	}
}
