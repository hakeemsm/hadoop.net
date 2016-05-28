using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>NLineInputFormat which splits N lines of input as one split.</summary>
	/// <remarks>
	/// NLineInputFormat which splits N lines of input as one split.
	/// In many "pleasantly" parallel applications, each process/mapper
	/// processes the same input file (s), but with computations are
	/// controlled by different parameters.(Referred to as "parameter sweeps").
	/// One way to achieve this, is to specify a set of parameters
	/// (one set per line) as input in a control file
	/// (which is the input path to the map-reduce application,
	/// where as the input dataset is specified
	/// via a config variable in JobConf.).
	/// The NLineInputFormat can be used in such applications, that splits
	/// the input file such that by default, one line is fed as
	/// a value to one map task, and key is the offset.
	/// i.e. (k,v) is (LongWritable, Text).
	/// The location hints will span the whole mapred cluster.
	/// </remarks>
	public class NLineInputFormat : FileInputFormat<LongWritable, Text>
	{
		public const string LinesPerMap = "mapreduce.input.lineinputformat.linespermap";

		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<LongWritable, Text> CreateRecordReader(InputSplit genericSplit
			, TaskAttemptContext context)
		{
			context.SetStatus(genericSplit.ToString());
			return new LineRecordReader();
		}

		/// <summary>
		/// Logically splits the set of input files for the job, splits N lines
		/// of the input as one split.
		/// </summary>
		/// <seealso cref="FileInputFormat{K, V}.GetSplits(Org.Apache.Hadoop.Mapreduce.JobContext)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<InputSplit> GetSplits(JobContext job)
		{
			IList<InputSplit> splits = new AList<InputSplit>();
			int numLinesPerSplit = GetNumLinesPerSplit(job);
			foreach (FileStatus status in ListStatus(job))
			{
				Sharpen.Collections.AddAll(splits, GetSplitsForFile(status, job.GetConfiguration(
					), numLinesPerSplit));
			}
			return splits;
		}

		/// <exception cref="System.IO.IOException"/>
		public static IList<FileSplit> GetSplitsForFile(FileStatus status, Configuration 
			conf, int numLinesPerSplit)
		{
			IList<FileSplit> splits = new AList<FileSplit>();
			Path fileName = status.GetPath();
			if (status.IsDirectory())
			{
				throw new IOException("Not a file: " + fileName);
			}
			FileSystem fs = fileName.GetFileSystem(conf);
			LineReader lr = null;
			try
			{
				FSDataInputStream @in = fs.Open(fileName);
				lr = new LineReader(@in, conf);
				Text line = new Text();
				int numLines = 0;
				long begin = 0;
				long length = 0;
				int num = -1;
				while ((num = lr.ReadLine(line)) > 0)
				{
					numLines++;
					length += num;
					if (numLines == numLinesPerSplit)
					{
						splits.AddItem(CreateFileSplit(fileName, begin, length));
						begin += length;
						length = 0;
						numLines = 0;
					}
				}
				if (numLines != 0)
				{
					splits.AddItem(CreateFileSplit(fileName, begin, length));
				}
			}
			finally
			{
				if (lr != null)
				{
					lr.Close();
				}
			}
			return splits;
		}

		/// <summary>
		/// NLineInputFormat uses LineRecordReader, which always reads
		/// (and consumes) at least one character out of its upper split
		/// boundary.
		/// </summary>
		/// <remarks>
		/// NLineInputFormat uses LineRecordReader, which always reads
		/// (and consumes) at least one character out of its upper split
		/// boundary. So to make sure that each mapper gets N lines, we
		/// move back the upper split limits of each split
		/// by one character here.
		/// </remarks>
		/// <param name="fileName">Path of file</param>
		/// <param name="begin">the position of the first byte in the file to process</param>
		/// <param name="length">number of bytes in InputSplit</param>
		/// <returns>FileSplit</returns>
		protected internal static FileSplit CreateFileSplit(Path fileName, long begin, long
			 length)
		{
			return (begin == 0) ? new FileSplit(fileName, begin, length - 1, new string[] {  }
				) : new FileSplit(fileName, begin - 1, length, new string[] {  });
		}

		/// <summary>Set the number of lines per split</summary>
		/// <param name="job">the job to modify</param>
		/// <param name="numLines">the number of lines per split</param>
		public static void SetNumLinesPerSplit(Job job, int numLines)
		{
			job.GetConfiguration().SetInt(LinesPerMap, numLines);
		}

		/// <summary>Get the number of lines per split</summary>
		/// <param name="job">the job</param>
		/// <returns>the number of lines per split</returns>
		public static int GetNumLinesPerSplit(JobContext job)
		{
			return job.GetConfiguration().GetInt(LinesPerMap, 1);
		}
	}
}
