using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
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
	public class NLineInputFormat : FileInputFormat<LongWritable, Text>, JobConfigurable
	{
		private int N = 1;

		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<LongWritable, Text> GetRecordReader(InputSplit genericSplit
			, JobConf job, Reporter reporter)
		{
			reporter.SetStatus(genericSplit.ToString());
			return new LineRecordReader(job, (FileSplit)genericSplit);
		}

		/// <summary>
		/// Logically splits the set of input files for the job, splits N lines
		/// of the input as one split.
		/// </summary>
		/// <seealso cref="Org.Apache.Hadoop.Mapred.FileInputFormat{K, V}.GetSplits(Org.Apache.Hadoop.Mapred.JobConf, int)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public override InputSplit[] GetSplits(JobConf job, int numSplits)
		{
			AList<FileSplit> splits = new AList<FileSplit>();
			foreach (FileStatus status in ListStatus(job))
			{
				foreach (FileSplit split in NLineInputFormat.GetSplitsForFile(status, job, N))
				{
					splits.AddItem(new FileSplit(split));
				}
			}
			return Sharpen.Collections.ToArray(splits, new FileSplit[splits.Count]);
		}

		public virtual void Configure(JobConf conf)
		{
			N = conf.GetInt("mapreduce.input.lineinputformat.linespermap", 1);
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
	}
}
