using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// An abstract
	/// <see cref="Org.Apache.Hadoop.Mapred.InputFormat{K, V}"/>
	/// that returns
	/// <see cref="CombineFileSplit"/>
	/// 's
	/// in
	/// <see cref="Org.Apache.Hadoop.Mapred.InputFormat{K, V}.GetSplits(Org.Apache.Hadoop.Mapred.JobConf, int)
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
	/// is similar to the default spliting behaviour in Hadoop: each
	/// block is a locally processed split.
	/// Subclasses implement
	/// <see cref="Org.Apache.Hadoop.Mapred.InputFormat{K, V}.GetRecordReader(Org.Apache.Hadoop.Mapred.InputSplit, Org.Apache.Hadoop.Mapred.JobConf, Org.Apache.Hadoop.Mapred.Reporter)
	/// 	"/>
	/// to construct <code>RecordReader</code>'s for <code>CombineFileSplit</code>'s.
	/// </summary>
	/// <seealso cref="CombineFileSplit"/>
	public abstract class CombineFileInputFormat<K, V> : Org.Apache.Hadoop.Mapreduce.Lib.Input.CombineFileInputFormat
		<K, V>, InputFormat<K, V>
	{
		/// <summary>default constructor</summary>
		public CombineFileInputFormat()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual InputSplit[] GetSplits(JobConf job, int numSplits)
		{
			IList<InputSplit> newStyleSplits = base.GetSplits(Job.GetInstance(job));
			InputSplit[] ret = new InputSplit[newStyleSplits.Count];
			for (int pos = 0; pos < newStyleSplits.Count; ++pos)
			{
				CombineFileSplit newStyleSplit = (CombineFileSplit)newStyleSplits[pos];
				ret[pos] = new CombineFileSplit(job, newStyleSplit.GetPaths(), newStyleSplit.GetStartOffsets
					(), newStyleSplit.GetLengths(), newStyleSplit.GetLocations());
			}
			return ret;
		}

		/// <summary>Create a new pool and add the filters to it.</summary>
		/// <remarks>
		/// Create a new pool and add the filters to it.
		/// A split cannot have files from different pools.
		/// </remarks>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.Lib.Input.CombineFileInputFormat{K, V}.CreatePool(System.Collections.IList{E}) ."
			)]
		protected internal virtual void CreatePool(JobConf conf, IList<PathFilter> filters
			)
		{
			CreatePool(filters);
		}

		/// <summary>Create a new pool and add the filters to it.</summary>
		/// <remarks>
		/// Create a new pool and add the filters to it.
		/// A pathname can satisfy any one of the specified filters.
		/// A split cannot have files from different pools.
		/// </remarks>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Mapreduce.Lib.Input.CombineFileInputFormat{K, V}.CreatePool(Org.Apache.Hadoop.FS.PathFilter[]) ."
			)]
		protected internal virtual void CreatePool(JobConf conf, params PathFilter[] filters
			)
		{
			CreatePool(filters);
		}

		/// <summary>This is not implemented yet.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract RecordReader<K, V> GetRecordReader(InputSplit split, JobConf job, 
			Reporter reporter);

		// abstract method from super class implemented to return null
		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<K, V> CreateRecordReader(InputSplit split, TaskAttemptContext
			 context)
		{
			return null;
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
			IList<FileStatus> result = base.ListStatus(Job.GetInstance(job));
			return Sharpen.Collections.ToArray(result, new FileStatus[result.Count]);
		}

		/// <summary>
		/// Subclasses should avoid overriding this method and should instead only
		/// override
		/// <see cref="CombineFileInputFormat{K, V}.IsSplitable(Org.Apache.Hadoop.FS.FileSystem, Org.Apache.Hadoop.FS.Path)
		/// 	"/>
		/// .  The implementation of
		/// this method simply calls the other method to preserve compatibility.
		/// </summary>
		/// <seealso><a href="https://issues.apache.org/jira/browse/MAPREDUCE-5530">
		/// * MAPREDUCE-5530</a></seealso>
		/// <param name="context">the job context</param>
		/// <param name="file">the file name to check</param>
		/// <returns>is this file splitable?</returns>
		[InterfaceAudience.Private]
		protected internal override bool IsSplitable(JobContext context, Path file)
		{
			try
			{
				return IsSplitable(FileSystem.Get(context.GetConfiguration()), file);
			}
			catch (IOException ioe)
			{
				throw new RuntimeException(ioe);
			}
		}

		protected internal virtual bool IsSplitable(FileSystem fs, Path file)
		{
			CompressionCodec codec = new CompressionCodecFactory(fs.GetConf()).GetCodec(file);
			if (null == codec)
			{
				return true;
			}
			return codec is SplittableCompressionCodec;
		}
	}
}
