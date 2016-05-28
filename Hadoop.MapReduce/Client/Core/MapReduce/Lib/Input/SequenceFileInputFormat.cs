using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}"/>
	/// for
	/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
	/// s.
	/// </summary>
	public class SequenceFileInputFormat<K, V> : FileInputFormat<K, V>
	{
		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<K, V> CreateRecordReader(InputSplit split, TaskAttemptContext
			 context)
		{
			return new SequenceFileRecordReader<K, V>();
		}

		protected internal override long GetFormatMinSplitSize()
		{
			return SequenceFile.SyncInterval;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override IList<FileStatus> ListStatus(JobContext job)
		{
			IList<FileStatus> files = base.ListStatus(job);
			int len = files.Count;
			for (int i = 0; i < len; ++i)
			{
				FileStatus file = files[i];
				if (file.IsDirectory())
				{
					// it's a MapFile
					Path p = file.GetPath();
					FileSystem fs = p.GetFileSystem(job.GetConfiguration());
					// use the data file
					files.Set(i, fs.GetFileStatus(new Path(p, MapFile.DataFileName)));
				}
			}
			return files;
		}
	}
}
