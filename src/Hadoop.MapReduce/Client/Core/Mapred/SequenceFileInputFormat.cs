using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// An
	/// <see cref="InputFormat{K, V}"/>
	/// for
	/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
	/// s.
	/// </summary>
	public class SequenceFileInputFormat<K, V> : FileInputFormat<K, V>
	{
		public SequenceFileInputFormat()
		{
			SetMinSplitSize(SequenceFile.SyncInterval);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override FileStatus[] ListStatus(JobConf job)
		{
			FileStatus[] files = base.ListStatus(job);
			for (int i = 0; i < files.Length; i++)
			{
				FileStatus file = files[i];
				if (file.IsDirectory())
				{
					// it's a MapFile
					Path dataFile = new Path(file.GetPath(), MapFile.DataFileName);
					FileSystem fs = file.GetPath().GetFileSystem(job);
					// use the data file
					files[i] = fs.GetFileStatus(dataFile);
				}
			}
			return files;
		}

		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<K, V> GetRecordReader(InputSplit split, JobConf job, 
			Reporter reporter)
		{
			reporter.SetStatus(split.ToString());
			return new SequenceFileRecordReader<K, V>(job, (FileSplit)split);
		}
	}
}
