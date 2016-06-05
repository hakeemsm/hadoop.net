using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Serializer;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Split
{
	/// <summary>
	/// The class that is used by the Job clients to write splits (both the meta
	/// and the raw bytes parts)
	/// </summary>
	public class JobSplitWriter
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(JobSplitWriter));

		private const int splitVersion = JobSplit.MetaSplitVersion;

		private static readonly byte[] SplitFileHeader;

		static JobSplitWriter()
		{
			try
			{
				SplitFileHeader = Sharpen.Runtime.GetBytesForString("SPL", "UTF-8");
			}
			catch (UnsupportedEncodingException u)
			{
				throw new RuntimeException(u);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static void CreateSplitFiles<T>(Path jobSubmitDir, Configuration conf, FileSystem
			 fs, IList<InputSplit> splits)
			where T : InputSplit
		{
			T[] array = (T[])Sharpen.Collections.ToArray(splits, new InputSplit[splits.Count]
				);
			CreateSplitFiles(jobSubmitDir, conf, fs, array);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static void CreateSplitFiles<T>(Path jobSubmitDir, Configuration conf, FileSystem
			 fs, T[] splits)
			where T : InputSplit
		{
			FSDataOutputStream @out = CreateFile(fs, JobSubmissionFiles.GetJobSplitFile(jobSubmitDir
				), conf);
			JobSplit.SplitMetaInfo[] info = WriteNewSplits(conf, splits, @out);
			@out.Close();
			WriteJobSplitMetaInfo(fs, JobSubmissionFiles.GetJobSplitMetaFile(jobSubmitDir), new 
				FsPermission(JobSubmissionFiles.JobFilePermission), splitVersion, info);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CreateSplitFiles(Path jobSubmitDir, Configuration conf, FileSystem
			 fs, InputSplit[] splits)
		{
			FSDataOutputStream @out = CreateFile(fs, JobSubmissionFiles.GetJobSplitFile(jobSubmitDir
				), conf);
			JobSplit.SplitMetaInfo[] info = WriteOldSplits(splits, @out, conf);
			@out.Close();
			WriteJobSplitMetaInfo(fs, JobSubmissionFiles.GetJobSplitMetaFile(jobSubmitDir), new 
				FsPermission(JobSubmissionFiles.JobFilePermission), splitVersion, info);
		}

		/// <exception cref="System.IO.IOException"/>
		private static FSDataOutputStream CreateFile(FileSystem fs, Path splitFile, Configuration
			 job)
		{
			FSDataOutputStream @out = FileSystem.Create(fs, splitFile, new FsPermission(JobSubmissionFiles
				.JobFilePermission));
			int replication = job.GetInt(Job.SubmitReplication, 10);
			fs.SetReplication(splitFile, (short)replication);
			WriteSplitHeader(@out);
			return @out;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteSplitHeader(FSDataOutputStream @out)
		{
			@out.Write(SplitFileHeader);
			@out.WriteInt(splitVersion);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private static JobSplit.SplitMetaInfo[] WriteNewSplits<T>(Configuration conf, T[]
			 array, FSDataOutputStream @out)
			where T : InputSplit
		{
			JobSplit.SplitMetaInfo[] info = new JobSplit.SplitMetaInfo[array.Length];
			if (array.Length != 0)
			{
				SerializationFactory factory = new SerializationFactory(conf);
				int i = 0;
				int maxBlockLocations = conf.GetInt(MRConfig.MaxBlockLocationsKey, MRConfig.MaxBlockLocationsDefault
					);
				long offset = @out.GetPos();
				foreach (T split in array)
				{
					long prevCount = @out.GetPos();
					Text.WriteString(@out, split.GetType().FullName);
					Org.Apache.Hadoop.IO.Serializer.Serializer<T> serializer = factory.GetSerializer(
						(Type)split.GetType());
					serializer.Open(@out);
					serializer.Serialize(split);
					long currCount = @out.GetPos();
					string[] locations = split.GetLocations();
					if (locations.Length > maxBlockLocations)
					{
						Log.Warn("Max block location exceeded for split: " + split + " splitsize: " + locations
							.Length + " maxsize: " + maxBlockLocations);
						locations = Arrays.CopyOf(locations, maxBlockLocations);
					}
					info[i++] = new JobSplit.SplitMetaInfo(locations, offset, split.GetLength());
					offset += currCount - prevCount;
				}
			}
			return info;
		}

		/// <exception cref="System.IO.IOException"/>
		private static JobSplit.SplitMetaInfo[] WriteOldSplits(InputSplit[] splits, FSDataOutputStream
			 @out, Configuration conf)
		{
			JobSplit.SplitMetaInfo[] info = new JobSplit.SplitMetaInfo[splits.Length];
			if (splits.Length != 0)
			{
				int i = 0;
				long offset = @out.GetPos();
				int maxBlockLocations = conf.GetInt(MRConfig.MaxBlockLocationsKey, MRConfig.MaxBlockLocationsDefault
					);
				foreach (InputSplit split in splits)
				{
					long prevLen = @out.GetPos();
					Text.WriteString(@out, split.GetType().FullName);
					split.Write(@out);
					long currLen = @out.GetPos();
					string[] locations = split.GetLocations();
					if (locations.Length > maxBlockLocations)
					{
						Log.Warn("Max block location exceeded for split: " + split + " splitsize: " + locations
							.Length + " maxsize: " + maxBlockLocations);
						locations = Arrays.CopyOf(locations, maxBlockLocations);
					}
					info[i++] = new JobSplit.SplitMetaInfo(locations, offset, split.GetLength());
					offset += currLen - prevLen;
				}
			}
			return info;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteJobSplitMetaInfo(FileSystem fs, Path filename, FsPermission
			 p, int splitMetaInfoVersion, JobSplit.SplitMetaInfo[] allSplitMetaInfo)
		{
			// write the splits meta-info to a file for the job tracker
			FSDataOutputStream @out = FileSystem.Create(fs, filename, p);
			@out.Write(JobSplit.MetaSplitFileHeader);
			WritableUtils.WriteVInt(@out, splitMetaInfoVersion);
			WritableUtils.WriteVInt(@out, allSplitMetaInfo.Length);
			foreach (JobSplit.SplitMetaInfo splitMetaInfo in allSplitMetaInfo)
			{
				splitMetaInfo.Write(@out);
			}
			@out.Close();
		}
	}
}
