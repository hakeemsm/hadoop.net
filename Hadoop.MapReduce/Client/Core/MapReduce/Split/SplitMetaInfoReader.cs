using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Split
{
	/// <summary>
	/// A utility that reads the split meta info and creates
	/// split meta info objects
	/// </summary>
	public class SplitMetaInfoReader
	{
		/// <exception cref="System.IO.IOException"/>
		public static JobSplit.TaskSplitMetaInfo[] ReadSplitMetaInfo(JobID jobId, FileSystem
			 fs, Configuration conf, Path jobSubmitDir)
		{
			long maxMetaInfoSize = conf.GetLong(MRJobConfig.SplitMetainfoMaxsize, MRJobConfig
				.DefaultSplitMetainfoMaxsize);
			Path metaSplitFile = JobSubmissionFiles.GetJobSplitMetaFile(jobSubmitDir);
			string jobSplitFile = JobSubmissionFiles.GetJobSplitFile(jobSubmitDir).ToString();
			FileStatus fStatus = fs.GetFileStatus(metaSplitFile);
			if (maxMetaInfoSize > 0 && fStatus.GetLen() > maxMetaInfoSize)
			{
				throw new IOException("Split metadata size exceeded " + maxMetaInfoSize + ". Aborting job "
					 + jobId);
			}
			FSDataInputStream @in = fs.Open(metaSplitFile);
			byte[] header = new byte[JobSplit.MetaSplitFileHeader.Length];
			@in.ReadFully(header);
			if (!Arrays.Equals(JobSplit.MetaSplitFileHeader, header))
			{
				throw new IOException("Invalid header on split file");
			}
			int vers = WritableUtils.ReadVInt(@in);
			if (vers != JobSplit.MetaSplitVersion)
			{
				@in.Close();
				throw new IOException("Unsupported split version " + vers);
			}
			int numSplits = WritableUtils.ReadVInt(@in);
			//TODO: check for insane values
			JobSplit.TaskSplitMetaInfo[] allSplitMetaInfo = new JobSplit.TaskSplitMetaInfo[numSplits
				];
			for (int i = 0; i < numSplits; i++)
			{
				JobSplit.SplitMetaInfo splitMetaInfo = new JobSplit.SplitMetaInfo();
				splitMetaInfo.ReadFields(@in);
				JobSplit.TaskSplitIndex splitIndex = new JobSplit.TaskSplitIndex(jobSplitFile, splitMetaInfo
					.GetStartOffset());
				allSplitMetaInfo[i] = new JobSplit.TaskSplitMetaInfo(splitIndex, splitMetaInfo.GetLocations
					(), splitMetaInfo.GetInputDataLength());
			}
			@in.Close();
			return allSplitMetaInfo;
		}
	}
}
