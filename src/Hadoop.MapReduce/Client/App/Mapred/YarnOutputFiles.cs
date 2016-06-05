using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Manipulate the working area for the transient store for maps and reduces.
	/// 	</summary>
	/// <remarks>
	/// Manipulate the working area for the transient store for maps and reduces.
	/// This class is used by map and reduce tasks to identify the directories that
	/// they need to write to/read from for intermediate files. The callers of
	/// these methods are from child space.
	/// </remarks>
	public class YarnOutputFiles : MapOutputFile
	{
		private JobConf conf;

		private const string JobOutputDir = "output";

		private const string SpillFilePattern = "%s_spill_%d.out";

		private const string SpillIndexFilePattern = SpillFilePattern + ".index";

		public YarnOutputFiles()
		{
		}

		private LocalDirAllocator lDirAlloc = new LocalDirAllocator(MRConfig.LocalDir);

		// assume configured to $localdir/usercache/$user/appcache/$appId
		private Path GetAttemptOutputDir()
		{
			return new Path(JobOutputDir, conf.Get(JobContext.TaskAttemptId));
		}

		/// <summary>Return the path to local map output file created earlier</summary>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetOutputFile()
		{
			Path attemptOutput = new Path(GetAttemptOutputDir(), MapOutputFilenameString);
			return lDirAlloc.GetLocalPathToRead(attemptOutput.ToString(), conf);
		}

		/// <summary>Create a local map output file name.</summary>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetOutputFileForWrite(long size)
		{
			Path attemptOutput = new Path(GetAttemptOutputDir(), MapOutputFilenameString);
			return lDirAlloc.GetLocalPathForWrite(attemptOutput.ToString(), size, conf);
		}

		/// <summary>Create a local map output file name on the same volume.</summary>
		public override Path GetOutputFileForWriteInVolume(Path existing)
		{
			Path outputDir = new Path(existing.GetParent(), JobOutputDir);
			Path attemptOutputDir = new Path(outputDir, conf.Get(JobContext.TaskAttemptId));
			return new Path(attemptOutputDir, MapOutputFilenameString);
		}

		/// <summary>Return the path to a local map output index file created earlier</summary>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetOutputIndexFile()
		{
			Path attemptIndexOutput = new Path(GetAttemptOutputDir(), MapOutputFilenameString
				 + MapOutputIndexSuffixString);
			return lDirAlloc.GetLocalPathToRead(attemptIndexOutput.ToString(), conf);
		}

		/// <summary>Create a local map output index file name.</summary>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetOutputIndexFileForWrite(long size)
		{
			Path attemptIndexOutput = new Path(GetAttemptOutputDir(), MapOutputFilenameString
				 + MapOutputIndexSuffixString);
			return lDirAlloc.GetLocalPathForWrite(attemptIndexOutput.ToString(), size, conf);
		}

		/// <summary>Create a local map output index file name on the same volume.</summary>
		public override Path GetOutputIndexFileForWriteInVolume(Path existing)
		{
			Path outputDir = new Path(existing.GetParent(), JobOutputDir);
			Path attemptOutputDir = new Path(outputDir, conf.Get(JobContext.TaskAttemptId));
			return new Path(attemptOutputDir, MapOutputFilenameString + MapOutputIndexSuffixString
				);
		}

		/// <summary>Return a local map spill file created earlier.</summary>
		/// <param name="spillNumber">the number</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetSpillFile(int spillNumber)
		{
			return lDirAlloc.GetLocalPathToRead(string.Format(SpillFilePattern, conf.Get(JobContext
				.TaskAttemptId), spillNumber), conf);
		}

		/// <summary>Create a local map spill file name.</summary>
		/// <param name="spillNumber">the number</param>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetSpillFileForWrite(int spillNumber, long size)
		{
			return lDirAlloc.GetLocalPathForWrite(string.Format(string.Format(SpillFilePattern
				, conf.Get(JobContext.TaskAttemptId), spillNumber)), size, conf);
		}

		/// <summary>Return a local map spill index file created earlier</summary>
		/// <param name="spillNumber">the number</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetSpillIndexFile(int spillNumber)
		{
			return lDirAlloc.GetLocalPathToRead(string.Format(SpillIndexFilePattern, conf.Get
				(JobContext.TaskAttemptId), spillNumber), conf);
		}

		/// <summary>Create a local map spill index file name.</summary>
		/// <param name="spillNumber">the number</param>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetSpillIndexFileForWrite(int spillNumber, long size)
		{
			return lDirAlloc.GetLocalPathForWrite(string.Format(SpillIndexFilePattern, conf.Get
				(JobContext.TaskAttemptId), spillNumber), size, conf);
		}

		/// <summary>Return a local reduce input file created earlier</summary>
		/// <param name="mapId">a map task id</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"></exception>
		public override Path GetInputFile(int mapId)
		{
			throw new NotSupportedException("Incompatible with LocalRunner");
		}

		/// <summary>Create a local reduce input file name.</summary>
		/// <param name="mapId">a map task id</param>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetInputFileForWrite(TaskID mapId, long size)
		{
			return lDirAlloc.GetLocalPathForWrite(string.Format(ReduceInputFileFormatString, 
				GetAttemptOutputDir().ToString(), mapId.GetId()), size, conf);
		}

		/// <summary>Removes all of the files related to a task.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAll()
		{
			throw new NotSupportedException("Incompatible with LocalRunner");
		}

		public override void SetConf(Configuration conf)
		{
			if (conf is JobConf)
			{
				this.conf = (JobConf)conf;
			}
			else
			{
				this.conf = new JobConf(conf);
			}
		}

		public override Configuration GetConf()
		{
			return conf;
		}
	}
}
