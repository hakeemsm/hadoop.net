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
	/// these methods are from the Child running the Task.
	/// </remarks>
	public class MROutputFiles : MapOutputFile
	{
		private LocalDirAllocator lDirAlloc = new LocalDirAllocator(MRConfig.LocalDir);

		public MROutputFiles()
		{
		}

		/// <summary>Return the path to local map output file created earlier</summary>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetOutputFile()
		{
			return lDirAlloc.GetLocalPathToRead(MRJobConfig.Output + Path.Separator + MapOutputFilenameString
				, GetConf());
		}

		/// <summary>Create a local map output file name.</summary>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetOutputFileForWrite(long size)
		{
			return lDirAlloc.GetLocalPathForWrite(MRJobConfig.Output + Path.Separator + MapOutputFilenameString
				, size, GetConf());
		}

		/// <summary>Create a local map output file name on the same volume.</summary>
		public override Path GetOutputFileForWriteInVolume(Path existing)
		{
			return new Path(existing.GetParent(), MapOutputFilenameString);
		}

		/// <summary>Return the path to a local map output index file created earlier</summary>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetOutputIndexFile()
		{
			return lDirAlloc.GetLocalPathToRead(MRJobConfig.Output + Path.Separator + MapOutputFilenameString
				 + MapOutputIndexSuffixString, GetConf());
		}

		/// <summary>Create a local map output index file name.</summary>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetOutputIndexFileForWrite(long size)
		{
			return lDirAlloc.GetLocalPathForWrite(MRJobConfig.Output + Path.Separator + MapOutputFilenameString
				 + MapOutputIndexSuffixString, size, GetConf());
		}

		/// <summary>Create a local map output index file name on the same volume.</summary>
		public override Path GetOutputIndexFileForWriteInVolume(Path existing)
		{
			return new Path(existing.GetParent(), MapOutputFilenameString + MapOutputIndexSuffixString
				);
		}

		/// <summary>Return a local map spill file created earlier.</summary>
		/// <param name="spillNumber">the number</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetSpillFile(int spillNumber)
		{
			return lDirAlloc.GetLocalPathToRead(MRJobConfig.Output + "/spill" + spillNumber +
				 ".out", GetConf());
		}

		/// <summary>Create a local map spill file name.</summary>
		/// <param name="spillNumber">the number</param>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetSpillFileForWrite(int spillNumber, long size)
		{
			return lDirAlloc.GetLocalPathForWrite(MRJobConfig.Output + "/spill" + spillNumber
				 + ".out", size, GetConf());
		}

		/// <summary>Return a local map spill index file created earlier</summary>
		/// <param name="spillNumber">the number</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetSpillIndexFile(int spillNumber)
		{
			return lDirAlloc.GetLocalPathToRead(MRJobConfig.Output + "/spill" + spillNumber +
				 ".out.index", GetConf());
		}

		/// <summary>Create a local map spill index file name.</summary>
		/// <param name="spillNumber">the number</param>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetSpillIndexFileForWrite(int spillNumber, long size)
		{
			return lDirAlloc.GetLocalPathForWrite(MRJobConfig.Output + "/spill" + spillNumber
				 + ".out.index", size, GetConf());
		}

		/// <summary>Return a local reduce input file created earlier</summary>
		/// <param name="mapId">a map task id</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetInputFile(int mapId)
		{
			return lDirAlloc.GetLocalPathToRead(string.Format(ReduceInputFileFormatString, MRJobConfig
				.Output, Sharpen.Extensions.ValueOf(mapId)), GetConf());
		}

		/// <summary>Create a local reduce input file name.</summary>
		/// <param name="mapId">a map task id</param>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override Path GetInputFileForWrite(TaskID mapId, long size)
		{
			return lDirAlloc.GetLocalPathForWrite(string.Format(ReduceInputFileFormatString, 
				MRJobConfig.Output, mapId.GetId()), size, GetConf());
		}

		/// <summary>Removes all of the files related to a task.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAll()
		{
			((JobConf)GetConf()).DeleteLocalFiles(MRJobConfig.Output);
		}

		public override void SetConf(Configuration conf)
		{
			if (!(conf is JobConf))
			{
				conf = new JobConf(conf);
			}
			base.SetConf(conf);
		}
	}
}
