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
	/// these methods are from child space and see mapreduce.cluster.local.dir as
	/// taskTracker/jobCache/jobId/attemptId
	/// This class should not be used from TaskTracker space.
	/// </remarks>
	public abstract class MapOutputFile : Configurable
	{
		private Configuration conf;

		internal const string MapOutputFilenameString = "file.out";

		internal const string MapOutputIndexSuffixString = ".index";

		internal const string ReduceInputFileFormatString = "%s/map_%d.out";

		public MapOutputFile()
		{
		}

		/// <summary>Return the path to local map output file created earlier</summary>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract Path GetOutputFile();

		/// <summary>Create a local map output file name.</summary>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract Path GetOutputFileForWrite(long size);

		/// <summary>Create a local map output file name on the same volume.</summary>
		public abstract Path GetOutputFileForWriteInVolume(Path existing);

		/// <summary>Return the path to a local map output index file created earlier</summary>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract Path GetOutputIndexFile();

		/// <summary>Create a local map output index file name.</summary>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract Path GetOutputIndexFileForWrite(long size);

		/// <summary>Create a local map output index file name on the same volume.</summary>
		public abstract Path GetOutputIndexFileForWriteInVolume(Path existing);

		/// <summary>Return a local map spill file created earlier.</summary>
		/// <param name="spillNumber">the number</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract Path GetSpillFile(int spillNumber);

		/// <summary>Create a local map spill file name.</summary>
		/// <param name="spillNumber">the number</param>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract Path GetSpillFileForWrite(int spillNumber, long size);

		/// <summary>Return a local map spill index file created earlier</summary>
		/// <param name="spillNumber">the number</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract Path GetSpillIndexFile(int spillNumber);

		/// <summary>Create a local map spill index file name.</summary>
		/// <param name="spillNumber">the number</param>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract Path GetSpillIndexFileForWrite(int spillNumber, long size);

		/// <summary>Return a local reduce input file created earlier</summary>
		/// <param name="mapId">a map task id</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract Path GetInputFile(int mapId);

		/// <summary>Create a local reduce input file name.</summary>
		/// <param name="mapId">a map task id</param>
		/// <param name="size">the size of the file</param>
		/// <returns>path</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract Path GetInputFileForWrite(TaskID mapId, long size);

		/// <summary>Removes all of the files related to a task.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RemoveAll();

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}
	}
}
