using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A utility class.</summary>
	/// <remarks>
	/// A utility class. It provides
	/// A path filter utility to filter out output/part files in the output dir
	/// </remarks>
	public class Utils
	{
		public class OutputFileUtils
		{
			/// <summary>
			/// This class filters output(part) files from the given directory
			/// It does not accept files with filenames _logs and _SUCCESS.
			/// </summary>
			/// <remarks>
			/// This class filters output(part) files from the given directory
			/// It does not accept files with filenames _logs and _SUCCESS.
			/// This can be used to list paths of output directory as follows:
			/// Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
			/// new OutputFilesFilter()));
			/// </remarks>
			public class OutputFilesFilter : Utils.OutputFileUtils.OutputLogFilter
			{
				public override bool Accept(Path path)
				{
					return base.Accept(path) && !FileOutputCommitter.SucceededFileName.Equals(path.GetName
						());
				}
			}

			/// <summary>
			/// This class filters log files from directory given
			/// It doesnt accept paths having _logs.
			/// </summary>
			/// <remarks>
			/// This class filters log files from directory given
			/// It doesnt accept paths having _logs.
			/// This can be used to list paths of output directory as follows:
			/// Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
			/// new OutputLogFilter()));
			/// </remarks>
			public class OutputLogFilter : PathFilter
			{
				public virtual bool Accept(Path path)
				{
					return !"_logs".Equals(path.GetName());
				}
			}
		}
	}
}
