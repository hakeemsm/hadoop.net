using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
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
		private static readonly PathFilter LogFilter = new Utils.OutputFileUtils.OutputLogFilter
			();

		public virtual bool Accept(Path path)
		{
			return LogFilter.Accept(path);
		}
	}
}
