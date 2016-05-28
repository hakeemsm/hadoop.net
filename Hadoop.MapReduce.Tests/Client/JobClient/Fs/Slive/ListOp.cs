using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Operation which selects a random directory and attempts to list that
	/// directory (if it exists)
	/// This operation will capture statistics on success the time taken to list that
	/// directory and the number of successful listings that occurred as well as the
	/// number of entries in the selected directory and on failure or error it will
	/// capture the number of failures and the amount of time taken to fail
	/// </summary>
	internal class ListOp : Operation
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Slive.ListOp
			));

		internal ListOp(ConfigExtractor cfg, Random rnd)
			: base(typeof(Org.Apache.Hadoop.FS.Slive.ListOp).Name, cfg, rnd)
		{
		}

		/// <summary>Gets the directory to list</summary>
		/// <returns>Path</returns>
		protected internal virtual Path GetDirectory()
		{
			Path dir = GetFinder().GetDirectory();
			return dir;
		}

		internal override IList<OperationOutput> Run(FileSystem fs)
		{
			// Operation
			IList<OperationOutput> @out = base.Run(fs);
			try
			{
				Path dir = GetDirectory();
				long dirEntries = 0;
				long timeTaken = 0;
				{
					long startTime = Timer.Now();
					FileStatus[] files = fs.ListStatus(dir);
					timeTaken = Timer.Elapsed(startTime);
					dirEntries = files.Length;
				}
				// log stats
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.OkTimeTaken, timeTaken));
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.Successes, 1L));
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.DirEntries, dirEntries));
				Log.Info("Directory " + dir + " has " + dirEntries + " entries");
			}
			catch (FileNotFoundException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.NotFound, 1L));
				Log.Warn("Error with listing", e);
			}
			catch (IOException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.Failures, 1L));
				Log.Warn("Error with listing", e);
			}
			return @out;
		}
	}
}
