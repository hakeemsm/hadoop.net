using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Operation which selects a random file and attempts to delete that file (if it
	/// exists)
	/// This operation will capture statistics on success the time taken to delete
	/// and the number of successful deletions that occurred and on failure or error
	/// it will capture the number of failures and the amount of time taken to fail
	/// </summary>
	internal class DeleteOp : Operation
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Slive.DeleteOp
			));

		internal DeleteOp(ConfigExtractor cfg, Random rnd)
			: base(typeof(Org.Apache.Hadoop.FS.Slive.DeleteOp).Name, cfg, rnd)
		{
		}

		/// <summary>Gets the file to delete</summary>
		protected internal virtual Path GetDeleteFile()
		{
			Path fn = GetFinder().GetFile();
			return fn;
		}

		internal override IList<OperationOutput> Run(FileSystem fs)
		{
			// Operation
			IList<OperationOutput> @out = base.Run(fs);
			try
			{
				Path fn = GetDeleteFile();
				long timeTaken = 0;
				bool deleteStatus = false;
				{
					long startTime = Timer.Now();
					deleteStatus = fs.Delete(fn, false);
					timeTaken = Timer.Elapsed(startTime);
				}
				// collect the stats
				if (!deleteStatus)
				{
					@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
						.Failures, 1L));
					Log.Info("Could not delete " + fn);
				}
				else
				{
					@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
						.OkTimeTaken, timeTaken));
					@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
						.Successes, 1L));
					Log.Info("Could delete " + fn);
				}
			}
			catch (FileNotFoundException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.NotFound, 1L));
				Log.Warn("Error with deleting", e);
			}
			catch (IOException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.Failures, 1L));
				Log.Warn("Error with deleting", e);
			}
			return @out;
		}
	}
}
