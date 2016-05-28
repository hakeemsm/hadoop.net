using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Operation which selects a random directory and attempts to create that
	/// directory.
	/// </summary>
	/// <remarks>
	/// Operation which selects a random directory and attempts to create that
	/// directory.
	/// This operation will capture statistics on success the time taken to create
	/// that directory and the number of successful creations that occurred and on
	/// failure or error it will capture the number of failures and the amount of
	/// time taken to fail
	/// </remarks>
	internal class MkdirOp : Operation
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Slive.MkdirOp
			));

		internal MkdirOp(ConfigExtractor cfg, Random rnd)
			: base(typeof(Org.Apache.Hadoop.FS.Slive.MkdirOp).Name, cfg, rnd)
		{
		}

		/// <summary>Gets the directory name to try to make</summary>
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
				bool mkRes = false;
				long timeTaken = 0;
				{
					long startTime = Timer.Now();
					mkRes = fs.Mkdirs(dir);
					timeTaken = Timer.Elapsed(startTime);
				}
				// log stats
				if (mkRes)
				{
					@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
						.OkTimeTaken, timeTaken));
					@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
						.Successes, 1L));
					Log.Info("Made directory " + dir);
				}
				else
				{
					@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
						.Failures, 1L));
					Log.Warn("Could not make " + dir);
				}
			}
			catch (FileNotFoundException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.NotFound, 1L));
				Log.Warn("Error with mkdir", e);
			}
			catch (IOException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.Failures, 1L));
				Log.Warn("Error with mkdir", e);
			}
			return @out;
		}
	}
}
