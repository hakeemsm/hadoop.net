using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>Class which provides a report for the given operation output</summary>
	internal class ReportWriter
	{
		internal const string OkTimeTaken = "milliseconds_taken";

		internal const string Failures = "failures";

		internal const string Successes = "successes";

		internal const string BytesWritten = "bytes_written";

		internal const string FilesCreated = "files_created";

		internal const string DirEntries = "dir_entries";

		internal const string OpCount = "op_count";

		internal const string ChunksVerified = "chunks_verified";

		internal const string ChunksUnverified = "chunks_unverified";

		internal const string BytesRead = "bytes_read";

		internal const string NotFound = "files_not_found";

		internal const string BadFiles = "bad_files";

		private static readonly Log Log = LogFactory.GetLog(typeof(ReportWriter));

		private const string SectionDelim = "-------------";

		// simple measurement types
		// expect long values
		// these will be reported on + rates by this reporter
		/// <returns>String to be used for as a section delimiter</returns>
		private string GetSectionDelimiter()
		{
			return SectionDelim;
		}

		/// <summary>
		/// Writes a message the the logging library and the given print writer (if it
		/// is not null)
		/// </summary>
		/// <param name="msg">the message to write</param>
		/// <param name="os">the print writer if specified to also write to</param>
		private void WriteMessage(string msg, PrintWriter os)
		{
			Log.Info(msg);
			if (os != null)
			{
				os.WriteLine(msg);
			}
		}

		/// <summary>
		/// Provides a simple report showing only the input size, and for each
		/// operation the operation type, measurement type and its values.
		/// </summary>
		/// <param name="input">the list of operations to report on</param>
		/// <param name="os">
		/// any print writer for which output should be written to (along with
		/// the logging library)
		/// </param>
		internal virtual void BasicReport(IList<OperationOutput> input, PrintWriter os)
		{
			WriteMessage("Default report for " + input.Count + " operations ", os);
			WriteMessage(GetSectionDelimiter(), os);
			foreach (OperationOutput data in input)
			{
				WriteMessage("Operation \"" + data.GetOperationType() + "\" measuring \"" + data.
					GetMeasurementType() + "\" = " + data.GetValue(), os);
			}
			WriteMessage(GetSectionDelimiter(), os);
		}

		/// <summary>Provides a more detailed report for a given operation.</summary>
		/// <remarks>
		/// Provides a more detailed report for a given operation. This will output the
		/// keys and values for all input and then sort based on measurement type and
		/// attempt to show rates for various metrics which have expected types to be
		/// able to measure there rate. Currently this will show rates for bytes
		/// written, success count, files created, directory entries, op count and
		/// bytes read if the variable for time taken is available for each measurement
		/// type.
		/// </remarks>
		/// <param name="operation">the operation that is being reported on.</param>
		/// <param name="input">the set of data for that that operation.</param>
		/// <param name="os">
		/// any print writer for which output should be written to (along with
		/// the logging library)
		/// </param>
		internal virtual void OpReport(string operation, IList<OperationOutput> input, PrintWriter
			 os)
		{
			WriteMessage("Basic report for operation type " + operation, os);
			WriteMessage(GetSectionDelimiter(), os);
			foreach (OperationOutput data in input)
			{
				WriteMessage("Measurement \"" + data.GetMeasurementType() + "\" = " + data.GetValue
					(), os);
			}
			// split up into measurement types for rates...
			IDictionary<string, OperationOutput> combined = new SortedDictionary<string, OperationOutput
				>();
			foreach (OperationOutput data_1 in input)
			{
				if (combined.Contains(data_1.GetMeasurementType()))
				{
					OperationOutput curr = combined[data_1.GetMeasurementType()];
					combined[data_1.GetMeasurementType()] = OperationOutput.Merge(curr, data_1);
				}
				else
				{
					combined[data_1.GetMeasurementType()] = data_1;
				}
			}
			// handle the known types
			OperationOutput timeTaken = combined[OkTimeTaken];
			if (timeTaken != null)
			{
				long mTaken = long.Parse(timeTaken.GetValue().ToString());
				if (mTaken > 0)
				{
					NumberFormat formatter = Formatter.GetDecimalFormatter();
					foreach (string measurementType in combined.Keys)
					{
						double rate = null;
						string rateType = string.Empty;
						if (measurementType.Equals(BytesWritten))
						{
							long mbWritten = long.Parse(combined[measurementType].GetValue().ToString()) / (Constants
								.Megabytes);
							rate = (double)mbWritten / (double)(mTaken / 1000.0d);
							rateType = "MB/sec";
						}
						else
						{
							if (measurementType.Equals(Successes))
							{
								long succ = long.Parse(combined[measurementType].GetValue().ToString());
								rate = (double)succ / (double)(mTaken / 1000.0d);
								rateType = "successes/sec";
							}
							else
							{
								if (measurementType.Equals(FilesCreated))
								{
									long filesCreated = long.Parse(combined[measurementType].GetValue().ToString());
									rate = (double)filesCreated / (double)(mTaken / 1000.0d);
									rateType = "files created/sec";
								}
								else
								{
									if (measurementType.Equals(DirEntries))
									{
										long entries = long.Parse(combined[measurementType].GetValue().ToString());
										rate = (double)entries / (double)(mTaken / 1000.0d);
										rateType = "directory entries/sec";
									}
									else
									{
										if (measurementType.Equals(OpCount))
										{
											long opCount = long.Parse(combined[measurementType].GetValue().ToString());
											rate = (double)opCount / (double)(mTaken / 1000.0d);
											rateType = "operations/sec";
										}
										else
										{
											if (measurementType.Equals(BytesRead))
											{
												long mbRead = long.Parse(combined[measurementType].GetValue().ToString()) / (Constants
													.Megabytes);
												rate = (double)mbRead / (double)(mTaken / 1000.0d);
												rateType = "MB/sec";
											}
										}
									}
								}
							}
						}
						if (rate != null)
						{
							WriteMessage("Rate for measurement \"" + measurementType + "\" = " + formatter.Format
								(rate) + " " + rateType, os);
						}
					}
				}
			}
			WriteMessage(GetSectionDelimiter(), os);
		}
	}
}
