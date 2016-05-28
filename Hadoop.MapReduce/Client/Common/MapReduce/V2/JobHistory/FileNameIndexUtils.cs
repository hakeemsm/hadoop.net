using System;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Jobhistory
{
	public class FileNameIndexUtils
	{
		internal const int JobNameTrimLength = 50;

		internal const string Delimiter = "-";

		internal const string DelimiterEscape = "%2D";

		private static readonly Log Log = LogFactory.GetLog(typeof(FileNameIndexUtils));

		private const int JobIdIndex = 0;

		private const int SubmitTimeIndex = 1;

		private const int UserIndex = 2;

		private const int JobNameIndex = 3;

		private const int FinishTimeIndex = 4;

		private const int NumMapsIndex = 5;

		private const int NumReducesIndex = 6;

		private const int JobStatusIndex = 7;

		private const int QueueNameIndex = 8;

		private const int JobStartTimeIndex = 9;

		// Sanitize job history file for predictable parsing
		// Job history file names need to be backwards compatible
		// Only append new elements to the end of this list
		/// <summary>Constructs the job history file name from the JobIndexInfo.</summary>
		/// <param name="indexInfo">the index info.</param>
		/// <returns>the done job history filename.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string GetDoneFileName(JobIndexInfo indexInfo)
		{
			StringBuilder sb = new StringBuilder();
			//JobId
			sb.Append(EscapeDelimiters(TypeConverter.FromYarn(indexInfo.GetJobId()).ToString(
				)));
			sb.Append(Delimiter);
			//SubmitTime
			sb.Append(indexInfo.GetSubmitTime());
			sb.Append(Delimiter);
			//UserName
			sb.Append(EscapeDelimiters(GetUserName(indexInfo)));
			sb.Append(Delimiter);
			//JobName
			sb.Append(EscapeDelimiters(TrimJobName(GetJobName(indexInfo))));
			sb.Append(Delimiter);
			//FinishTime
			sb.Append(indexInfo.GetFinishTime());
			sb.Append(Delimiter);
			//NumMaps
			sb.Append(indexInfo.GetNumMaps());
			sb.Append(Delimiter);
			//NumReduces
			sb.Append(indexInfo.GetNumReduces());
			sb.Append(Delimiter);
			//JobStatus
			sb.Append(indexInfo.GetJobStatus());
			sb.Append(Delimiter);
			//QueueName
			sb.Append(EscapeDelimiters(GetQueueName(indexInfo)));
			sb.Append(Delimiter);
			//JobStartTime
			sb.Append(indexInfo.GetJobStartTime());
			sb.Append(JobHistoryUtils.JobHistoryFileExtension);
			return EncodeJobHistoryFileName(sb.ToString());
		}

		/// <summary>
		/// Parses the provided job history file name to construct a
		/// JobIndexInfo object which is returned.
		/// </summary>
		/// <param name="jhFileName">the job history filename.</param>
		/// <returns>a JobIndexInfo object built from the filename.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static JobIndexInfo GetIndexInfo(string jhFileName)
		{
			string fileName = Sharpen.Runtime.Substring(jhFileName, 0, jhFileName.IndexOf(JobHistoryUtils
				.JobHistoryFileExtension));
			JobIndexInfo indexInfo = new JobIndexInfo();
			string[] jobDetails = fileName.Split(Delimiter);
			JobID oldJobId = JobID.ForName(DecodeJobHistoryFileName(jobDetails[JobIdIndex]));
			JobId jobId = TypeConverter.ToYarn(oldJobId);
			indexInfo.SetJobId(jobId);
			// Do not fail if there are some minor parse errors
			try
			{
				try
				{
					indexInfo.SetSubmitTime(long.Parse(DecodeJobHistoryFileName(jobDetails[SubmitTimeIndex
						])));
				}
				catch (FormatException e)
				{
					Log.Warn("Unable to parse submit time from job history file " + jhFileName + " : "
						 + e);
				}
				indexInfo.SetUser(DecodeJobHistoryFileName(jobDetails[UserIndex]));
				indexInfo.SetJobName(DecodeJobHistoryFileName(jobDetails[JobNameIndex]));
				try
				{
					indexInfo.SetFinishTime(long.Parse(DecodeJobHistoryFileName(jobDetails[FinishTimeIndex
						])));
				}
				catch (FormatException e)
				{
					Log.Warn("Unable to parse finish time from job history file " + jhFileName + " : "
						 + e);
				}
				try
				{
					indexInfo.SetNumMaps(System.Convert.ToInt32(DecodeJobHistoryFileName(jobDetails[NumMapsIndex
						])));
				}
				catch (FormatException e)
				{
					Log.Warn("Unable to parse num maps from job history file " + jhFileName + " : " +
						 e);
				}
				try
				{
					indexInfo.SetNumReduces(System.Convert.ToInt32(DecodeJobHistoryFileName(jobDetails
						[NumReducesIndex])));
				}
				catch (FormatException e)
				{
					Log.Warn("Unable to parse num reduces from job history file " + jhFileName + " : "
						 + e);
				}
				indexInfo.SetJobStatus(DecodeJobHistoryFileName(jobDetails[JobStatusIndex]));
				indexInfo.SetQueueName(DecodeJobHistoryFileName(jobDetails[QueueNameIndex]));
				try
				{
					if (jobDetails.Length <= JobStartTimeIndex)
					{
						indexInfo.SetJobStartTime(indexInfo.GetSubmitTime());
					}
					else
					{
						indexInfo.SetJobStartTime(long.Parse(DecodeJobHistoryFileName(jobDetails[JobStartTimeIndex
							])));
					}
				}
				catch (FormatException e)
				{
					Log.Warn("Unable to parse start time from job history file " + jhFileName + " : "
						 + e);
				}
			}
			catch (IndexOutOfRangeException)
			{
				Log.Warn("Parsing job history file with partial data encoded into name: " + jhFileName
					);
			}
			return indexInfo;
		}

		/// <summary>
		/// Helper function to encode the URL of the filename of the job-history
		/// log file.
		/// </summary>
		/// <param name="logFileName">file name of the job-history file</param>
		/// <returns>URL encoded filename</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string EncodeJobHistoryFileName(string logFileName)
		{
			string replacementDelimiterEscape = null;
			// Temporarily protect the escape delimiters from encoding
			if (logFileName.Contains(DelimiterEscape))
			{
				replacementDelimiterEscape = NonOccursString(logFileName);
				logFileName = logFileName.ReplaceAll(DelimiterEscape, replacementDelimiterEscape);
			}
			string encodedFileName = null;
			try
			{
				encodedFileName = URLEncoder.Encode(logFileName, "UTF-8");
			}
			catch (UnsupportedEncodingException uee)
			{
				IOException ioe = new IOException();
				Sharpen.Extensions.InitCause(ioe, uee);
				ioe.SetStackTrace(uee.GetStackTrace());
				throw ioe;
			}
			// Restore protected escape delimiters after encoding
			if (replacementDelimiterEscape != null)
			{
				encodedFileName = encodedFileName.ReplaceAll(replacementDelimiterEscape, DelimiterEscape
					);
			}
			return encodedFileName;
		}

		/// <summary>
		/// Helper function to decode the URL of the filename of the job-history
		/// log file.
		/// </summary>
		/// <param name="logFileName">file name of the job-history file</param>
		/// <returns>URL decoded filename</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string DecodeJobHistoryFileName(string logFileName)
		{
			string decodedFileName = null;
			try
			{
				decodedFileName = URLDecoder.Decode(logFileName, "UTF-8");
			}
			catch (UnsupportedEncodingException uee)
			{
				IOException ioe = new IOException();
				Sharpen.Extensions.InitCause(ioe, uee);
				ioe.SetStackTrace(uee.GetStackTrace());
				throw ioe;
			}
			return decodedFileName;
		}

		internal static string NonOccursString(string logFileName)
		{
			int adHocIndex = 0;
			string unfoundString = "q" + adHocIndex;
			while (logFileName.Contains(unfoundString))
			{
				unfoundString = "q" + ++adHocIndex;
			}
			return unfoundString + "q";
		}

		private static string GetUserName(JobIndexInfo indexInfo)
		{
			return GetNonEmptyString(indexInfo.GetUser());
		}

		private static string GetJobName(JobIndexInfo indexInfo)
		{
			return GetNonEmptyString(indexInfo.GetJobName());
		}

		private static string GetQueueName(JobIndexInfo indexInfo)
		{
			return GetNonEmptyString(indexInfo.GetQueueName());
		}

		//TODO Maybe handle default values for longs and integers here?
		private static string GetNonEmptyString(string @in)
		{
			if (@in == null || @in.Length == 0)
			{
				@in = "NA";
			}
			return @in;
		}

		private static string EscapeDelimiters(string escapee)
		{
			return escapee.ReplaceAll(Delimiter, DelimiterEscape);
		}

		/// <summary>Trims the job-name if required</summary>
		private static string TrimJobName(string jobName)
		{
			if (jobName.Length > JobNameTrimLength)
			{
				jobName = Sharpen.Runtime.Substring(jobName, 0, JobNameTrimLength);
			}
			return jobName;
		}
	}
}
