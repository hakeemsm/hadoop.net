using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Logaggregation
{
	public class LogCLIHelpers : Configurable
	{
		private Configuration conf;

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual int DumpAContainersLogs(string appId, string containerId, string nodeId
			, string jobOwner)
		{
			Path remoteRootLogDir = new Path(GetConf().Get(YarnConfiguration.NmRemoteAppLogDir
				, YarnConfiguration.DefaultNmRemoteAppLogDir));
			string suffix = LogAggregationUtils.GetRemoteNodeLogDirSuffix(GetConf());
			Path remoteAppLogDir = LogAggregationUtils.GetRemoteAppLogDir(remoteRootLogDir, ConverterUtils
				.ToApplicationId(appId), jobOwner, suffix);
			RemoteIterator<FileStatus> nodeFiles;
			try
			{
				Path qualifiedLogDir = FileContext.GetFileContext(GetConf()).MakeQualified(remoteAppLogDir
					);
				nodeFiles = FileContext.GetFileContext(qualifiedLogDir.ToUri(), GetConf()).ListStatus
					(remoteAppLogDir);
			}
			catch (FileNotFoundException)
			{
				LogDirNotExist(remoteAppLogDir.ToString());
				return -1;
			}
			bool foundContainerLogs = false;
			while (nodeFiles.HasNext())
			{
				FileStatus thisNodeFile = nodeFiles.Next();
				string fileName = thisNodeFile.GetPath().GetName();
				if (fileName.Contains(LogAggregationUtils.GetNodeString(nodeId)) && !fileName.EndsWith
					(LogAggregationUtils.TmpFileSuffix))
				{
					AggregatedLogFormat.LogReader reader = null;
					try
					{
						reader = new AggregatedLogFormat.LogReader(GetConf(), thisNodeFile.GetPath());
						if (DumpAContainerLogs(containerId, reader, System.Console.Out, thisNodeFile.GetModificationTime
							()) > -1)
						{
							foundContainerLogs = true;
						}
					}
					finally
					{
						if (reader != null)
						{
							reader.Close();
						}
					}
				}
			}
			if (!foundContainerLogs)
			{
				ContainerLogNotFound(containerId);
				return -1;
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual int DumpAContainerLogs(string containerIdStr, AggregatedLogFormat.LogReader
			 reader, TextWriter @out, long logUploadedTime)
		{
			DataInputStream valueStream;
			AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
			valueStream = reader.Next(key);
			while (valueStream != null && !key.ToString().Equals(containerIdStr))
			{
				// Next container
				key = new AggregatedLogFormat.LogKey();
				valueStream = reader.Next(key);
			}
			if (valueStream == null)
			{
				return -1;
			}
			bool foundContainerLogs = false;
			while (true)
			{
				try
				{
					AggregatedLogFormat.LogReader.ReadAContainerLogsForALogType(valueStream, @out, logUploadedTime
						);
					foundContainerLogs = true;
				}
				catch (EOFException)
				{
					break;
				}
			}
			if (foundContainerLogs)
			{
				return 0;
			}
			return -1;
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual int DumpAllContainersLogs(ApplicationId appId, string appOwner, TextWriter
			 @out)
		{
			Path remoteRootLogDir = new Path(GetConf().Get(YarnConfiguration.NmRemoteAppLogDir
				, YarnConfiguration.DefaultNmRemoteAppLogDir));
			string user = appOwner;
			string logDirSuffix = LogAggregationUtils.GetRemoteNodeLogDirSuffix(GetConf());
			// TODO Change this to get a list of files from the LAS.
			Path remoteAppLogDir = LogAggregationUtils.GetRemoteAppLogDir(remoteRootLogDir, appId
				, user, logDirSuffix);
			RemoteIterator<FileStatus> nodeFiles;
			try
			{
				Path qualifiedLogDir = FileContext.GetFileContext(GetConf()).MakeQualified(remoteAppLogDir
					);
				nodeFiles = FileContext.GetFileContext(qualifiedLogDir.ToUri(), GetConf()).ListStatus
					(remoteAppLogDir);
			}
			catch (FileNotFoundException)
			{
				LogDirNotExist(remoteAppLogDir.ToString());
				return -1;
			}
			bool foundAnyLogs = false;
			while (nodeFiles.HasNext())
			{
				FileStatus thisNodeFile = nodeFiles.Next();
				if (!thisNodeFile.GetPath().GetName().EndsWith(LogAggregationUtils.TmpFileSuffix))
				{
					AggregatedLogFormat.LogReader reader = new AggregatedLogFormat.LogReader(GetConf(
						), thisNodeFile.GetPath());
					try
					{
						DataInputStream valueStream;
						AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
						valueStream = reader.Next(key);
						while (valueStream != null)
						{
							string containerString = "\n\nContainer: " + key + " on " + thisNodeFile.GetPath(
								).GetName();
							@out.WriteLine(containerString);
							@out.WriteLine(StringUtils.Repeat("=", containerString.Length));
							while (true)
							{
								try
								{
									AggregatedLogFormat.LogReader.ReadAContainerLogsForALogType(valueStream, @out, thisNodeFile
										.GetModificationTime());
									foundAnyLogs = true;
								}
								catch (EOFException)
								{
									break;
								}
							}
							// Next container
							key = new AggregatedLogFormat.LogKey();
							valueStream = reader.Next(key);
						}
					}
					finally
					{
						reader.Close();
					}
				}
			}
			if (!foundAnyLogs)
			{
				EmptyLogDir(remoteAppLogDir.ToString());
				return -1;
			}
			return 0;
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		public virtual Configuration GetConf()
		{
			return this.conf;
		}

		private static void ContainerLogNotFound(string containerId)
		{
			System.Console.Out.WriteLine("Logs for container " + containerId + " are not present in this log-file."
				);
		}

		private static void LogDirNotExist(string remoteAppLogDir)
		{
			System.Console.Out.WriteLine(remoteAppLogDir + " does not exist.");
			System.Console.Out.WriteLine("Log aggregation has not completed or is not enabled."
				);
		}

		private static void EmptyLogDir(string remoteAppLogDir)
		{
			System.Console.Out.WriteLine(remoteAppLogDir + " does not have any log files.");
		}
	}
}
