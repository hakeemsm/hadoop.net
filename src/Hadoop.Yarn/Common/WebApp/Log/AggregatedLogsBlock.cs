using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Logaggregation;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Log
{
	public class AggregatedLogsBlock : HtmlBlock
	{
		private readonly Configuration conf;

		[Com.Google.Inject.Inject]
		internal AggregatedLogsBlock(Configuration conf)
		{
			this.conf = conf;
		}

		protected internal override void Render(HtmlBlock.Block html)
		{
			ContainerId containerId = VerifyAndGetContainerId(html);
			NodeId nodeId = VerifyAndGetNodeId(html);
			string appOwner = VerifyAndGetAppOwner(html);
			AggregatedLogsBlock.LogLimits logLimits = VerifyAndGetLogLimits(html);
			if (containerId == null || nodeId == null || appOwner == null || appOwner.IsEmpty
				() || logLimits == null)
			{
				return;
			}
			ApplicationId applicationId = containerId.GetApplicationAttemptId().GetApplicationId
				();
			string logEntity = $(YarnWebParams.EntityString);
			if (logEntity == null || logEntity.IsEmpty())
			{
				logEntity = containerId.ToString();
			}
			if (!conf.GetBoolean(YarnConfiguration.LogAggregationEnabled, YarnConfiguration.DefaultLogAggregationEnabled
				))
			{
				html.H1().("Aggregation is not enabled. Try the nodemanager at " + nodeId).();
				return;
			}
			Path remoteRootLogDir = new Path(conf.Get(YarnConfiguration.NmRemoteAppLogDir, YarnConfiguration
				.DefaultNmRemoteAppLogDir));
			Path remoteAppDir = LogAggregationUtils.GetRemoteAppLogDir(remoteRootLogDir, applicationId
				, appOwner, LogAggregationUtils.GetRemoteNodeLogDirSuffix(conf));
			RemoteIterator<FileStatus> nodeFiles;
			try
			{
				Path qualifiedLogDir = FileContext.GetFileContext(conf).MakeQualified(remoteAppDir
					);
				nodeFiles = FileContext.GetFileContext(qualifiedLogDir.ToUri(), conf).ListStatus(
					remoteAppDir);
			}
			catch (FileNotFoundException)
			{
				html.H1().("Logs not available for " + logEntity + ". Aggregation may not be complete, "
					 + "Check back later or try the nodemanager at " + nodeId).();
				return;
			}
			catch (Exception)
			{
				html.H1().("Error getting logs at " + nodeId).();
				return;
			}
			bool foundLog = false;
			string desiredLogType = $(YarnWebParams.ContainerLogType);
			try
			{
				while (nodeFiles.HasNext())
				{
					AggregatedLogFormat.LogReader reader = null;
					try
					{
						FileStatus thisNodeFile = nodeFiles.Next();
						if (!thisNodeFile.GetPath().GetName().Contains(LogAggregationUtils.GetNodeString(
							nodeId)) || thisNodeFile.GetPath().GetName().EndsWith(LogAggregationUtils.TmpFileSuffix
							))
						{
							continue;
						}
						long logUploadedTime = thisNodeFile.GetModificationTime();
						reader = new AggregatedLogFormat.LogReader(conf, thisNodeFile.GetPath());
						string owner = null;
						IDictionary<ApplicationAccessType, string> appAcls = null;
						try
						{
							owner = reader.GetApplicationOwner();
							appAcls = reader.GetApplicationAcls();
						}
						catch (IOException e)
						{
							Log.Error("Error getting logs for " + logEntity, e);
							continue;
						}
						ApplicationACLsManager aclsManager = new ApplicationACLsManager(conf);
						aclsManager.AddApplication(applicationId, appAcls);
						string remoteUser = Request().GetRemoteUser();
						UserGroupInformation callerUGI = null;
						if (remoteUser != null)
						{
							callerUGI = UserGroupInformation.CreateRemoteUser(remoteUser);
						}
						if (callerUGI != null && !aclsManager.CheckAccess(callerUGI, ApplicationAccessType
							.ViewApp, owner, applicationId))
						{
							html.H1().("User [" + remoteUser + "] is not authorized to view the logs for " + 
								logEntity + " in log file [" + thisNodeFile.GetPath().GetName() + "]").();
							Log.Error("User [" + remoteUser + "] is not authorized to view the logs for " + logEntity
								);
							continue;
						}
						AggregatedLogFormat.ContainerLogsReader logReader = reader.GetContainerLogsReader
							(containerId);
						if (logReader == null)
						{
							continue;
						}
						foundLog = ReadContainerLogs(html, logReader, logLimits, desiredLogType, logUploadedTime
							);
					}
					catch (IOException ex)
					{
						Log.Error("Error getting logs for " + logEntity, ex);
						continue;
					}
					finally
					{
						if (reader != null)
						{
							reader.Close();
						}
					}
				}
				if (!foundLog)
				{
					if (desiredLogType.IsEmpty())
					{
						html.H1("No logs available for container " + containerId.ToString());
					}
					else
					{
						html.H1("Unable to locate '" + desiredLogType + "' log for container " + containerId
							.ToString());
					}
				}
			}
			catch (IOException e)
			{
				html.H1().("Error getting logs for " + logEntity).();
				Log.Error("Error getting logs for " + logEntity, e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private bool ReadContainerLogs(HtmlBlock.Block html, AggregatedLogFormat.ContainerLogsReader
			 logReader, AggregatedLogsBlock.LogLimits logLimits, string desiredLogType, long
			 logUpLoadTime)
		{
			int bufferSize = 65536;
			char[] cbuf = new char[bufferSize];
			bool foundLog = false;
			string logType = logReader.NextLog();
			while (logType != null)
			{
				if (desiredLogType == null || desiredLogType.IsEmpty() || desiredLogType.Equals(logType
					))
				{
					long logLength = logReader.GetCurrentLogLength();
					if (foundLog)
					{
						html.Pre().("\n\n").();
					}
					html.P().("Log Type: " + logType).();
					html.P().("Log Upload Time: " + Times.Format(logUpLoadTime)).();
					html.P().("Log Length: " + System.Convert.ToString(logLength)).();
					long start = logLimits.start < 0 ? logLength + logLimits.start : logLimits.start;
					start = start < 0 ? 0 : start;
					start = start > logLength ? logLength : start;
					long end = logLimits.end < 0 ? logLength + logLimits.end : logLimits.end;
					end = end < 0 ? 0 : end;
					end = end > logLength ? logLength : end;
					end = end < start ? start : end;
					long toRead = end - start;
					if (toRead < logLength)
					{
						html.P().("Showing " + toRead + " bytes of " + logLength + " total. Click ").A(Url
							("logs", $(YarnWebParams.NmNodename), $(YarnWebParams.ContainerId), $(YarnWebParams
							.EntityString), $(YarnWebParams.AppOwner), logType, "?start=0"), "here").(" for the full log."
							).();
					}
					long totalSkipped = 0;
					while (totalSkipped < start)
					{
						long ret = logReader.Skip(start - totalSkipped);
						if (ret == 0)
						{
							//Read one byte
							int nextByte = logReader.Read();
							// Check if we have reached EOF
							if (nextByte == -1)
							{
								throw new IOException("Premature EOF from container log");
							}
							ret = 1;
						}
						totalSkipped += ret;
					}
					int len = 0;
					int currentToRead = toRead > bufferSize ? bufferSize : (int)toRead;
					Hamlet.PRE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> pre = html.Pre();
					while (toRead > 0 && (len = logReader.Read(cbuf, 0, currentToRead)) > 0)
					{
						pre.(new string(cbuf, 0, len));
						toRead = toRead - len;
						currentToRead = toRead > bufferSize ? bufferSize : (int)toRead;
					}
					pre.();
					foundLog = true;
				}
				logType = logReader.NextLog();
			}
			return foundLog;
		}

		private ContainerId VerifyAndGetContainerId(HtmlBlock.Block html)
		{
			string containerIdStr = $(YarnWebParams.ContainerId);
			if (containerIdStr == null || containerIdStr.IsEmpty())
			{
				html.H1().("Cannot get container logs without a ContainerId").();
				return null;
			}
			ContainerId containerId = null;
			try
			{
				containerId = ConverterUtils.ToContainerId(containerIdStr);
			}
			catch (ArgumentException)
			{
				html.H1().("Cannot get container logs for invalid containerId: " + containerIdStr
					).();
				return null;
			}
			return containerId;
		}

		private NodeId VerifyAndGetNodeId(HtmlBlock.Block html)
		{
			string nodeIdStr = $(YarnWebParams.NmNodename);
			if (nodeIdStr == null || nodeIdStr.IsEmpty())
			{
				html.H1().("Cannot get container logs without a NodeId").();
				return null;
			}
			NodeId nodeId = null;
			try
			{
				nodeId = ConverterUtils.ToNodeId(nodeIdStr);
			}
			catch (ArgumentException)
			{
				html.H1().("Cannot get container logs. Invalid nodeId: " + nodeIdStr).();
				return null;
			}
			return nodeId;
		}

		private string VerifyAndGetAppOwner(HtmlBlock.Block html)
		{
			string appOwner = $(YarnWebParams.AppOwner);
			if (appOwner == null || appOwner.IsEmpty())
			{
				html.H1().("Cannot get container logs without an app owner").();
			}
			return appOwner;
		}

		private class LogLimits
		{
			internal long start;

			internal long end;
		}

		private AggregatedLogsBlock.LogLimits VerifyAndGetLogLimits(HtmlBlock.Block html)
		{
			long start = -4096;
			long end = long.MaxValue;
			bool isValid = true;
			string startStr = $("start");
			if (startStr != null && !startStr.IsEmpty())
			{
				try
				{
					start = long.Parse(startStr);
				}
				catch (FormatException)
				{
					isValid = false;
					html.H1().("Invalid log start value: " + startStr).();
				}
			}
			string endStr = $("end");
			if (endStr != null && !endStr.IsEmpty())
			{
				try
				{
					end = long.Parse(endStr);
				}
				catch (FormatException)
				{
					isValid = false;
					html.H1().("Invalid log end value: " + endStr).();
				}
			}
			if (!isValid)
			{
				return null;
			}
			AggregatedLogsBlock.LogLimits limits = new AggregatedLogsBlock.LogLimits();
			limits.start = start;
			limits.end = end;
			return limits;
		}
	}
}
