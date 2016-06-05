using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class ContainerLogsPage : NMView
	{
		public const string RedirectUrl = "redirect.url";

		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			string redirectUrl = $(RedirectUrl);
			if (redirectUrl == null || redirectUrl.IsEmpty())
			{
				Set(Title, StringHelper.Join("Logs for ", $(YarnWebParams.ContainerId)));
			}
			else
			{
				if (redirectUrl.Equals("false"))
				{
					Set(Title, StringHelper.Join("Failed redirect for ", $(YarnWebParams.ContainerId)
						));
				}
				else
				{
					//Error getting redirect url. Fall through.
					Set(Title, StringHelper.Join("Redirecting to log server for ", $(YarnWebParams.ContainerId
						)));
					html.Meta_http("refresh", "1; url=" + redirectUrl);
				}
			}
			Set(JQueryUI.AccordionId, "nav");
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:0}");
		}

		protected override Type Content()
		{
			return typeof(ContainerLogsPage.ContainersLogsBlock);
		}

		public class ContainersLogsBlock : HtmlBlock, YarnWebParams
		{
			private readonly Context nmContext;

			[Com.Google.Inject.Inject]
			public ContainersLogsBlock(Context context)
			{
				this.nmContext = context;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				string redirectUrl = $(RedirectUrl);
				if (redirectUrl != null && redirectUrl.Equals("false"))
				{
					html.H1("Failed while trying to construct the redirect url to the log" + " server. Log Server url may not be configured"
						);
				}
				//Intentional fallthrough.
				ContainerId containerId;
				try
				{
					containerId = ConverterUtils.ToContainerId($(YarnWebParams.ContainerId));
				}
				catch (ArgumentException)
				{
					html.H1("Invalid container ID: " + $(YarnWebParams.ContainerId));
					return;
				}
				try
				{
					if ($(ContainerLogType).IsEmpty())
					{
						IList<FilePath> logFiles = ContainerLogsUtils.GetContainerLogDirs(containerId, Request
							().GetRemoteUser(), nmContext);
						PrintLogFileDirectory(html, logFiles);
					}
					else
					{
						FilePath logFile = ContainerLogsUtils.GetContainerLogFile(containerId, $(ContainerLogType
							), Request().GetRemoteUser(), nmContext);
						PrintLogFile(html, logFile);
					}
				}
				catch (YarnException ex)
				{
					html.H1(ex.Message);
				}
				catch (NotFoundException ex)
				{
					html.H1(ex.Message);
				}
			}

			private void PrintLogFile(HtmlBlock.Block html, FilePath logFile)
			{
				long start = $("start").IsEmpty() ? -4 * 1024 : long.Parse($("start"));
				start = start < 0 ? logFile.Length() + start : start;
				start = start < 0 ? 0 : start;
				long end = $("end").IsEmpty() ? logFile.Length() : long.Parse($("end"));
				end = end < 0 ? logFile.Length() + end : end;
				end = end < 0 ? logFile.Length() : end;
				if (start > end)
				{
					html.H1("Invalid start and end values. Start: [" + start + "]" + ", end[" + end +
						 "]");
					return;
				}
				else
				{
					FileInputStream logByteStream = null;
					try
					{
						logByteStream = ContainerLogsUtils.OpenLogFileForRead($(YarnWebParams.ContainerId
							), logFile, nmContext);
					}
					catch (IOException ex)
					{
						html.H1(ex.Message);
						return;
					}
					try
					{
						long toRead = end - start;
						if (toRead < logFile.Length())
						{
							html.P().("Showing " + toRead + " bytes. Click ").A(Url("containerlogs", $(YarnWebParams
								.ContainerId), $(AppOwner), logFile.GetName(), "?start=0"), "here").(" for full log"
								).();
						}
						IOUtils.SkipFully(logByteStream, start);
						InputStreamReader reader = new InputStreamReader(logByteStream, Sharpen.Extensions.GetEncoding
							("UTF-8"));
						int bufferSize = 65536;
						char[] cbuf = new char[bufferSize];
						int len = 0;
						int currentToRead = toRead > bufferSize ? bufferSize : (int)toRead;
						Hamlet.PRE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> pre = html.Pre();
						while ((len = reader.Read(cbuf, 0, currentToRead)) > 0 && toRead > 0)
						{
							pre.(new string(cbuf, 0, len));
							toRead = toRead - len;
							currentToRead = toRead > bufferSize ? bufferSize : (int)toRead;
						}
						pre.();
						reader.Close();
					}
					catch (IOException e)
					{
						Log.Error("Exception reading log file " + logFile.GetAbsolutePath(), e);
						html.H1("Exception reading log file. It might be because log " + "file was aggregated : "
							 + logFile.GetName());
					}
					finally
					{
						if (logByteStream != null)
						{
							try
							{
								logByteStream.Close();
							}
							catch (IOException)
							{
							}
						}
					}
				}
			}

			// Ignore
			private void PrintLogFileDirectory(HtmlBlock.Block html, IList<FilePath> containerLogsDirs
				)
			{
				// Print out log types in lexical order
				containerLogsDirs.Sort();
				bool foundLogFile = false;
				foreach (FilePath containerLogsDir in containerLogsDirs)
				{
					FilePath[] logFiles = containerLogsDir.ListFiles();
					if (logFiles != null)
					{
						Arrays.Sort(logFiles);
						foreach (FilePath logFile in logFiles)
						{
							foundLogFile = true;
							html.P().A(Url("containerlogs", $(YarnWebParams.ContainerId), $(AppOwner), logFile
								.GetName(), "?start=-4096"), logFile.GetName() + " : Total file length is " + logFile
								.Length() + " bytes.").();
						}
					}
				}
				if (!foundLogFile)
				{
					html.H1("No logs available for container " + $(YarnWebParams.ContainerId));
					return;
				}
			}
		}
	}
}
