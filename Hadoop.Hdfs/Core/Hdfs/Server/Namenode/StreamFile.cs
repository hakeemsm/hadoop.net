using System;
using System.Collections.Generic;
using System.IO;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Mortbay.Jetty;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	[System.Serializable]
	public class StreamFile : DfsServlet
	{
		/// <summary>for java.io.Serializable</summary>
		private const long serialVersionUID = 1L;

		public const string ContentLength = "Content-Length";

		/* Return a DFS client to use to make the given HTTP request */
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual DFSClient GetDFSClient(HttpServletRequest request)
		{
			Configuration conf = (Configuration)GetServletContext().GetAttribute(JspHelper.CurrentConf
				);
			UserGroupInformation ugi = GetUGI(request, conf);
			ServletContext context = GetServletContext();
			DataNode datanode = (DataNode)context.GetAttribute("datanode");
			return DatanodeJspHelper.GetDFSClient(request, datanode, conf, ugi);
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoGet(HttpServletRequest request, HttpServletResponse response
			)
		{
			string path = ServletUtil.GetDecodedPath(request, "/streamFile");
			string rawPath = ServletUtil.GetRawPath(request, "/streamFile");
			string filename = JspHelper.ValidatePath(path);
			string rawFilename = JspHelper.ValidatePath(rawPath);
			if (filename == null)
			{
				response.SetContentType("text/plain");
				PrintWriter @out = response.GetWriter();
				@out.Write("Invalid input");
				return;
			}
			Enumeration<string> reqRanges = request.GetHeaders("Range");
			if (reqRanges != null && !reqRanges.MoveNext())
			{
				reqRanges = null;
			}
			DFSClient dfs;
			try
			{
				dfs = GetDFSClient(request);
			}
			catch (Exception e)
			{
				response.SendError(400, e.Message);
				return;
			}
			HdfsDataInputStream @in = null;
			OutputStream out_1 = null;
			try
			{
				@in = dfs.CreateWrappedInputStream(dfs.Open(filename));
				out_1 = response.GetOutputStream();
				long fileLen = @in.GetVisibleLength();
				if (reqRanges != null)
				{
					IList<InclusiveByteRange> ranges = InclusiveByteRange.SatisfiableRanges(reqRanges
						, fileLen);
					StreamFile.SendPartialData(@in, out_1, response, fileLen, ranges);
				}
				else
				{
					// No ranges, so send entire file
					response.SetHeader("Content-Disposition", "attachment; filename=\"" + rawFilename
						 + "\"");
					response.SetContentType("application/octet-stream");
					response.SetHeader(ContentLength, string.Empty + fileLen);
					StreamFile.CopyFromOffset(@in, out_1, 0L, fileLen);
				}
				@in.Close();
				@in = null;
				out_1.Close();
				out_1 = null;
				dfs.Close();
				dfs = null;
			}
			catch (IOException ioe)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("response.isCommitted()=" + response.IsCommitted(), ioe);
				}
				throw;
			}
			finally
			{
				IOUtils.Cleanup(Log, @in);
				IOUtils.Cleanup(Log, out_1);
				IOUtils.Cleanup(Log, dfs);
			}
		}

		/// <summary>Send a partial content response with the given range.</summary>
		/// <remarks>
		/// Send a partial content response with the given range. If there are
		/// no satisfiable ranges, or if multiple ranges are requested, which
		/// is unsupported, respond with range not satisfiable.
		/// </remarks>
		/// <param name="in">stream to read from</param>
		/// <param name="out">stream to write to</param>
		/// <param name="response">http response to use</param>
		/// <param name="contentLength">for the response header</param>
		/// <param name="ranges">to write to respond with</param>
		/// <exception cref="System.IO.IOException">on error sending the response</exception>
		internal static void SendPartialData(FSDataInputStream @in, OutputStream @out, HttpServletResponse
			 response, long contentLength, IList<InclusiveByteRange> ranges)
		{
			if (ranges == null || ranges.Count != 1)
			{
				response.SetContentLength(0);
				response.SetStatus(HttpServletResponse.ScRequestedRangeNotSatisfiable);
				response.SetHeader("Content-Range", InclusiveByteRange.To416HeaderRangeString(contentLength
					));
			}
			else
			{
				InclusiveByteRange singleSatisfiableRange = ranges[0];
				long singleLength = singleSatisfiableRange.GetSize(contentLength);
				response.SetStatus(HttpServletResponse.ScPartialContent);
				response.SetHeader("Content-Range", singleSatisfiableRange.ToHeaderRangeString(contentLength
					));
				CopyFromOffset(@in, @out, singleSatisfiableRange.GetFirst(contentLength), singleLength
					);
			}
		}

		/* Copy count bytes at the given offset from one stream to another */
		/// <exception cref="System.IO.IOException"/>
		internal static void CopyFromOffset(FSDataInputStream @in, OutputStream @out, long
			 offset, long count)
		{
			@in.Seek(offset);
			IOUtils.CopyBytes(@in, @out, count, false);
		}
	}
}
