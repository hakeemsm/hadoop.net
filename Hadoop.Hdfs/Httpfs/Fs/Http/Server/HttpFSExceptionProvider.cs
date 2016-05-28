using System;
using System.IO;
using System.Security;
using Com.Sun.Jersey.Api.Container;
using Javax.WS.RS.Core;
using Org.Apache.Hadoop.Lib.Service;
using Org.Apache.Hadoop.Lib.Wsrs;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Server
{
	/// <summary>
	/// JAX-RS <code>ExceptionMapper</code> implementation that maps HttpFSServer's
	/// exceptions to HTTP status codes.
	/// </summary>
	public class HttpFSExceptionProvider : ExceptionProvider
	{
		private static Logger AuditLog = LoggerFactory.GetLogger("httpfsaudit");

		private static Logger Log = LoggerFactory.GetLogger(typeof(HttpFSExceptionProvider
			));

		/// <summary>Maps different exceptions thrown by HttpFSServer to HTTP status codes.</summary>
		/// <remarks>
		/// Maps different exceptions thrown by HttpFSServer to HTTP status codes.
		/// <ul>
		/// <li>SecurityException : HTTP UNAUTHORIZED</li>
		/// <li>FileNotFoundException : HTTP NOT_FOUND</li>
		/// <li>IOException : INTERNAL_HTTP SERVER_ERROR</li>
		/// <li>UnsupporteOperationException : HTTP BAD_REQUEST</li>
		/// <li>all other exceptions : HTTP INTERNAL_SERVER_ERROR </li>
		/// </ul>
		/// </remarks>
		/// <param name="throwable">exception thrown.</param>
		/// <returns>mapped HTTP status code</returns>
		public override Response ToResponse(Exception throwable)
		{
			Response.Status status;
			if (throwable is FileSystemAccessException)
			{
				throwable = throwable.InnerException;
			}
			if (throwable is ContainerException)
			{
				throwable = throwable.InnerException;
			}
			if (throwable is SecurityException)
			{
				status = Response.Status.Unauthorized;
			}
			else
			{
				if (throwable is FileNotFoundException)
				{
					status = Response.Status.NotFound;
				}
				else
				{
					if (throwable is IOException)
					{
						status = Response.Status.InternalServerError;
					}
					else
					{
						if (throwable is NotSupportedException)
						{
							status = Response.Status.BadRequest;
						}
						else
						{
							if (throwable is ArgumentException)
							{
								status = Response.Status.BadRequest;
							}
							else
							{
								status = Response.Status.InternalServerError;
							}
						}
					}
				}
			}
			return CreateResponse(status, throwable);
		}

		/// <summary>Logs the HTTP status code and exception in HttpFSServer's log.</summary>
		/// <param name="status">HTTP status code.</param>
		/// <param name="throwable">exception thrown.</param>
		protected internal override void Log(Response.Status status, Exception throwable)
		{
			string method = MDC.Get("method");
			string path = MDC.Get("path");
			string message = GetOneLineMessage(throwable);
			AuditLog.Warn("FAILED [{}:{}] response [{}] {}", new object[] { method, path, status
				, message });
			Log.Warn("[{}:{}] response [{}] {}", new object[] { method, path, status, message
				 }, throwable);
		}
	}
}
