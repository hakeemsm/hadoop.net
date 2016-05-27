using System;
using System.IO;
using System.Security;
using Com.Sun.Jersey.Api.Container;
using Javax.WS.RS.Core;
using Javax.WS.RS.Ext;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	/// <summary>Jersey provider that converts KMS exceptions into detailed HTTP errors.</summary>
	public class KMSExceptionsProvider : ExceptionMapper<Exception>
	{
		private static Logger Log = LoggerFactory.GetLogger(typeof(KMSExceptionsProvider)
			);

		private static readonly string Enter = Runtime.GetProperty("line.separator");

		protected internal virtual Response CreateResponse(Response.Status status, Exception
			 ex)
		{
			return HttpExceptionUtils.CreateJerseyExceptionResponse(status, ex);
		}

		protected internal virtual string GetOneLineMessage(Exception exception)
		{
			string message = exception.Message;
			if (message != null)
			{
				int i = message.IndexOf(Enter);
				if (i > -1)
				{
					message = Sharpen.Runtime.Substring(message, 0, i);
				}
			}
			return message;
		}

		/// <summary>Maps different exceptions thrown by KMS to HTTP status codes.</summary>
		public virtual Response ToResponse(Exception exception)
		{
			Response.Status status;
			bool doAudit = true;
			Exception throwable = exception;
			if (exception is ContainerException)
			{
				throwable = exception.InnerException;
			}
			if (throwable is SecurityException)
			{
				status = Response.Status.Forbidden;
			}
			else
			{
				if (throwable is AuthenticationException)
				{
					status = Response.Status.Forbidden;
					// we don't audit here because we did it already when checking access
					doAudit = false;
				}
				else
				{
					if (throwable is AuthorizationException)
					{
						status = Response.Status.Forbidden;
						// we don't audit here because we did it already when checking access
						doAudit = false;
					}
					else
					{
						if (throwable is AccessControlException)
						{
							status = Response.Status.Forbidden;
						}
						else
						{
							if (exception is IOException)
							{
								status = Response.Status.InternalServerError;
							}
							else
							{
								if (exception is NotSupportedException)
								{
									status = Response.Status.BadRequest;
								}
								else
								{
									if (exception is ArgumentException)
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
				}
			}
			if (doAudit)
			{
				KMSWebApp.GetKMSAudit().Error(KMSMDCFilter.GetUgi(), KMSMDCFilter.GetMethod(), KMSMDCFilter
					.GetURL(), GetOneLineMessage(exception));
			}
			return CreateResponse(status, throwable);
		}

		protected internal virtual void Log(Response.Status status, Exception ex)
		{
			UserGroupInformation ugi = KMSMDCFilter.GetUgi();
			string method = KMSMDCFilter.GetMethod();
			string url = KMSMDCFilter.GetURL();
			string msg = GetOneLineMessage(ex);
			Log.Warn("User:'{}' Method:{} URL:{} Response:{}-{}", ugi, method, url, status, msg
				, ex);
		}
	}
}
