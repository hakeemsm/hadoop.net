using System;
using System.IO;
using System.Security;
using Com.Google.Common.Annotations;
using Com.Sun.Jersey.Api;
using Com.Sun.Jersey.Api.Container;
using Javax.Servlet.Http;
using Javax.WS.RS.Core;
using Javax.WS.RS.Ext;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Handle exceptions.</summary>
	public class ExceptionHandler : ExceptionMapper<Exception>
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(ExceptionHandler));

		private static Exception ToCause(Exception e)
		{
			Exception t = e.InnerException;
			if (e is SecurityException)
			{
				// For the issue reported in HDFS-6475, if SecurityException's cause
				// is InvalidToken, and the InvalidToken's cause is StandbyException,
				// return StandbyException; Otherwise, leave the exception as is,
				// since they are handled elsewhere. See HDFS-6588.
				if (t != null && t is SecretManager.InvalidToken)
				{
					Exception t1 = t.InnerException;
					if (t1 != null && t1 is StandbyException)
					{
						e = (StandbyException)t1;
					}
				}
			}
			else
			{
				if (t != null && t is Exception)
				{
					e = (Exception)t;
				}
			}
			return e;
		}

		[Context]
		private HttpServletResponse response;

		public virtual Response ToResponse(Exception e)
		{
			if (Log.IsTraceEnabled())
			{
				Log.Trace("GOT EXCEPITION", e);
			}
			//clear content type
			response.SetContentType(null);
			//Convert exception
			if (e is ParamException)
			{
				ParamException paramexception = (ParamException)e;
				e = new ArgumentException("Invalid value for webhdfs parameter \"" + paramexception
					.GetParameterName() + "\": " + e.InnerException.Message, e);
			}
			if (e is ContainerException)
			{
				e = ToCause(e);
			}
			if (e is RemoteException)
			{
				e = ((RemoteException)e).UnwrapRemoteException();
			}
			if (e is SecurityException)
			{
				e = ToCause(e);
			}
			//Map response status
			Response.Status s;
			if (e is SecurityException)
			{
				s = Response.Status.Forbidden;
			}
			else
			{
				if (e is AuthorizationException)
				{
					s = Response.Status.Forbidden;
				}
				else
				{
					if (e is FileNotFoundException)
					{
						s = Response.Status.NotFound;
					}
					else
					{
						if (e is IOException)
						{
							s = Response.Status.Forbidden;
						}
						else
						{
							if (e is NotSupportedException)
							{
								s = Response.Status.BadRequest;
							}
							else
							{
								if (e is ArgumentException)
								{
									s = Response.Status.BadRequest;
								}
								else
								{
									Log.Warn("INTERNAL_SERVER_ERROR", e);
									s = Response.Status.InternalServerError;
								}
							}
						}
					}
				}
			}
			string js = JsonUtil.ToJsonString(e);
			return Response.Status(s).Type(MediaType.ApplicationJson).Entity(js).Build();
		}

		[VisibleForTesting]
		public virtual void InitResponse(HttpServletResponse response)
		{
			this.response = response;
		}
	}
}
