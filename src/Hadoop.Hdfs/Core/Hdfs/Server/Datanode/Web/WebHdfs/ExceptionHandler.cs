using System;
using System.IO;
using System.Security;
using Com.Google.Common.Base;
using Com.Sun.Jersey.Api;
using Com.Sun.Jersey.Api.Container;
using IO.Netty.Buffer;
using IO.Netty.Handler.Codec.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Web.Webhdfs
{
	internal class ExceptionHandler
	{
		internal static Log Log = WebHdfsHandler.Log;

		internal static DefaultFullHttpResponse ExceptionCaught(Exception cause)
		{
			Exception e = cause is Exception ? (Exception)cause : new Exception(cause);
			if (Log.IsTraceEnabled())
			{
				Log.Trace("GOT EXCEPITION", e);
			}
			//Convert exception
			if (e is ParamException)
			{
				ParamException paramexception = (ParamException)e;
				e = new ArgumentException("Invalid value for webhdfs parameter \"" + paramexception
					.GetParameterName() + "\": " + e.InnerException.Message, e);
			}
			else
			{
				if (e is ContainerException || e is SecurityException)
				{
					e = ToCause(e);
				}
				else
				{
					if (e is RemoteException)
					{
						e = ((RemoteException)e).UnwrapRemoteException();
					}
				}
			}
			//Map response status
			HttpResponseStatus s;
			if (e is SecurityException)
			{
				s = HttpResponseStatus.Forbidden;
			}
			else
			{
				if (e is AuthorizationException)
				{
					s = HttpResponseStatus.Forbidden;
				}
				else
				{
					if (e is FileNotFoundException)
					{
						s = HttpResponseStatus.NotFound;
					}
					else
					{
						if (e is IOException)
						{
							s = HttpResponseStatus.Forbidden;
						}
						else
						{
							if (e is NotSupportedException)
							{
								s = HttpResponseStatus.BadRequest;
							}
							else
							{
								if (e is ArgumentException)
								{
									s = HttpResponseStatus.BadRequest;
								}
								else
								{
									Log.Warn("INTERNAL_SERVER_ERROR", e);
									s = HttpResponseStatus.InternalServerError;
								}
							}
						}
					}
				}
			}
			byte[] js = Sharpen.Runtime.GetBytesForString(JsonUtil.ToJsonString(e), Charsets.
				Utf8);
			DefaultFullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.Http11, s, 
				Unpooled.WrappedBuffer(js));
			resp.Headers().Set(HttpHeaders.Names.ContentType, WebHdfsHandler.ApplicationJsonUtf8
				);
			resp.Headers().Set(HttpHeaders.Names.ContentLength, js.Length);
			return resp;
		}

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
	}
}
