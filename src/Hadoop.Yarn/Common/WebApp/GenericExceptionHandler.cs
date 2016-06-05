using System;
using System.IO;
using System.Security;
using Com.Sun.Jersey.Api;
using Javax.Servlet.Http;
using Javax.WS.RS;
using Javax.WS.RS.Core;
using Javax.WS.RS.Ext;
using Javax.Xml.Bind;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	/// <summary>
	/// Handle webservices jersey exceptions and create json or xml response
	/// with the ExceptionData.
	/// </summary>
	public class GenericExceptionHandler : ExceptionMapper<Exception>
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(GenericExceptionHandler
			));

		[Context]
		private HttpServletResponse response;

		public virtual Response ToResponse(Exception e)
		{
			if (Log.IsTraceEnabled())
			{
				Log.Trace("GOT EXCEPITION", e);
			}
			// Don't catch this as filter forward on 404
			// (ServletContainer.FEATURE_FILTER_FORWARD_ON_404)
			// won't work and the web UI won't work!
			if (e is NotFoundException)
			{
				return ((NotFoundException)e).GetResponse();
			}
			// clear content type
			response.SetContentType(null);
			// Convert exception
			if (e is RemoteException)
			{
				e = ((RemoteException)e).UnwrapRemoteException();
			}
			// Map response status
			Response.Status s;
			if (e is SecurityException)
			{
				s = Response.Status.Unauthorized;
			}
			else
			{
				if (e is AuthorizationException)
				{
					s = Response.Status.Unauthorized;
				}
				else
				{
					if (e is FileNotFoundException)
					{
						s = Response.Status.NotFound;
					}
					else
					{
						if (e is NotFoundException)
						{
							s = Response.Status.NotFound;
						}
						else
						{
							if (e is IOException)
							{
								s = Response.Status.NotFound;
							}
							else
							{
								if (e is ForbiddenException)
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
											if (e is FormatException)
											{
												s = Response.Status.BadRequest;
											}
											else
											{
												if (e is BadRequestException)
												{
													s = Response.Status.BadRequest;
												}
												else
												{
													if (e is WebApplicationException && e.InnerException is UnmarshalException)
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
							}
						}
					}
				}
			}
			// let jaxb handle marshalling data out in the same format requested
			RemoteExceptionData exception = new RemoteExceptionData(e.GetType().Name, e.Message
				, e.GetType().FullName);
			return Response.Status(s).Entity(exception).Build();
		}
	}
}
