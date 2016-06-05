using System;
using Javax.WS.RS.Core;
using Javax.WS.RS.Ext;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	public class ExceptionProvider : ExceptionMapper<Exception>
	{
		private static Logger Log = LoggerFactory.GetLogger(typeof(ExceptionProvider));

		private static readonly string Enter = Runtime.GetProperty("line.separator");

		protected internal virtual Response CreateResponse(Response.Status status, Exception
			 throwable)
		{
			return HttpExceptionUtils.CreateJerseyExceptionResponse(status, throwable);
		}

		protected internal virtual string GetOneLineMessage(Exception throwable)
		{
			string message = throwable.Message;
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

		protected internal virtual void Log(Response.Status status, Exception throwable)
		{
			Log.Debug("{}", throwable.Message, throwable);
		}

		public virtual Response ToResponse(Exception throwable)
		{
			return CreateResponse(Response.Status.BadRequest, throwable);
		}
	}
}
