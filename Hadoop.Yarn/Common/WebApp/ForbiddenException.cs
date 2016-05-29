using System;
using Javax.WS.RS;
using Javax.WS.RS.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	[System.Serializable]
	public class ForbiddenException : WebApplicationException
	{
		private const long serialVersionUID = 1L;

		public ForbiddenException()
			: base(Response.Status.Forbidden)
		{
		}

		public ForbiddenException(Exception cause)
			: base(cause, Response.Status.Forbidden)
		{
		}

		public ForbiddenException(string msg)
			: base(new Exception(msg), Response.Status.Forbidden)
		{
		}
	}
}
