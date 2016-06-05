using System;
using Javax.WS.RS;
using Javax.WS.RS.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	[System.Serializable]
	public class BadRequestException : WebApplicationException
	{
		private const long serialVersionUID = 1L;

		public BadRequestException()
			: base(Response.Status.BadRequest)
		{
		}

		public BadRequestException(Exception cause)
			: base(cause, Response.Status.BadRequest)
		{
		}

		public BadRequestException(string msg)
			: base(new Exception(msg), Response.Status.BadRequest)
		{
		}
	}
}
