using System;
using Javax.WS.RS;
using Javax.WS.RS.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	[System.Serializable]
	public class NotFoundException : WebApplicationException
	{
		private const long serialVersionUID = 1L;

		public NotFoundException()
			: base(Response.Status.NotFound)
		{
		}

		public NotFoundException(Exception cause)
			: base(cause, Response.Status.NotFound)
		{
		}

		public NotFoundException(string msg)
			: base(new Exception(msg), Response.Status.NotFound)
		{
		}
		/*
		* Created our own NotFoundException because com.sun.jersey.api.NotFoundException
		* sets the Response and therefore won't be handled by the GenericExceptionhandler
		* to fill in correct response.
		*/
	}
}
