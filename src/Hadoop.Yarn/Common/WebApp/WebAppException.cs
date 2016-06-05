using System;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	[System.Serializable]
	public class WebAppException : YarnRuntimeException
	{
		private const long serialVersionUID = 1L;

		public WebAppException(string msg)
			: base(msg)
		{
		}

		public WebAppException(Exception cause)
			: base(cause)
		{
		}

		public WebAppException(string msg, Exception cause)
			: base(msg, cause)
		{
		}
	}
}
