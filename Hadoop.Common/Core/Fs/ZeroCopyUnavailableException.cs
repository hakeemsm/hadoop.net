using System;
using System.IO;


namespace Org.Apache.Hadoop.FS
{
	[System.Serializable]
	public class ZeroCopyUnavailableException : IOException
	{
		private const long serialVersionUID = 0L;

		public ZeroCopyUnavailableException(string message)
			: base(message)
		{
		}

		public ZeroCopyUnavailableException(string message, Exception e)
			: base(message, e)
		{
		}

		public ZeroCopyUnavailableException(Exception e)
			: base(e)
		{
		}
	}
}
