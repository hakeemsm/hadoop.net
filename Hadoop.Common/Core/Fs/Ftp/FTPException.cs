using System;


namespace Org.Apache.Hadoop.FS.Ftp
{
	/// <summary>
	/// A class to wrap a
	/// <see cref="System.Exception"/>
	/// into a Runtime Exception.
	/// </summary>
	[System.Serializable]
	public class FTPException : RuntimeException
	{
		private const long serialVersionUID = 1L;

		public FTPException(string message)
			: base(message)
		{
		}

		public FTPException(Exception t)
			: base(t)
		{
		}

		public FTPException(string message, Exception t)
			: base(message, t)
		{
		}
	}
}
