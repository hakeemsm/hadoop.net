using Sharpen;

namespace org.apache.hadoop.fs.ftp
{
	/// <summary>
	/// A class to wrap a
	/// <see cref="System.Exception"/>
	/// into a Runtime Exception.
	/// </summary>
	[System.Serializable]
	public class FTPException : System.Exception
	{
		private const long serialVersionUID = 1L;

		public FTPException(string message)
			: base(message)
		{
		}

		public FTPException(System.Exception t)
			: base(t)
		{
		}

		public FTPException(string message, System.Exception t)
			: base(message, t)
		{
		}
	}
}
