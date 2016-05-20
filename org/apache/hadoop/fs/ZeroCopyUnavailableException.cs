using Sharpen;

namespace org.apache.hadoop.fs
{
	[System.Serializable]
	public class ZeroCopyUnavailableException : System.IO.IOException
	{
		private const long serialVersionUID = 0L;

		public ZeroCopyUnavailableException(string message)
			: base(message)
		{
		}

		public ZeroCopyUnavailableException(string message, System.Exception e)
			: base(message, e)
		{
		}

		public ZeroCopyUnavailableException(System.Exception e)
			: base(e)
		{
		}
	}
}
