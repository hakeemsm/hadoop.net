using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Thrown when the user makes a malformed request, for example missing required
	/// parameters or parameters that are not valid.
	/// </summary>
	[System.Serializable]
	public class InvalidRequestException : System.IO.IOException
	{
		internal const long serialVersionUID = 0L;

		public InvalidRequestException(string str)
			: base(str)
		{
		}
	}
}
