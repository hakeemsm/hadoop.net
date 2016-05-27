using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// Thrown when the user makes a malformed request, for example missing required
	/// parameters or parameters that are not valid.
	/// </summary>
	[System.Serializable]
	public class InvalidRequestException : IOException
	{
		internal const long serialVersionUID = 0L;

		public InvalidRequestException(string str)
			: base(str)
		{
		}
	}
}
