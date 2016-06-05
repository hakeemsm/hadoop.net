

namespace Org.Apache.Hadoop.FS
{
	/// <summary>ENOTSUP</summary>
	[System.Serializable]
	public class PathOperationException : PathExistsException
	{
		internal const long serialVersionUID = 0L;

		/// <param name="path">for the exception</param>
		public PathOperationException(string path)
			: base(path, "Operation not supported")
		{
		}
	}
}
