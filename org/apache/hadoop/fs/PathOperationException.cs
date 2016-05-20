using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>ENOTSUP</summary>
	[System.Serializable]
	public class PathOperationException : org.apache.hadoop.fs.PathExistsException
	{
		internal const long serialVersionUID = 0L;

		/// <param name="path">for the exception</param>
		public PathOperationException(string path)
			: base(path, "Operation not supported")
		{
		}
	}
}
