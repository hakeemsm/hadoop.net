using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Exception corresponding to Permission denied - ENOENT</summary>
	[System.Serializable]
	public class PathNotFoundException : org.apache.hadoop.fs.PathIOException
	{
		internal const long serialVersionUID = 0L;

		/// <param name="path">for the exception</param>
		public PathNotFoundException(string path)
			: base(path, "No such file or directory")
		{
		}
	}
}
