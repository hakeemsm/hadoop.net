using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Thrown for unexpected filesystem errors, presumed to reflect disk errors
	/// in the native filesystem.
	/// </summary>
	[System.Serializable]
	public class FSError : System.Exception
	{
		private const long serialVersionUID = 1L;

		internal FSError(System.Exception cause)
			: base(cause)
		{
		}
	}
}
