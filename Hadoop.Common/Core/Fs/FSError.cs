using System;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// Thrown for unexpected filesystem errors, presumed to reflect disk errors
	/// in the native filesystem.
	/// </summary>
	[System.Serializable]
	public class FSError : Error
	{
		private const long serialVersionUID = 1L;

		internal FSError(Exception cause)
			: base(cause)
		{
		}
	}
}
