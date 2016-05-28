using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This exception is thrown when the name node is in safe mode.</summary>
	/// <remarks>
	/// This exception is thrown when the name node is in safe mode.
	/// Client cannot modified namespace until the safe mode is off.
	/// </remarks>
	[System.Serializable]
	public class SafeModeException : IOException
	{
		private const long serialVersionUID = 1L;

		public SafeModeException(string text, FSNamesystem.SafeModeInfo mode)
			: base(text + ". Name node is in safe mode.\n" + mode.GetTurnOffTip())
		{
		}
	}
}
