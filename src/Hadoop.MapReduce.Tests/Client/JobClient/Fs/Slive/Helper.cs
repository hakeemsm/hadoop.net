using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>Simple slive helper methods (may not exist in 0.20)</summary>
	internal class Helper
	{
		private Helper()
		{
		}

		private static readonly string[] emptyStringArray = new string[] {  };

		/// <summary>Splits strings on comma and trims accordingly</summary>
		/// <param name="str"/>
		/// <returns>array of split</returns>
		internal static string[] GetTrimmedStrings(string str)
		{
			if (null == str || string.Empty.Equals(str.Trim()))
			{
				return emptyStringArray;
			}
			return str.Trim().Split("\\s*,\\s*");
		}

		/// <summary>Converts a byte value into a useful string for output</summary>
		/// <param name="bytes"/>
		/// <returns>String</returns>
		internal static string ToByteInfo(long bytes)
		{
			StringBuilder str = new StringBuilder();
			if (bytes < 0)
			{
				bytes = 0;
			}
			str.Append(bytes);
			str.Append(" bytes or ");
			str.Append(bytes / 1024);
			str.Append(" kilobytes or ");
			str.Append(bytes / (1024 * 1024));
			str.Append(" megabytes or ");
			str.Append(bytes / (1024 * 1024 * 1024));
			str.Append(" gigabytes");
			return str.ToString();
		}

		/// <summary>Stringifys an array using the given separator.</summary>
		/// <param name="args">the array to format</param>
		/// <param name="sep">the separator string to use (ie comma or space)</param>
		/// <returns>String representing that array</returns>
		internal static string StringifyArray(object[] args, string sep)
		{
			StringBuilder optStr = new StringBuilder();
			for (int i = 0; i < args.Length; ++i)
			{
				optStr.Append(args[i]);
				if ((i + 1) != args.Length)
				{
					optStr.Append(sep);
				}
			}
			return optStr.ToString();
		}
	}
}
