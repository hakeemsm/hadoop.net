using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	internal sealed class CommandUtils
	{
		internal static string formatDescription(string usage, params string[] desciptions
			)
		{
			java.lang.StringBuilder b = new java.lang.StringBuilder(usage + ": " + desciptions
				[0]);
			for (int i = 1; i < desciptions.Length; i++)
			{
				b.Append("\n\t\t" + desciptions[i]);
			}
			return b.ToString();
		}
	}
}
