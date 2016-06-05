using System.Text;


namespace Org.Apache.Hadoop.FS.Shell
{
	internal sealed class CommandUtils
	{
		internal static string FormatDescription(string usage, params string[] desciptions
			)
		{
			StringBuilder b = new StringBuilder(usage + ": " + desciptions[0]);
			for (int i = 1; i < desciptions.Length; i++)
			{
				b.Append("\n\t\t" + desciptions[i]);
			}
			return b.ToString();
		}
	}
}
