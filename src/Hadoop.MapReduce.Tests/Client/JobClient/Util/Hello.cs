using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>A simple Hello class that is called from TestRunJar</summary>
	public class Hello
	{
		public static void Main(string[] args)
		{
			try
			{
				System.Console.Out.WriteLine("Creating file" + args[0]);
				FileOutputStream fstream = new FileOutputStream(args[0]);
				fstream.Write(Sharpen.Runtime.GetBytesForString("Hello Hadoopers"));
				fstream.Close();
			}
			catch (IOException)
			{
			}
		}
		// do nothing
	}
}
