using Sharpen;
using Sharpen.Jar;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>A micro-application that prints the main class name out of a jar file.</summary>
	public class PrintJarMainClass
	{
		/// <param name="args"/>
		public static void Main(string[] args)
		{
			try
			{
				using (JarFile jar_file = new JarFile(args[0]))
				{
					Manifest manifest = jar_file.GetManifest();
					if (manifest != null)
					{
						string value = manifest.GetMainAttributes().GetValue("Main-Class");
						if (value != null)
						{
							System.Console.Out.WriteLine(value.ReplaceAll("/", "."));
							return;
						}
					}
				}
			}
			catch
			{
			}
			// ignore it
			System.Console.Out.WriteLine("UNKNOWN");
			System.Environment.Exit(1);
		}
	}
}
