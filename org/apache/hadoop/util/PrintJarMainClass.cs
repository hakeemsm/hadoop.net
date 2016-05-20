using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>A micro-application that prints the main class name out of a jar file.</summary>
	public class PrintJarMainClass
	{
		/// <param name="args"/>
		public static void Main(string[] args)
		{
			try
			{
				using (java.util.jar.JarFile jar_file = new java.util.jar.JarFile(args[0]))
				{
					java.util.jar.Manifest manifest = jar_file.getManifest();
					if (manifest != null)
					{
						string value = manifest.getMainAttributes().getValue("Main-Class");
						if (value != null)
						{
							System.Console.Out.WriteLine(value.replaceAll("/", "."));
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
