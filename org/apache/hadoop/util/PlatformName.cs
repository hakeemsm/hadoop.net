using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>A helper class for getting build-info of the java-vm.</summary>
	public class PlatformName
	{
		/// <summary>
		/// The complete platform 'name' to identify the platform as
		/// per the java-vm.
		/// </summary>
		public static readonly string PLATFORM_NAME = (Sharpen.Runtime.getProperty("os.name"
			).StartsWith("Windows") ? Sharpen.Runtime.getenv("os") : Sharpen.Runtime.getProperty
			("os.name")) + "-" + Sharpen.Runtime.getProperty("os.arch") + "-" + Sharpen.Runtime
			.getProperty("sun.arch.data.model");

		/// <summary>The java vendor name used in this platform.</summary>
		public static readonly string JAVA_VENDOR_NAME = Sharpen.Runtime.getProperty("java.vendor"
			);

		/// <summary>
		/// A public static variable to indicate the current java vendor is
		/// IBM java or not.
		/// </summary>
		public static readonly bool IBM_JAVA = JAVA_VENDOR_NAME.contains("IBM");

		public static void Main(string[] args)
		{
			System.Console.Out.WriteLine(PLATFORM_NAME);
		}
	}
}
