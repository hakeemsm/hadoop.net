using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>A helper class for getting build-info of the java-vm.</summary>
	public class PlatformName
	{
		/// <summary>
		/// The complete platform 'name' to identify the platform as
		/// per the java-vm.
		/// </summary>
		public static readonly string PlatformName = (Runtime.GetProperty("os.name").StartsWith
			("Windows") ? Runtime.Getenv("os") : Runtime.GetProperty("os.name")) + "-" + Runtime
			.GetProperty("os.arch") + "-" + Runtime.GetProperty("sun.arch.data.model");

		/// <summary>The java vendor name used in this platform.</summary>
		public static readonly string JavaVendorName = Runtime.GetProperty("java.vendor");

		/// <summary>
		/// A public static variable to indicate the current java vendor is
		/// IBM java or not.
		/// </summary>
		public static readonly bool IbmJava = JavaVendorName.Contains("IBM");

		public static void Main(string[] args)
		{
			System.Console.Out.WriteLine(PlatformName);
		}
	}
}
