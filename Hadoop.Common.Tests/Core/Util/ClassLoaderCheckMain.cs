using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// Test class used by
	/// <see cref="TestRunJar"/>
	/// to verify that it is loaded by the
	/// <see cref="ApplicationClassLoader"/>
	/// .
	/// </summary>
	public class ClassLoaderCheckMain
	{
		public static void Main(string[] args)
		{
			// ClassLoaderCheckMain should be loaded by the application classloader
			ClassLoaderCheck.CheckClassLoader(typeof(ClassLoaderCheckMain), true);
			// ClassLoaderCheckSecond should NOT be loaded by the application
			// classloader
			ClassLoaderCheck.CheckClassLoader(typeof(ClassLoaderCheckSecond), false);
			// ClassLoaderCheckThird should be loaded by the application classloader
			ClassLoaderCheck.CheckClassLoader(typeof(ClassLoaderCheckThird), true);
		}
	}
}
