using System.IO;
using NUnit.Framework;


namespace Org.Apache.Hadoop.Util
{
	public class TestNativeLibraryChecker : TestCase
	{
		private void ExpectExit(string[] args)
		{
			try
			{
				// should throw exit exception
				NativeLibraryChecker.Main(args);
				Fail("should call exit");
			}
			catch (ExitUtil.ExitException)
			{
				// pass
				ExitUtil.ResetFirstExitException();
			}
		}

		[Fact]
		public virtual void TestNativeLibraryChecker()
		{
			ExitUtil.DisableSystemExit();
			// help should return normally
			NativeLibraryChecker.Main(new string[] { "-h" });
			// illegal argmuments should exit
			ExpectExit(new string[] { "-a", "-h" });
			ExpectExit(new string[] { "aaa" });
			if (NativeCodeLoader.IsNativeCodeLoaded())
			{
				// no argument should return normally
				NativeLibraryChecker.Main(new string[0]);
			}
			else
			{
				// no argument should exit
				ExpectExit(new string[0]);
			}
		}

		[Fact]
		public virtual void TestNativeLibraryCheckerOutput()
		{
			ExpectOutput(new string[] { "-a" });
			// no argument
			ExpectOutput(new string[0]);
		}

		private void ExpectOutput(string[] args)
		{
			ExitUtil.DisableSystemExit();
			ByteArrayOutputStream outContent = new ByteArrayOutputStream();
			TextWriter originalPs = System.Console.Out;
			Runtime.SetOut(new TextWriter(outContent));
			try
			{
				NativeLibraryChecker.Main(args);
			}
			catch (ExitUtil.ExitException)
			{
				ExitUtil.ResetFirstExitException();
			}
			finally
			{
				if (Shell.Windows)
				{
					Assert.Equal(outContent.ToString().IndexOf("winutils: true") !=
						 -1, true);
				}
				if (NativeCodeLoader.IsNativeCodeLoaded())
				{
					Assert.Equal(outContent.ToString().IndexOf("hadoop:  true") !=
						 -1, true);
				}
				Runtime.SetOut(originalPs);
			}
		}
	}
}
