using System;
using NUnit.Framework;


namespace Org.Apache.Hadoop.FS
{
	public class TestFsShell
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestConfWithInvalidFile()
		{
			string[] args = new string[1];
			args[0] = "--conf=invalidFile";
			Exception th = null;
			try
			{
				FsShell.Main(args);
			}
			catch (Exception e)
			{
				th = e;
			}
			if (!(th is RuntimeException))
			{
				throw Extensions.InitCause(new AssertionFailedError("Expected Runtime exception, got: "
					 + th), th);
			}
		}
	}
}
