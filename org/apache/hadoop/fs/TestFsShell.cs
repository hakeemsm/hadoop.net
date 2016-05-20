using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestFsShell
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testConfWithInvalidFile()
		{
			string[] args = new string[1];
			args[0] = "--conf=invalidFile";
			System.Exception th = null;
			try
			{
				org.apache.hadoop.fs.FsShell.Main(args);
			}
			catch (System.Exception e)
			{
				th = e;
			}
			if (!(th is System.Exception))
			{
				throw new NUnit.Framework.AssertionFailedError("Expected Runtime exception, got: "
					 + th).initCause(th);
			}
		}
	}
}
