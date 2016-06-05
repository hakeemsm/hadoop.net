using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;

using Management;

namespace Org.Apache.Hadoop.Util
{
	public class TestShell : TestCase
	{
		private class Command : Shell
		{
			private int runCount = 0;

			private Command(long interval)
				: base(interval)
			{
			}

			protected internal override string[] GetExecString()
			{
				// There is no /bin/echo equivalent on Windows so just launch it as a
				// shell built-in.
				//
				return Shell.Windows ? (new string[] { "cmd.exe", "/c", "echo", "hello" }) : (new 
					string[] { "echo", "hello" });
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ParseExecResult(BufferedReader lines)
			{
				++runCount;
			}

			public virtual int GetRunCount()
			{
				return runCount;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInterval()
		{
			TestInterval(long.MinValue / 60000);
			// test a negative interval
			TestInterval(0L);
			// test a zero interval
			TestInterval(10L);
			// interval equal to 10mins
			TestInterval(Time.Now() / 60000 + 60);
		}

		// test a very big interval
		/// <summary>Assert that a string has a substring in it</summary>
		/// <param name="string">string to search</param>
		/// <param name="search">what to search for it</param>
		private void AssertInString(string @string, string search)
		{
			NUnit.Framework.Assert.IsNotNull("Empty String", @string);
			if (!@string.Contains(search))
			{
				Fail("Did not find \"" + search + "\" in " + @string);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestShellCommandExecutorToString()
		{
			Shell.ShellCommandExecutor sce = new Shell.ShellCommandExecutor(new string[] { "ls"
				, "..", "arg 2" });
			string command = sce.ToString();
			AssertInString(command, "ls");
			AssertInString(command, " .. ");
			AssertInString(command, "\"arg 2\"");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestShellCommandTimeout()
		{
			if (Shell.Windows)
			{
				// setExecutable does not work on Windows
				return;
			}
			string rootDir = new FilePath(Runtime.GetProperty("test.build.data", "/tmp")).GetAbsolutePath
				();
			FilePath shellFile = new FilePath(rootDir, "timeout.sh");
			string timeoutCommand = "sleep 4; echo \"hello\"";
			PrintWriter writer = new PrintWriter(new FileOutputStream(shellFile));
			writer.WriteLine(timeoutCommand);
			writer.Close();
			FileUtil.SetExecutable(shellFile, true);
			Shell.ShellCommandExecutor shexc = new Shell.ShellCommandExecutor(new string[] { 
				shellFile.GetAbsolutePath() }, null, null, 100);
			try
			{
				shexc.Execute();
			}
			catch (Exception)
			{
			}
			//When timing out exception is thrown.
			shellFile.Delete();
			Assert.True("Script didnt not timeout", shexc.IsTimedOut());
		}

		private static int CountTimerThreads()
		{
			ThreadMXBean threadBean = ManagementFactory.GetThreadMXBean();
			int count = 0;
			ThreadInfo[] infos = threadBean.GetThreadInfo(threadBean.GetAllThreadIds(), 20);
			foreach (ThreadInfo info in infos)
			{
				if (info == null)
				{
					continue;
				}
				foreach (StackTraceElement elem in info.GetStackTrace())
				{
					if (elem.GetClassName().Contains("Timer"))
					{
						count++;
						break;
					}
				}
			}
			return count;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestShellCommandTimerLeak()
		{
			string[] quickCommand = new string[] { "/bin/sleep", "100" };
			int timersBefore = CountTimerThreads();
			System.Console.Error.WriteLine("before: " + timersBefore);
			for (int i = 0; i < 10; i++)
			{
				Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(quickCommand, 
					null, null, 1);
				try
				{
					shexec.Execute();
					Fail("Bad command should throw exception");
				}
				catch (Exception)
				{
				}
			}
			// expected
			Thread.Sleep(1000);
			int timersAfter = CountTimerThreads();
			System.Console.Error.WriteLine("after: " + timersAfter);
			Assert.Equal(timersBefore, timersAfter);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestInterval(long interval)
		{
			TestShell.Command command = new TestShell.Command(interval);
			command.Run();
			Assert.Equal(1, command.GetRunCount());
			command.Run();
			if (interval > 0)
			{
				Assert.Equal(1, command.GetRunCount());
			}
			else
			{
				Assert.Equal(2, command.GetRunCount());
			}
		}
	}
}
