using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework.Runner.Notification;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Test
{
	/// <summary>
	/// JUnit run listener which prints full thread dump into System.err
	/// in case a test is failed due to timeout.
	/// </summary>
	public class TimedOutTestsListener : RunListener
	{
		internal const string TestTimedOutPrefix = "test timed out after";

		private static string Indent = "    ";

		private readonly PrintWriter output;

		public TimedOutTestsListener()
		{
			this.output = new PrintWriter(System.Console.Error);
		}

		public TimedOutTestsListener(PrintWriter output)
		{
			this.output = output;
		}

		/// <exception cref="System.Exception"/>
		public override void TestFailure(Failure failure)
		{
			if (failure != null && failure.GetMessage() != null && failure.GetMessage().StartsWith
				(TestTimedOutPrefix))
			{
				output.WriteLine("====> TEST TIMED OUT. PRINTING THREAD DUMP. <====");
				output.WriteLine();
				output.Write(BuildThreadDiagnosticString());
			}
		}

		public static string BuildThreadDiagnosticString()
		{
			StringWriter sw = new StringWriter();
			PrintWriter output = new PrintWriter(sw);
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
			output.WriteLine(string.Format("Timestamp: %s", dateFormat.Format(new DateTime())
				));
			output.WriteLine();
			output.WriteLine(BuildThreadDump());
			string deadlocksInfo = BuildDeadlockInfo();
			if (deadlocksInfo != null)
			{
				output.WriteLine("====> DEADLOCKS DETECTED <====");
				output.WriteLine();
				output.WriteLine(deadlocksInfo);
			}
			return sw.ToString();
		}

		internal static string BuildThreadDump()
		{
			StringBuilder dump = new StringBuilder();
			IDictionary<Sharpen.Thread, StackTraceElement[]> stackTraces = Sharpen.Thread.GetAllStackTraces
				();
			foreach (KeyValuePair<Sharpen.Thread, StackTraceElement[]> e in stackTraces)
			{
				Sharpen.Thread thread = e.Key;
				dump.Append(string.Format("\"%s\" %s prio=%d tid=%d %s\njava.lang.Thread.State: %s"
					, thread.GetName(), (thread.IsDaemon() ? "daemon" : string.Empty), thread.GetPriority
					(), thread.GetId(), Sharpen.Thread.State.Waiting.Equals(thread.GetState()) ? "in Object.wait()"
					 : StringUtils.ToLowerCase(thread.GetState().ToString()), Sharpen.Thread.State.Waiting
					.Equals(thread.GetState()) ? "WAITING (on object monitor)" : thread.GetState()));
				foreach (StackTraceElement stackTraceElement in e.Value)
				{
					dump.Append("\n        at ");
					dump.Append(stackTraceElement);
				}
				dump.Append("\n");
			}
			return dump.ToString();
		}

		internal static string BuildDeadlockInfo()
		{
			ThreadMXBean threadBean = ManagementFactory.GetThreadMXBean();
			long[] threadIds = threadBean.FindMonitorDeadlockedThreads();
			if (threadIds != null && threadIds.Length > 0)
			{
				StringWriter stringWriter = new StringWriter();
				PrintWriter @out = new PrintWriter(stringWriter);
				ThreadInfo[] infos = threadBean.GetThreadInfo(threadIds, true, true);
				foreach (ThreadInfo ti in infos)
				{
					PrintThreadInfo(ti, @out);
					PrintLockInfo(ti.GetLockedSynchronizers(), @out);
					@out.WriteLine();
				}
				@out.Close();
				return stringWriter.ToString();
			}
			else
			{
				return null;
			}
		}

		private static void PrintThreadInfo(ThreadInfo ti, PrintWriter @out)
		{
			// print thread information
			PrintThread(ti, @out);
			// print stack trace with locks
			StackTraceElement[] stacktrace = ti.GetStackTrace();
			MonitorInfo[] monitors = ti.GetLockedMonitors();
			for (int i = 0; i < stacktrace.Length; i++)
			{
				StackTraceElement ste = stacktrace[i];
				@out.WriteLine(Indent + "at " + ste.ToString());
				foreach (MonitorInfo mi in monitors)
				{
					if (mi.GetLockedStackDepth() == i)
					{
						@out.WriteLine(Indent + "  - locked " + mi);
					}
				}
			}
			@out.WriteLine();
		}

		private static void PrintThread(ThreadInfo ti, PrintWriter @out)
		{
			@out.Write("\"" + ti.GetThreadName() + "\"" + " Id=" + ti.GetThreadId() + " in " 
				+ ti.GetThreadState());
			if (ti.GetLockName() != null)
			{
				@out.Write(" on lock=" + ti.GetLockName());
			}
			if (ti.IsSuspended())
			{
				@out.Write(" (suspended)");
			}
			if (ti.IsInNative())
			{
				@out.Write(" (running in native)");
			}
			@out.WriteLine();
			if (ti.GetLockOwnerName() != null)
			{
				@out.WriteLine(Indent + " owned by " + ti.GetLockOwnerName() + " Id=" + ti.GetLockOwnerId
					());
			}
		}

		private static void PrintLockInfo(LockInfo[] locks, PrintWriter @out)
		{
			@out.WriteLine(Indent + "Locked synchronizers: count = " + locks.Length);
			foreach (LockInfo li in locks)
			{
				@out.WriteLine(Indent + "  - " + li);
			}
			@out.WriteLine();
		}
	}
}
