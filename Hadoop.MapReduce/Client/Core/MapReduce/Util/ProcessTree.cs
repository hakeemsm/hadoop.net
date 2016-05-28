using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Util
{
	/// <summary>Process tree related operations</summary>
	public class ProcessTree
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(ProcessTree));

		public const long DefaultSleeptimeBeforeSigkill = 5000L;

		private const int Sigquit = 3;

		private const int Sigterm = 15;

		private const int Sigkill = 9;

		private const string SigquitStr = "SIGQUIT";

		private const string SigtermStr = "SIGTERM";

		private const string SigkillStr = "SIGKILL";

		public static readonly bool isSetsidAvailable = IsSetsidSupported();

		private static bool IsSetsidSupported()
		{
			Shell.ShellCommandExecutor shexec = null;
			bool setsidSupported = true;
			try
			{
				string[] args = new string[] { "setsid", "bash", "-c", "echo $$" };
				shexec = new Shell.ShellCommandExecutor(args);
				shexec.Execute();
			}
			catch (IOException)
			{
				Log.Warn("setsid is not available on this machine. So not using it.");
				setsidSupported = false;
			}
			finally
			{
				// handle the exit code
				Log.Info("setsid exited with exit code " + shexec.GetExitCode());
			}
			return setsidSupported;
		}

		/// <summary>Destroy the process-tree.</summary>
		/// <param name="pid">
		/// process id of the root process of the subtree of processes
		/// to be killed
		/// </param>
		/// <param name="sleeptimeBeforeSigkill">
		/// The time to wait before sending SIGKILL
		/// after sending SIGTERM
		/// </param>
		/// <param name="isProcessGroup">pid is a process group leader or not</param>
		/// <param name="inBackground">
		/// Process is to be killed in the back ground with
		/// a separate thread
		/// </param>
		public static void Destroy(string pid, long sleeptimeBeforeSigkill, bool isProcessGroup
			, bool inBackground)
		{
			if (isProcessGroup)
			{
				DestroyProcessGroup(pid, sleeptimeBeforeSigkill, inBackground);
			}
			else
			{
				//TODO: Destroy all the processes in the subtree in this case also.
				// For the time being, killing only the root process.
				DestroyProcess(pid, sleeptimeBeforeSigkill, inBackground);
			}
		}

		/// <summary>Destroy the process.</summary>
		/// <param name="pid">Process id of to-be-killed-process</param>
		/// <param name="sleeptimeBeforeSigkill">
		/// The time to wait before sending SIGKILL
		/// after sending SIGTERM
		/// </param>
		/// <param name="inBackground">
		/// Process is to be killed in the back ground with
		/// a separate thread
		/// </param>
		protected internal static void DestroyProcess(string pid, long sleeptimeBeforeSigkill
			, bool inBackground)
		{
			TerminateProcess(pid);
			SigKill(pid, false, sleeptimeBeforeSigkill, inBackground);
		}

		/// <summary>Destroy the process group.</summary>
		/// <param name="pgrpId">Process group id of to-be-killed-processes</param>
		/// <param name="sleeptimeBeforeSigkill">
		/// The time to wait before sending SIGKILL
		/// after sending SIGTERM
		/// </param>
		/// <param name="inBackground">
		/// Process group is to be killed in the back ground with
		/// a separate thread
		/// </param>
		protected internal static void DestroyProcessGroup(string pgrpId, long sleeptimeBeforeSigkill
			, bool inBackground)
		{
			TerminateProcessGroup(pgrpId);
			SigKill(pgrpId, true, sleeptimeBeforeSigkill, inBackground);
		}

		/// <summary>Send a specified signal to the specified pid</summary>
		/// <param name="pid">the pid of the process [group] to signal.</param>
		/// <param name="signalNum">the signal to send.</param>
		/// <param name="signalName">
		/// the human-readable description of the signal
		/// (for logging).
		/// </param>
		private static void SendSignal(string pid, int signalNum, string signalName)
		{
			Shell.ShellCommandExecutor shexec = null;
			try
			{
				string[] args = new string[] { "kill", "-" + signalNum, pid };
				shexec = new Shell.ShellCommandExecutor(args);
				shexec.Execute();
			}
			catch (IOException ioe)
			{
				Log.Warn("Error executing shell command " + ioe);
			}
			finally
			{
				if (pid.StartsWith("-"))
				{
					Log.Info("Sending signal to all members of process group " + pid + ": " + signalName
						 + ". Exit code " + shexec.GetExitCode());
				}
				else
				{
					Log.Info("Signaling process " + pid + " with " + signalName + ". Exit code " + shexec
						.GetExitCode());
				}
			}
		}

		/// <summary>Send a specified signal to the process, if it is alive.</summary>
		/// <param name="pid">the pid of the process to signal.</param>
		/// <param name="signalNum">the signal to send.</param>
		/// <param name="signalName">
		/// the human-readable description of the signal
		/// (for logging).
		/// </param>
		/// <param name="alwaysSignal">if true then send signal even if isAlive(pid) is false
		/// 	</param>
		private static void MaybeSignalProcess(string pid, int signalNum, string signalName
			, bool alwaysSignal)
		{
			// If process tree is not alive then don't signal, unless alwaysSignal
			// forces it so.
			if (alwaysSignal || ProcessTree.IsAlive(pid))
			{
				SendSignal(pid, signalNum, signalName);
			}
		}

		private static void MaybeSignalProcessGroup(string pgrpId, int signalNum, string 
			signalName, bool alwaysSignal)
		{
			if (alwaysSignal || ProcessTree.IsProcessGroupAlive(pgrpId))
			{
				// signaling a process group means using a negative pid.
				SendSignal("-" + pgrpId, signalNum, signalName);
			}
		}

		/// <summary>Sends terminate signal to the process, allowing it to gracefully exit.</summary>
		/// <param name="pid">pid of the process to be sent SIGTERM</param>
		public static void TerminateProcess(string pid)
		{
			MaybeSignalProcess(pid, Sigterm, SigtermStr, true);
		}

		/// <summary>
		/// Sends terminate signal to all the process belonging to the passed process
		/// group, allowing the group to gracefully exit.
		/// </summary>
		/// <param name="pgrpId">process group id</param>
		public static void TerminateProcessGroup(string pgrpId)
		{
			MaybeSignalProcessGroup(pgrpId, Sigterm, SigtermStr, true);
		}

		/// <summary>
		/// Kills the process(OR process group) by sending the signal SIGKILL
		/// in the current thread
		/// </summary>
		/// <param name="pid">Process id(OR process group id) of to-be-deleted-process</param>
		/// <param name="isProcessGroup">Is pid a process group id of to-be-deleted-processes
		/// 	</param>
		/// <param name="sleepTimeBeforeSigKill">
		/// wait time before sending SIGKILL after
		/// sending SIGTERM
		/// </param>
		private static void SigKillInCurrentThread(string pid, bool isProcessGroup, long 
			sleepTimeBeforeSigKill)
		{
			// Kill the subprocesses of root process(even if the root process is not
			// alive) if process group is to be killed.
			if (isProcessGroup || ProcessTree.IsAlive(pid))
			{
				try
				{
					// Sleep for some time before sending SIGKILL
					Sharpen.Thread.Sleep(sleepTimeBeforeSigKill);
				}
				catch (Exception)
				{
					Log.Warn("Thread sleep is interrupted.");
				}
				if (isProcessGroup)
				{
					KillProcessGroup(pid);
				}
				else
				{
					KillProcess(pid);
				}
			}
		}

		/// <summary>Kills the process(OR process group) by sending the signal SIGKILL</summary>
		/// <param name="pid">Process id(OR process group id) of to-be-deleted-process</param>
		/// <param name="isProcessGroup">Is pid a process group id of to-be-deleted-processes
		/// 	</param>
		/// <param name="sleeptimeBeforeSigkill">
		/// The time to wait before sending SIGKILL
		/// after sending SIGTERM
		/// </param>
		/// <param name="inBackground">
		/// Process is to be killed in the back ground with
		/// a separate thread
		/// </param>
		private static void SigKill(string pid, bool isProcessGroup, long sleeptimeBeforeSigkill
			, bool inBackground)
		{
			if (inBackground)
			{
				// use a separate thread for killing
				ProcessTree.SigKillThread sigKillThread = new ProcessTree.SigKillThread(pid, isProcessGroup
					, sleeptimeBeforeSigkill);
				sigKillThread.SetDaemon(true);
				sigKillThread.Start();
			}
			else
			{
				SigKillInCurrentThread(pid, isProcessGroup, sleeptimeBeforeSigkill);
			}
		}

		/// <summary>Sends kill signal to process, forcefully terminating the process.</summary>
		/// <param name="pid">process id</param>
		public static void KillProcess(string pid)
		{
			MaybeSignalProcess(pid, Sigkill, SigkillStr, false);
		}

		/// <summary>
		/// Sends SIGQUIT to process; Java programs will dump their stack to
		/// stdout.
		/// </summary>
		/// <param name="pid">process id</param>
		public static void SigQuitProcess(string pid)
		{
			MaybeSignalProcess(pid, Sigquit, SigquitStr, false);
		}

		/// <summary>
		/// Sends kill signal to all process belonging to same process group,
		/// forcefully terminating the process group.
		/// </summary>
		/// <param name="pgrpId">process group id</param>
		public static void KillProcessGroup(string pgrpId)
		{
			MaybeSignalProcessGroup(pgrpId, Sigkill, SigkillStr, false);
		}

		/// <summary>
		/// Sends SIGQUIT to all processes belonging to the same process group,
		/// ordering all processes in the group to send their stack dump to
		/// stdout.
		/// </summary>
		/// <param name="pgrpId">process group id</param>
		public static void SigQuitProcessGroup(string pgrpId)
		{
			MaybeSignalProcessGroup(pgrpId, Sigquit, SigquitStr, false);
		}

		/// <summary>
		/// Is the process with PID pid still alive?
		/// This method assumes that isAlive is called on a pid that was alive not
		/// too long ago, and hence assumes no chance of pid-wrapping-around.
		/// </summary>
		/// <param name="pid">pid of the process to check.</param>
		/// <returns>true if process is alive.</returns>
		public static bool IsAlive(string pid)
		{
			Shell.ShellCommandExecutor shexec = null;
			try
			{
				string[] args = new string[] { "kill", "-0", pid };
				shexec = new Shell.ShellCommandExecutor(args);
				shexec.Execute();
			}
			catch (Shell.ExitCodeException)
			{
				return false;
			}
			catch (IOException ioe)
			{
				Log.Warn("Error executing shell command " + shexec.ToString() + ioe);
				return false;
			}
			return (shexec.GetExitCode() == 0 ? true : false);
		}

		/// <summary>
		/// Is the process group with  still alive?
		/// This method assumes that isAlive is called on a pid that was alive not
		/// too long ago, and hence assumes no chance of pid-wrapping-around.
		/// </summary>
		/// <param name="pgrpId">process group id</param>
		/// <returns>true if any of process in group is alive.</returns>
		public static bool IsProcessGroupAlive(string pgrpId)
		{
			Shell.ShellCommandExecutor shexec = null;
			try
			{
				string[] args = new string[] { "kill", "-0", "-" + pgrpId };
				shexec = new Shell.ShellCommandExecutor(args);
				shexec.Execute();
			}
			catch (Shell.ExitCodeException)
			{
				return false;
			}
			catch (IOException ioe)
			{
				Log.Warn("Error executing shell command " + shexec.ToString() + ioe);
				return false;
			}
			return (shexec.GetExitCode() == 0 ? true : false);
		}

		/// <summary>Helper thread class that kills process-tree with SIGKILL in background</summary>
		internal class SigKillThread : Sharpen.Thread
		{
			private string pid = null;

			private bool isProcessGroup = false;

			private long sleepTimeBeforeSigKill = DefaultSleeptimeBeforeSigkill;

			private SigKillThread(string pid, bool isProcessGroup, long interval)
			{
				this.pid = pid;
				this.isProcessGroup = isProcessGroup;
				this.SetName(this.GetType().FullName + "-" + pid);
				sleepTimeBeforeSigKill = interval;
			}

			public override void Run()
			{
				SigKillInCurrentThread(pid, isProcessGroup, sleepTimeBeforeSigKill);
			}
		}
	}
}
