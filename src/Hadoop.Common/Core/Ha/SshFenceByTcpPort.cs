using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Jcraft.Jsch;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.HA
{
	/// <summary>
	/// This fencing implementation sshes to the target node and uses
	/// <code>fuser</code> to kill the process listening on the service's
	/// TCP port.
	/// </summary>
	/// <remarks>
	/// This fencing implementation sshes to the target node and uses
	/// <code>fuser</code> to kill the process listening on the service's
	/// TCP port. This is more accurate than using "jps" since it doesn't
	/// require parsing, and will work even if there are multiple service
	/// processes running on the same machine.<p>
	/// It returns a successful status code if:
	/// <ul>
	/// <li><code>fuser</code> indicates it successfully killed a process, <em>or</em>
	/// <li><code>nc -z</code> indicates that nothing is listening on the target port
	/// </ul>
	/// <p>
	/// This fencing mechanism is configured as following in the fencing method
	/// list:
	/// <code>sshfence([[username][:ssh-port]])</code>
	/// where the optional argument specifies the username and port to use
	/// with ssh.
	/// <p>
	/// In order to achieve passwordless SSH, the operator must also configure
	/// <code>dfs.ha.fencing.ssh.private-key-files<code> to point to an
	/// SSH key that has passphrase-less access to the given username and host.
	/// </remarks>
	public class SshFenceByTcpPort : Configured, FenceMethod
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(SshFenceByTcpPort));

		internal const string ConfConnectTimeoutKey = "dfs.ha.fencing.ssh.connect-timeout";

		private const int ConfConnectTimeoutDefault = 30 * 1000;

		internal const string ConfIdentitiesKey = "dfs.ha.fencing.ssh.private-key-files";

		/// <summary>Verify that the argument, if given, in the conf is parseable.</summary>
		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		public virtual void CheckArgs(string argStr)
		{
			if (argStr != null)
			{
				new SshFenceByTcpPort.Args(argStr);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		public virtual bool TryFence(HAServiceTarget target, string argsStr)
		{
			SshFenceByTcpPort.Args args = new SshFenceByTcpPort.Args(argsStr);
			IPEndPoint serviceAddr = target.GetAddress();
			string host = serviceAddr.GetHostName();
			Session session;
			try
			{
				session = CreateSession(serviceAddr.GetHostName(), args);
			}
			catch (JSchException e)
			{
				Log.Warn("Unable to create SSH session", e);
				return false;
			}
			Log.Info("Connecting to " + host + "...");
			try
			{
				session.Connect(GetSshConnectTimeout());
			}
			catch (JSchException e)
			{
				Log.Warn("Unable to connect to " + host + " as user " + args.user, e);
				return false;
			}
			Log.Info("Connected to " + host);
			try
			{
				return DoFence(session, serviceAddr);
			}
			catch (JSchException e)
			{
				Log.Warn("Unable to achieve fencing on remote host", e);
				return false;
			}
			finally
			{
				session.Disconnect();
			}
		}

		/// <exception cref="Com.Jcraft.Jsch.JSchException"/>
		private Session CreateSession(string host, SshFenceByTcpPort.Args args)
		{
			JSch jsch = new JSch();
			foreach (string keyFile in GetKeyFiles())
			{
				jsch.AddIdentity(keyFile);
			}
			JSch.SetLogger(new SshFenceByTcpPort.LogAdapter());
			Session session = jsch.GetSession(args.user, host, args.sshPort);
			session.SetConfig("StrictHostKeyChecking", "no");
			return session;
		}

		/// <exception cref="Com.Jcraft.Jsch.JSchException"/>
		private bool DoFence(Session session, IPEndPoint serviceAddr)
		{
			int port = serviceAddr.Port;
			try
			{
				Log.Info("Looking for process running on port " + port);
				int rc = ExecCommand(session, "PATH=$PATH:/sbin:/usr/sbin fuser -v -k -n tcp " + 
					port);
				if (rc == 0)
				{
					Log.Info("Successfully killed process that was " + "listening on port " + port);
					// exit code 0 indicates the process was successfully killed.
					return true;
				}
				else
				{
					if (rc == 1)
					{
						// exit code 1 indicates either that the process was not running
						// or that fuser didn't have root privileges in order to find it
						// (eg running as a different user)
						Log.Info("Indeterminate response from trying to kill service. " + "Verifying whether it is running using nc..."
							);
						rc = ExecCommand(session, "nc -z " + serviceAddr.GetHostName() + " " + serviceAddr
							.Port);
						if (rc == 0)
						{
							// the service is still listening - we are unable to fence
							Log.Warn("Unable to fence - it is running but we cannot kill it");
							return false;
						}
						else
						{
							Log.Info("Verified that the service is down.");
							return true;
						}
					}
				}
				// other 
				Log.Info("rc: " + rc);
				return rc == 0;
			}
			catch (Exception e)
			{
				Log.Warn("Interrupted while trying to fence via ssh", e);
				return false;
			}
			catch (IOException e)
			{
				Log.Warn("Unknown failure while trying to fence via ssh", e);
				return false;
			}
		}

		/// <summary>
		/// Execute a command through the ssh session, pumping its
		/// stderr and stdout to our own logs.
		/// </summary>
		/// <exception cref="Com.Jcraft.Jsch.JSchException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		private int ExecCommand(Session session, string cmd)
		{
			Log.Debug("Running cmd: " + cmd);
			ChannelExec exec = null;
			try
			{
				exec = (ChannelExec)session.OpenChannel("exec");
				exec.SetCommand(cmd);
				exec.SetInputStream(null);
				exec.Connect();
				// Pump stdout of the command to our WARN logs
				StreamPumper outPumper = new StreamPumper(Log, cmd + " via ssh", exec.GetInputStream
					(), StreamPumper.StreamType.Stdout);
				outPumper.Start();
				// Pump stderr of the command to our WARN logs
				StreamPumper errPumper = new StreamPumper(Log, cmd + " via ssh", exec.GetErrStream
					(), StreamPumper.StreamType.Stderr);
				errPumper.Start();
				outPumper.Join();
				errPumper.Join();
				return exec.GetExitStatus();
			}
			finally
			{
				Cleanup(exec);
			}
		}

		private static void Cleanup(ChannelExec exec)
		{
			if (exec != null)
			{
				try
				{
					exec.Disconnect();
				}
				catch (Exception t)
				{
					Log.Warn("Couldn't disconnect ssh channel", t);
				}
			}
		}

		private int GetSshConnectTimeout()
		{
			return GetConf().GetInt(ConfConnectTimeoutKey, ConfConnectTimeoutDefault);
		}

		private ICollection<string> GetKeyFiles()
		{
			return GetConf().GetTrimmedStringCollection(ConfIdentitiesKey);
		}

		/// <summary>Container for the parsed arg line for this fencing method.</summary>
		internal class Args
		{
			private static readonly Pattern UserPortRe = Pattern.Compile("([^:]+?)?(?:\\:(\\d+))?"
				);

			private const int DefaultSshPort = 22;

			internal string user;

			internal int sshPort;

			/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
			public Args(string arg)
			{
				user = Runtime.GetProperty("user.name");
				sshPort = DefaultSshPort;
				// Parse optional user and ssh port
				if (arg != null && !arg.IsEmpty())
				{
					Matcher m = UserPortRe.Matcher(arg);
					if (!m.Matches())
					{
						throw new BadFencingConfigurationException("Unable to parse user and SSH port: " 
							+ arg);
					}
					if (m.Group(1) != null)
					{
						user = m.Group(1);
					}
					if (m.Group(2) != null)
					{
						sshPort = ParseConfiggedPort(m.Group(2));
					}
				}
			}

			/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
			private int ParseConfiggedPort(string portStr)
			{
				try
				{
					return Extensions.ValueOf(portStr);
				}
				catch (FormatException)
				{
					throw new BadFencingConfigurationException("Port number '" + portStr + "' invalid"
						);
				}
			}
		}

		/// <summary>Adapter from JSch's logger interface to our log4j</summary>
		private class LogAdapter : Logger
		{
			internal static readonly Org.Apache.Commons.Logging.Log Log = LogFactory.GetLog(typeof(
				SshFenceByTcpPort).FullName + ".jsch");

			public override bool IsEnabled(int level)
			{
				switch (level)
				{
					case Logger.Debug:
					{
						return Log.IsDebugEnabled();
					}

					case Logger.Info:
					{
						return Log.IsInfoEnabled();
					}

					case Logger.Warn:
					{
						return Log.IsWarnEnabled();
					}

					case Logger.Error:
					{
						return Log.IsErrorEnabled();
					}

					case Logger.Fatal:
					{
						return Log.IsFatalEnabled();
					}

					default:
					{
						return false;
					}
				}
			}

			public override void Log(int level, string message)
			{
				switch (level)
				{
					case Logger.Debug:
					{
						Log.Debug(message);
						break;
					}

					case Logger.Info:
					{
						Log.Info(message);
						break;
					}

					case Logger.Warn:
					{
						Log.Warn(message);
						break;
					}

					case Logger.Error:
					{
						Log.Error(message);
						break;
					}

					case Logger.Fatal:
					{
						Log.Fatal(message);
						break;
					}

					default:
					{
						break;
					}
				}
			}
		}
	}
}
