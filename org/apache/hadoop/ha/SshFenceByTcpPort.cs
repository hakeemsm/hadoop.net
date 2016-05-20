using Sharpen;

namespace org.apache.hadoop.ha
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
	public class SshFenceByTcpPort : org.apache.hadoop.conf.Configured, org.apache.hadoop.ha.FenceMethod
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.SshFenceByTcpPort
			)));

		internal const string CONF_CONNECT_TIMEOUT_KEY = "dfs.ha.fencing.ssh.connect-timeout";

		private const int CONF_CONNECT_TIMEOUT_DEFAULT = 30 * 1000;

		internal const string CONF_IDENTITIES_KEY = "dfs.ha.fencing.ssh.private-key-files";

		/// <summary>Verify that the argument, if given, in the conf is parseable.</summary>
		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		public virtual void checkArgs(string argStr)
		{
			if (argStr != null)
			{
				new org.apache.hadoop.ha.SshFenceByTcpPort.Args(argStr);
			}
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		public virtual bool tryFence(org.apache.hadoop.ha.HAServiceTarget target, string 
			argsStr)
		{
			org.apache.hadoop.ha.SshFenceByTcpPort.Args args = new org.apache.hadoop.ha.SshFenceByTcpPort.Args
				(argsStr);
			java.net.InetSocketAddress serviceAddr = target.getAddress();
			string host = serviceAddr.getHostName();
			com.jcraft.jsch.Session session;
			try
			{
				session = createSession(serviceAddr.getHostName(), args);
			}
			catch (com.jcraft.jsch.JSchException e)
			{
				LOG.warn("Unable to create SSH session", e);
				return false;
			}
			LOG.info("Connecting to " + host + "...");
			try
			{
				session.connect(getSshConnectTimeout());
			}
			catch (com.jcraft.jsch.JSchException e)
			{
				LOG.warn("Unable to connect to " + host + " as user " + args.user, e);
				return false;
			}
			LOG.info("Connected to " + host);
			try
			{
				return doFence(session, serviceAddr);
			}
			catch (com.jcraft.jsch.JSchException e)
			{
				LOG.warn("Unable to achieve fencing on remote host", e);
				return false;
			}
			finally
			{
				session.disconnect();
			}
		}

		/// <exception cref="com.jcraft.jsch.JSchException"/>
		private com.jcraft.jsch.Session createSession(string host, org.apache.hadoop.ha.SshFenceByTcpPort.Args
			 args)
		{
			com.jcraft.jsch.JSch jsch = new com.jcraft.jsch.JSch();
			foreach (string keyFile in getKeyFiles())
			{
				jsch.addIdentity(keyFile);
			}
			com.jcraft.jsch.JSch.setLogger(new org.apache.hadoop.ha.SshFenceByTcpPort.LogAdapter
				());
			com.jcraft.jsch.Session session = jsch.getSession(args.user, host, args.sshPort);
			session.setConfig("StrictHostKeyChecking", "no");
			return session;
		}

		/// <exception cref="com.jcraft.jsch.JSchException"/>
		private bool doFence(com.jcraft.jsch.Session session, java.net.InetSocketAddress 
			serviceAddr)
		{
			int port = serviceAddr.getPort();
			try
			{
				LOG.info("Looking for process running on port " + port);
				int rc = execCommand(session, "PATH=$PATH:/sbin:/usr/sbin fuser -v -k -n tcp " + 
					port);
				if (rc == 0)
				{
					LOG.info("Successfully killed process that was " + "listening on port " + port);
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
						LOG.info("Indeterminate response from trying to kill service. " + "Verifying whether it is running using nc..."
							);
						rc = execCommand(session, "nc -z " + serviceAddr.getHostName() + " " + serviceAddr
							.getPort());
						if (rc == 0)
						{
							// the service is still listening - we are unable to fence
							LOG.warn("Unable to fence - it is running but we cannot kill it");
							return false;
						}
						else
						{
							LOG.info("Verified that the service is down.");
							return true;
						}
					}
				}
				// other 
				LOG.info("rc: " + rc);
				return rc == 0;
			}
			catch (System.Exception e)
			{
				LOG.warn("Interrupted while trying to fence via ssh", e);
				return false;
			}
			catch (System.IO.IOException e)
			{
				LOG.warn("Unknown failure while trying to fence via ssh", e);
				return false;
			}
		}

		/// <summary>
		/// Execute a command through the ssh session, pumping its
		/// stderr and stdout to our own logs.
		/// </summary>
		/// <exception cref="com.jcraft.jsch.JSchException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		private int execCommand(com.jcraft.jsch.Session session, string cmd)
		{
			LOG.debug("Running cmd: " + cmd);
			com.jcraft.jsch.ChannelExec exec = null;
			try
			{
				exec = (com.jcraft.jsch.ChannelExec)session.openChannel("exec");
				exec.setCommand(cmd);
				exec.setInputStream(null);
				exec.connect();
				// Pump stdout of the command to our WARN logs
				org.apache.hadoop.ha.StreamPumper outPumper = new org.apache.hadoop.ha.StreamPumper
					(LOG, cmd + " via ssh", exec.getInputStream(), org.apache.hadoop.ha.StreamPumper.StreamType
					.STDOUT);
				outPumper.start();
				// Pump stderr of the command to our WARN logs
				org.apache.hadoop.ha.StreamPumper errPumper = new org.apache.hadoop.ha.StreamPumper
					(LOG, cmd + " via ssh", exec.getErrStream(), org.apache.hadoop.ha.StreamPumper.StreamType
					.STDERR);
				errPumper.start();
				outPumper.join();
				errPumper.join();
				return exec.getExitStatus();
			}
			finally
			{
				cleanup(exec);
			}
		}

		private static void cleanup(com.jcraft.jsch.ChannelExec exec)
		{
			if (exec != null)
			{
				try
				{
					exec.disconnect();
				}
				catch (System.Exception t)
				{
					LOG.warn("Couldn't disconnect ssh channel", t);
				}
			}
		}

		private int getSshConnectTimeout()
		{
			return getConf().getInt(CONF_CONNECT_TIMEOUT_KEY, CONF_CONNECT_TIMEOUT_DEFAULT);
		}

		private System.Collections.Generic.ICollection<string> getKeyFiles()
		{
			return getConf().getTrimmedStringCollection(CONF_IDENTITIES_KEY);
		}

		/// <summary>Container for the parsed arg line for this fencing method.</summary>
		internal class Args
		{
			private static readonly java.util.regex.Pattern USER_PORT_RE = java.util.regex.Pattern
				.compile("([^:]+?)?(?:\\:(\\d+))?");

			private const int DEFAULT_SSH_PORT = 22;

			internal string user;

			internal int sshPort;

			/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
			public Args(string arg)
			{
				user = Sharpen.Runtime.getProperty("user.name");
				sshPort = DEFAULT_SSH_PORT;
				// Parse optional user and ssh port
				if (arg != null && !arg.isEmpty())
				{
					java.util.regex.Matcher m = USER_PORT_RE.matcher(arg);
					if (!m.matches())
					{
						throw new org.apache.hadoop.ha.BadFencingConfigurationException("Unable to parse user and SSH port: "
							 + arg);
					}
					if (m.group(1) != null)
					{
						user = m.group(1);
					}
					if (m.group(2) != null)
					{
						sshPort = parseConfiggedPort(m.group(2));
					}
				}
			}

			/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
			private int parseConfiggedPort(string portStr)
			{
				try
				{
					return int.Parse(portStr);
				}
				catch (java.lang.NumberFormatException)
				{
					throw new org.apache.hadoop.ha.BadFencingConfigurationException("Port number '" +
						 portStr + "' invalid");
				}
			}
		}

		/// <summary>Adapter from JSch's logger interface to our log4j</summary>
		private class LogAdapter : com.jcraft.jsch.Logger
		{
			internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
				.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.SshFenceByTcpPort
				)).getName() + ".jsch");

			public override bool isEnabled(int level)
			{
				switch (level)
				{
					case com.jcraft.jsch.Logger.DEBUG:
					{
						return LOG.isDebugEnabled();
					}

					case com.jcraft.jsch.Logger.INFO:
					{
						return LOG.isInfoEnabled();
					}

					case com.jcraft.jsch.Logger.WARN:
					{
						return LOG.isWarnEnabled();
					}

					case com.jcraft.jsch.Logger.ERROR:
					{
						return LOG.isErrorEnabled();
					}

					case com.jcraft.jsch.Logger.FATAL:
					{
						return LOG.isFatalEnabled();
					}

					default:
					{
						return false;
					}
				}
			}

			public override void log(int level, string message)
			{
				switch (level)
				{
					case com.jcraft.jsch.Logger.DEBUG:
					{
						LOG.debug(message);
						break;
					}

					case com.jcraft.jsch.Logger.INFO:
					{
						LOG.info(message);
						break;
					}

					case com.jcraft.jsch.Logger.WARN:
					{
						LOG.warn(message);
						break;
					}

					case com.jcraft.jsch.Logger.ERROR:
					{
						LOG.error(message);
						break;
					}

					case com.jcraft.jsch.Logger.FATAL:
					{
						LOG.fatal(message);
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
