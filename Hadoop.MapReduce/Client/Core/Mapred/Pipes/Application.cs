using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	/// <summary>
	/// This class is responsible for launching and communicating with the child
	/// process.
	/// </summary>
	internal class Application<K1, V1, K2, V2>
		where K1 : WritableComparable
		where V1 : Writable
		where K2 : WritableComparable
		where V2 : Writable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.Pipes.Application
			).FullName);

		private Socket serverSocket;

		private SystemProcess process;

		private Socket clientSocket;

		private OutputHandler<K2, V2> handler;

		private DownwardProtocol<K1, V1> downlink;

		internal static readonly bool Windows = Runtime.GetProperty("os.name").StartsWith
			("Windows");

		/// <summary>Start the child process to handle the task for us.</summary>
		/// <param name="conf">the task's configuration</param>
		/// <param name="recordReader">the fake record reader to update progress with</param>
		/// <param name="output">the collector to send output to</param>
		/// <param name="reporter">the reporter for the task</param>
		/// <param name="outputKeyClass">the class of the output keys</param>
		/// <param name="outputValueClass">the class of the output values</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal Application(JobConf conf, RecordReader<FloatWritable, NullWritable> recordReader
			, OutputCollector<K2, V2> output, Reporter reporter, Type outputKeyClass, Type outputValueClass
			)
		{
			serverSocket = Sharpen.Extensions.CreateServerSocket(0);
			IDictionary<string, string> env = new Dictionary<string, string>();
			// add TMPDIR environment variable with the value of java.io.tmpdir
			env["TMPDIR"] = Runtime.GetProperty("java.io.tmpdir");
			env[Submitter.Port] = Sharpen.Extensions.ToString(serverSocket.GetLocalPort());
			//Add token to the environment if security is enabled
			Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken = TokenCache.
				GetJobToken(conf.GetCredentials());
			// This password is used as shared secret key between this application and
			// child pipes process
			byte[] password = jobToken.GetPassword();
			string localPasswordFile = new FilePath(".") + Path.Separator + "jobTokenPassword";
			WritePasswordToLocalFile(localPasswordFile, password, conf);
			env["hadoop.pipes.shared.secret.location"] = localPasswordFile;
			IList<string> cmd = new AList<string>();
			string interpretor = conf.Get(Submitter.Interpretor);
			if (interpretor != null)
			{
				cmd.AddItem(interpretor);
			}
			string executable = DistributedCache.GetLocalCacheFiles(conf)[0].ToString();
			if (!FileUtil.CanExecute(new FilePath(executable)))
			{
				// LinuxTaskController sets +x permissions on all distcache files already.
				// In case of DefaultTaskController, set permissions here.
				FileUtil.Chmod(executable, "u+x");
			}
			cmd.AddItem(executable);
			// wrap the command in a stdout/stderr capture
			// we are starting map/reduce task of the pipes job. this is not a cleanup
			// attempt. 
			TaskAttemptID taskid = ((TaskAttemptID)TaskAttemptID.ForName(conf.Get(MRJobConfig
				.TaskAttemptId)));
			FilePath stdout = TaskLog.GetTaskLogFile(taskid, false, TaskLog.LogName.Stdout);
			FilePath stderr = TaskLog.GetTaskLogFile(taskid, false, TaskLog.LogName.Stderr);
			long logLength = TaskLog.GetTaskLogLength(conf);
			cmd = TaskLog.CaptureOutAndError(null, cmd, stdout, stderr, logLength, false);
			process = RunClient(cmd, env);
			clientSocket = serverSocket.Accept();
			string challenge = GetSecurityChallenge();
			string digestToSend = CreateDigest(password, challenge);
			string digestExpected = CreateDigest(password, digestToSend);
			handler = new OutputHandler<K2, V2>(output, reporter, recordReader, digestExpected
				);
			K2 outputKey = (K2)ReflectionUtils.NewInstance(outputKeyClass, conf);
			V2 outputValue = (V2)ReflectionUtils.NewInstance(outputValueClass, conf);
			downlink = new BinaryProtocol<K1, V1, K2, V2>(clientSocket, handler, outputKey, outputValue
				, conf);
			downlink.Authenticate(digestToSend, challenge);
			WaitForAuthentication();
			Log.Debug("Authentication succeeded");
			downlink.Start();
			downlink.SetJobConf(conf);
		}

		private string GetSecurityChallenge()
		{
			Random rand = new Random(Runtime.CurrentTimeMillis());
			//Use 4 random integers so as to have 16 random bytes.
			StringBuilder strBuilder = new StringBuilder();
			strBuilder.Append(rand.Next(unchecked((int)(0x7fffffff))));
			strBuilder.Append(rand.Next(unchecked((int)(0x7fffffff))));
			strBuilder.Append(rand.Next(unchecked((int)(0x7fffffff))));
			strBuilder.Append(rand.Next(unchecked((int)(0x7fffffff))));
			return strBuilder.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		private void WritePasswordToLocalFile(string localPasswordFile, byte[] password, 
			JobConf conf)
		{
			FileSystem localFs = FileSystem.GetLocal(conf);
			Path localPath = new Path(localPasswordFile);
			FSDataOutputStream @out = FileSystem.Create(localFs, localPath, new FsPermission(
				"400"));
			@out.Write(password);
			@out.Close();
		}

		/// <summary>
		/// Get the downward protocol object that can send commands down to the
		/// application.
		/// </summary>
		/// <returns>the downlink proxy</returns>
		internal virtual DownwardProtocol<K1, V1> GetDownlink()
		{
			return downlink;
		}

		/// <summary>Wait for authentication response.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal virtual void WaitForAuthentication()
		{
			downlink.Flush();
			Log.Debug("Waiting for authentication response");
			handler.WaitForAuthentication();
		}

		/// <summary>Wait for the application to finish</summary>
		/// <returns>did the application finish correctly?</returns>
		/// <exception cref="System.Exception"/>
		internal virtual bool WaitForFinish()
		{
			downlink.Flush();
			return handler.WaitForFinish();
		}

		/// <summary>Abort the application and wait for it to finish.</summary>
		/// <param name="t">the exception that signalled the problem</param>
		/// <exception cref="System.IO.IOException">A wrapper around the exception that was passed in
		/// 	</exception>
		internal virtual void Abort(Exception t)
		{
			Log.Info("Aborting because of " + StringUtils.StringifyException(t));
			try
			{
				downlink.Abort();
				downlink.Flush();
			}
			catch (IOException)
			{
			}
			// IGNORE cleanup problems
			try
			{
				handler.WaitForFinish();
			}
			catch
			{
				process.Destroy();
			}
			IOException wrapper = new IOException("pipe child exception");
			Sharpen.Extensions.InitCause(wrapper, t);
			throw wrapper;
		}

		/// <summary>Clean up the child procress and socket.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void Cleanup()
		{
			serverSocket.Close();
			try
			{
				downlink.Close();
			}
			catch (Exception)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
			}
		}

		/// <summary>
		/// Run a given command in a subprocess, including threads to copy its stdout
		/// and stderr to our stdout and stderr.
		/// </summary>
		/// <param name="command">the command and its arguments</param>
		/// <param name="env">the environment to run the process in</param>
		/// <returns>a handle on the process</returns>
		/// <exception cref="System.IO.IOException"/>
		internal static SystemProcess RunClient(IList<string> command, IDictionary<string
			, string> env)
		{
			ProcessStartInfo builder = new ProcessStartInfo(command);
			if (env != null)
			{
				builder.EnvironmentVariables.PutAll(env);
			}
			SystemProcess result = builder.Start();
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		public static string CreateDigest(byte[] password, string data)
		{
			SecretKey key = JobTokenSecretManager.CreateSecretKey(password);
			return SecureShuffleUtils.HashFromString(data, key);
		}
	}
}
